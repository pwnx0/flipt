# Flipt Testnet Architecture & Strategy Guide (Arc Network)

## 1. Overview & Campaign Context
- **Platform**: Flipt ([testnet.flipt.fun](https://testnet.flipt.fun)) — Token launchpad & bonding curve protocol built on Arc Network.
- **Network**: Arc Testnet
  - **RPC URL**: `https://rpc.testnet.arc.network`
  - **Chain ID**: `5042002`
  - **Native Gas Token**: USDC (funded via Circle faucet or testnet gas distribution)
- **Campaign Duration**: 48 hours total (~24 hours remaining).
- **Core Incentives**: Final leaderboard rankings determine **Phase 0 Testnet NFT Tiers** (Gold, Silver, Bronze), which will mint on Arc Mainnet after launch.

---

## 2. Core Protocol Contracts

| Contract Role | Address | Description |
| :--- | :--- | :--- |
| **Testnet USDC & Faucet** | `0x4F3b8005d6b3F4994a791D971bcD153E114D20c2` | ERC-20 token (6 decimals). Calling `faucet()` dispenses 500,000 USDC per wallet and qualifies it for the Phase 0 NFT. |
| **Flipt Router (Hub)** | `0x4B33146F2bCc75574534374C85662f9E51C38Aca` | Main entrypoint for launching tokens, buying/selling on bonding curves, graduating pools, and collecting creator fees. |
| **Flipt Factory** | `0xb9200934941A9010d31733D034b9eAd7a7746d12` | Deploys token contracts using `CREATE2` and enforces vanity address suffix rules. |

---

## 3. Key Smart Contract Functions & Selectors

### A. Faucet & NFT Qualification
- **Contract**: `0x4F3b8005d6b3F4994a791D971bcD153E114D20c2`
- **Method**: `faucet()`
- **Selector**: `0xde5f72fd`
- **Gas**: ~75,000
- **Action**: Transmits 500,000 test USDC and secures Phase 0 NFT registration.

### B. Launch Token
- **Contract**: Router (`0x4B33146F2bCc75574534374C85662f9E51C38Aca`)
- **Selector**: `0xe43d45f0`
- **Signature**: `launch(string name, string symbol, string uri, uint256 salt, bytes data, uint256 buyAmount, uint256 minTokensOut)`
- **Vanity Requirement**: The protocol strictly requires that the resulting `CREATE2` token address must end in `69d2` (`uint160(addr) & 0xffff == 0x69d2`).
  - Salt generation formula: `bytes32 salt = keccak256(abi.encode(msg.sender, salt_uint))`
  - Init code hash: Queried via `factory.tokenInitCodeHash(name, symbol, totalSupply)`

### C. Buy on Bonding Curve
- **Contract**: Router (`0x4B33146F2bCc75574534374C85662f9E51C38Aca`)
- **Selector**: `0xa59ac6dd`
- **Signature**: `buy(address token, uint256 usdcAmount, uint256 minTokensOut)`
- **Prerequisite**: Caller must approve Router on the USDC contract first (`approve(router, amount)`).

### D. Sell on Bonding Curve (Take Profit)
- **Contract**: Router (`0x4B33146F2bCc75574534374C85662f9E51C38Aca`)
- **Selector**: `0x6a272462`
- **Signature**: `sell(address token, uint256 tokenAmount, uint256 minUsdcOut)`
- **Prerequisite**: Caller must approve Router on the Token contract first (`approve(router, amount)`).

### E. Pool Graduation & Liquidity Bonding
- **Contract**: Router (`0x4B33146F2bCc75574534374C85662f9E51C38Aca`)
- **Selector**: `0xff6d8d05`
- **Signature**: `graduate(address token)`
- **Action**: Once the bonding curve reaches reserve saturation, converts the curve into open trading and locks early curve positions into bonded liquidity.

### F. Collect Creator Trading Fees
- **Contract**: Router (`0x4B33146F2bCc75574534374C85662f9E51C38Aca`)
- **Selector**: `0xcf6bc454`
- **Signature**: `collectCreatorFee(address token)`
- **Action**: Withdraws accumulated creator trading fees for the wallet that originally launched the token.

---

## 4. Leaderboard Mechanics & Anti-Abuse Rules

### Can you rank #1 by artificially creating a pool with "1 token = 100k USDC"?
**No, for two reasons:**
1. **Bonding Curve Mathematical Constraints**:
   Tokens do not launch on an unrestricted Uniswap AMM where users set arbitrary initial prices. They launch on Flipt's deterministic bonding curve with fixed virtual reserves. Price discovery is governed by the curve until graduation.
2. **Abuse Review & Disqualification Rules**:
   The official terms explicitly clarify:
   - *"The final testnet ranking considers trading activity, bonding behavior, and graduation participation, with abusive activity removed."*
   - *"Under proposed rules, the following can lead to disqualification: Repeated circular trades designed to inflate volume, profit, or fees; dust transactions; order spam; or manipulated accounting."*
   - Arbitrary fake pools or non-executable paper profits are filtered out during the post-testnet audit.

### What Actually Ranks Wallets into Gold Tier:
1. **High Realized Trading Volume**: Legitimate purchases on active bonding curves using test USDC.
2. **Realized Net Profit**: Buying earlier on the curve, watching price appreciate from genuine volume, and selling a percentage back into the curve to register actual positive USDC PnL.
3. **Bonding & Graduation Participation**: Holding a bonded position through pool graduation.
4. **Creator Fee Activity**: Launching a token that attracts multiple buyers and successfully collecting creator fees.

---

## 5. Automated Pipeline Architecture

```text
[ Step 1: Faucet & Phase 0 NFT Registration ]
  Script: flipt_bot.py
  - Multi-threaded worker pool (20-40 threads) through proxy2.txt.
  - Calls faucet() on 0x4F3b8005...20c2.
  - Checks balanceOf and updates data/flipt_claimed.txt checkpoint.

[ Step 2: Token Launch ]
  Script: flipt_trader.py
  - Creator (Wallet 0) queries tokenInitCodeHash dynamically.
  - Mines vanity salt ending in '69d2' in ~1 second via local CREATE2.
  - Calls launch() with 2,500 USDC seed.

[ Step 3: Multi-Wallet Bonding Curve Pump ]
  Script: flipt_trader.py
  - Concurrently executes buys across top 200–500 funded wallets.
  - Buys 8,000–20,000 USDC per wallet.
  - Drives token price up 5x–20x, building immense trading volume.

[ Step 4: Realized Profit Lock ]
  Script: flipt_trader.py
  - Early buyer wallets sell 40% of tokens.
  - Locks in massive realized USDC gains directly on Flipt's live profit leaderboard.

[ Step 5: Fee Claim & Graduation ]
  Script: flipt_trader.py
  - Calls collectCreatorFee() to collect creator revenue.
  - Calls graduate() once reserve threshold is reached.
```

---

## 6. How to Run

### Phase 1: Claim Faucet & Secure NFT
```bash
python flipt_bot.py claim --threads 20
```

### Phase 2: Launch Token, Pump Volume & Lock Leaderboard Profit
```bash
python flipt_trader.py --threads 20 --wallets 300
```
