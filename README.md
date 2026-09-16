# Flipt Testnet Architecture & Production Guide (Arc Network)

## 1. Overview & Campaign Context
- **Platform**: Flipt ([testnet.flipt.fun](https://testnet.flipt.fun)) — Token launchpad and dynamic bonding curve protocol deployed on Arc Network.
- **Network Specification**:
  - **Network Name**: Arc Testnet
  - **Chain ID**: `5042002`
  - **Gas Token**: Native Arc ETH (`21 gwei` pinned base fee)
  - **Settlement Token**: Testnet USDC (6 decimals)
  - **Multicall3 Contract**: `0xcA11bde05977b3631167028862bE2a173976CA11`
- **Core Objective**: Execute diverse, verifiable on-chain actions (token launches, bonding curve buy waves, liquidity unbonding, pool graduation, fee collection) across 3,500+ wallets to qualify for **Phase 0 Testnet NFT Tiers** (Gold, Silver, Bronze) on Arc Mainnet.

---

## 2. Smart Contract Reference & Function Selectors

| Contract Role | Address | Description |
| :--- | :--- | :--- |
| **Router (Execution Hub)** | `0x4B33146F2bCc75574534374C85662f9E51C38Aca` | Main entrypoint for launching tokens, buying/selling, unbonding, claiming, and collecting fees. |
| **Hub (Quotes & Constants)** | `0x6ab2635fec3c426d825d005e24cfc05b82ea3994` | On-chain registry storing curve parameters, unbond delays, and quotes. |
| **Factory (CREATE2 Deployer)**| `0xb9200934941A9010d31733D034b9eAd7a7746d12` | Deploys token contracts via `CREATE2`; enforces mandatory vanity suffix. |
| **Testnet USDC** | `0x4F3b8005d6b3F4994a791D971bcD153E114D20c2` | Standard ERC-20 payment token used for bonding curve trading. |
| **Multicall3** | `0xcA11bde05977b3631167028862bE2a173976CA11` | Batched read engine (`aggregate3`) for low-latency preflights and position checks. |

### Function Selectors

| Action | Function Signature | Selector | Gas Limit |
| :--- | :--- | :--- | :--- |
| **Launch** | `launch(string,string,string,uint256,bytes,uint256,uint256)` | `0xe43d45f0` | ~1,800,000 |
| **Buy** | `buy(address,uint256,uint256)` | `0xa59ac6dd` | ~320,000 |
| **Sell** | `sell(address,uint256,uint256)` | `0x6a272462` | ~450,000 |
| **Approve** | `approve(address,uint256)` | `0x095ea7b3` | ~80,000 |
| **Unbond** | `unbond(address,uint256,uint8,uint256,uint64)` | `keccak("unbond(...)")[:4]` | ~450,000 |
| **Claim & Approve** | `claimApprove(address)` | `0x5dd68e16` | ~400,000 |
| **Graduate** | `graduate(address)` | `0xff6d8d05` | ~600,000 |
| **Collect Creator Fee** | `collectCreatorFee(address)` | `0xcf6bc454` | ~250,000 |
| **Aggregate3** | `aggregate3((address,bool,bytes)[])` | `0x82ad56cb` | Variable |

> [!IMPORTANT]
> **Vanity Address Rule**: The Flipt factory strictly enforces that all launched token addresses must end with bytes `\x69\xd2` (`uint160(addr) & 0xffff == 0x69d2`). The bot automatically grinds local `CREATE2` salts using `tokenInitCodeHash` in ~1 second.

---

## 3. High-Performance Architecture (`flipt_v4.py` / `flipt_v5.py`)

The bot engine has been completely hardened into an autonomous, 100% self-contained Python script with **zero external local file dependencies**.

```
                           ┌─────────────────────────────────────┐
                           │    ArcRpcPool (6 Live Endpoints)    │
                           │  - Health Scoring & Latency Weight  │
                           │  - 30s Timeout & WAF Evasion        │
                           └──────────────────┬──────────────────┘
                                              │
                           ┌──────────────────▼──────────────────┐
                           │      FastRpc Token Bucket Engine    │
                           │  - 25 req/s Rate Limiter (No 429s)  │
                           │  - Multicall3 aggregate3 Batching   │
                           │  - JSON-RPC Batch Send & Receipts   │
                           └──────────────────┬──────────────────┘
                                              │
     ┌────────────────────────────────────────┴────────────────────────────────────────┐
     │                                                                                 │
┌────▼──────────────────────────────┐                             ┌────────────────────▼─────────────────────┐
│    Zero Cold-Start JIT Launch     │                             │      Streaming 50-Wallet Trade Wave      │
│  - Preflight ONLY Creator (0.3s)  │                             │  - Stream 50 wallets per batch           │
│  - Mine '69d2' vanity salt (1s)   │                             │  - JIT Nonce + Balance query (1s)        │
│  - Pre-simulate on-chain          │                             │  - $50–$100 Buy Wave on Active Tokens    │
│  - Broadcast & Confirm Deploy     │                             │  - Nonce reservation & failure rollback  │
└───────────────────────────────────┘                             └──────────────────────────────────────────┘
```

### Key Engineering Features:
1. **Zero Cold-Start (JIT Streaming Pipeline)**:
   - Eliminates the previous 3-minute blocking startup delay.
   - Creator wallets are preflighted just-in-time in **< 0.5s**, initiating deployment in seconds.
   - Wallets are streamed in **50-wallet chunks** (1s preflight ➔ 1s batch buy ➔ next chunk).
2. **Safe RPC Batch Sizing**:
   - `preflight_batch` chunked to 50 wallets (150 Multicall3 items) to prevent `urlopen` socket timeouts.
   - `batch_nonces` chunked to 50 requests to avoid RPC gateway `HTTP 500: Internal Server Error`.
3. **Anti-Stranded Nonce Engine (`NonceManager`)**:
   - Every transaction reserves a local nonce atomically.
   - If a broadcast fails or drops, the reserved nonce is instantly rolled back so nonces are never skipped or stranded.
4. **Atomic State Persistence**:
   - Real-time balances, deployments, allowances, and receipts persist to `data/flipt_v4_state.json`.
   - Writes use atomic replacement (`.tmp` write ➔ atomic rename) to eliminate file corruption during concurrent reads.
5. **Sybil Resistance (160+ Unique Tokens)**:
   - Integrates `token_catalog.json` with 160+ curated creative token identities so deployments have unique names, symbols, and descriptions.

---

## 4. Bonding Curve & Live Market Dynamics

### The Bonding Curve Lifecycle:
1. **Bonding Phase**:
   - The token trades on the bonding curve up to **$6,375 USDC raised** (~793,000,000 tokens sold).
   - Buys increase the price deterministically.
2. **Frozen / Graduation Phase**:
   - Once ~$6,300–$6,375 USDC is raised, the curve reaches capacity (`phase: "frozen"`).
   - Additional curve buys will revert.
   - Anyone can call `graduate(token)` to migrate the pool into open DEX trading.
3. **The Real Exit State Machine**:
   - `unbond(token, amount, KIND_CLAIM)` (only valid after graduation; reverts on ungraduated curves).
   - **Mandatory delay**: Must wait out `UNBOND_DELAY` (90 seconds).
   - `claimApprove(token)` (claims unbonded tokens into wallet and approves router in one single transaction).
   - `sell(token, amount, minUsdcOut)` (sells tokens back into the pool for realized USDC profit).

### Live Market Integration (`https://api-testnet.flipt.fun/board/newest?limit=60`):
- The bot automatically queries the Flipt backend API for the newest active tokens.
- Filters strictly for `phase == "bonding"`, `graduated == false`, and `raised < $6,200 USDC`.
- Combines newly deployed bot tokens with active testnet tokens, distributing volume across live curves.

---

## 5. Execution Runbook & Commands

### Prerequisites
Make sure dependencies are installed and `pv.txt` contains your testnet private keys:
```bash
pip install web3 eth-account eth-abi eth-utils "eth-hash[pycryptodome]"
```

### A. Quick Health & Balance Check (Dry-Run)
Inspect the first 20 wallets with Multicall3:
```bash
python flipt_v4.py --preflight --wallets 20
```

### B. Dry-Run Verification (0 Gas Spent)
Simulate full pipeline execution (vanity mining, simulation, buy batching) without broadcasting transactions:
```bash
python flipt_v4.py --auto --wallets 50
```

### C. Live Production Run (Single Cycle)
Execute 1 full automated cycle (JIT Creator Deploy ➔ Post-Deploy $50–$100 Buy Waves):
```bash
python flipt_v4.py --auto --wallets 3542 --workers 25 --send
```

### D. Non-Stop Continuous Looping Mode
Runs continuously, deploying new tokens and streaming $50–$100 buys across all wallets, with a 30s rest between cycles:
```bash
python flipt_v4.py --loop --wallets 3542 --workers 25 --send
```

### E. Targeted Token Trading
Direct all funded wallets to execute $50–$100 buys on a specific target token:
```bash
python flipt_v4.py --trade --token 0xYOUR_TOKEN_ADDRESS --wallets 500 --workers 20 --send
```

### F. Position Exit & Profit Realization
Execute the full exit pipeline (`unbond` ➔ wait 90s ➔ `claimApprove` ➔ `sell`) across all graduated positions:
```bash
python flipt_v4.py --exit --wallets 200 --send
```

---

## 6. Diagnostic & Error Code Reference

| Error / Selector | Meaning | Bot Behavior |
| :--- | :--- | :--- |
| `0x8ae7acea` / `CorePaused()` | Flipt protocol router operations temporarily paused on Arc Testnet. | **Pre-flight simulation catches this automatically.** The transaction is skipped with a warning; 0 gas burned. |
| `HTTP Error 500: Internal Server Error` | RPC gateway batch size exceeded (over 100 calls in 1 HTTP post). | **Resolved**: Nonce batching capped at 50 requests per HTTP payload. |
| `<urlopen error The write operation timed out>` | Multicall payload too large for remote RPC socket. | **Resolved**: Preflight batching capped at 50 wallets (150 items), socket timeout set to 30s. |
| `execution reverted: curve capped` | Token bonding curve has raised $6,375 USDC and is saturated. | Bot checks `raised < $6,200 USDC` via Flipt API and skips saturated curves. |
| `nonce too low` / `replacement transaction underpriced` | Out-of-order or duplicate nonce submission. | **Resolved**: `NonceManager` reserves nonces locally and only advances after confirmed broadcast. |
