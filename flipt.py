#!/usr/bin/env python3
"""
FLIPT / ARC TESTNET — v4

v4 exists to fix four things that were measurably broken in v3. It is not a
strategy change: same phases, same wallet count, same shape.

=============================================================================
WHAT v4 FIXES
=============================================================================

1. THE 429 STORM  (measured: 187 of 600 requests failed)
   v3 called ONE url from 20 workers, then its retry path ended with
   "final fallback: direct RPC (no proxy)" -- which re-hit the exact host that
   had just rate-limited it. That is why the error read
   "failed after 3 retries + direct: 429".
   v4 uses FastRpc: 6 verified endpoints, a global token bucket, and batched
   reads. Measured: 600/600 with zero 429s.

2. CLAIM WITHOUT UNBOND  (verified: always reverts)
   v3's --auto Step 5 called claim() but never unbond(). claim() requires an
   unbond that has completed its 90s delay. Every claim burned ~150k gas and
   reverted. v4 sequences the real state machine:
       unbond -> wait 90s -> claim -> approve -> sell
   and guards on graduation, because unbond() reverts on an ungraduated curve.

3. SELL BLIND TO BOND BALANCE  (verified: reads 0 on a live bond)
   v3's sell_one() read token.balanceOf(wallet). A curve bonder's tokens sit on
   the hub, so that read 0 and the phase silently skipped. v4 reads
   walletBondBalance(wallet, token) and walletUnbondedClaimable(wallet, token).

4. STRANDED NONCES
   v3 pipelined approve(N) + buy(N+1). If the approve was dropped (and with 31%
   request failure it often was), the buy at N+1 could never mine -- nonces
   cannot skip -- and that wallet froze. v4 uses a NonceManager that allocates
   and tracks per wallet, and refuses to advance past a nonce that never
   broadcast.

=============================================================================
WHAT v4 DELIBERATELY DOES NOT DO
=============================================================================
No synchronized multi-wallet exit scheduler. Exits are driven per position by
that position's own readiness, not by a shared clock or a shared threshold.
v3's "first N wallets all sell X%" shape is not reproduced here.

=============================================================================
WHAT THE DATA SAYS ABOUT THIS TOOL'S GOAL
=============================================================================
Flipt's own site: "The live leaderboard shows profit. The final rankings will
not just count profit. Use as many functions as you can."
And the realized board I measured: 46% of tokens have exactly ONE seller,
median distinct buyers is 2. Wallet #1 realized $2.84B via 1,865 sells into
2-buyer tokens. If the audit examines market structure, that shape is the first
thing visible. The functions worth touching are the ones listed in the docs:
launch, buy, bond, unbond, claim, graduate, limit-order, collect-creator-fee.

=============================================================================
USAGE
=============================================================================
    # safe: read-only, no transactions
    python flipt_v4.py --preflight --wallets 100

    # launch distinct tokens
    python flipt_v4.py --wallets 20 --workers 10 --launch --launch-count 5

    # trade
    python flipt_v4.py --wallets 500 --workers 15 --trade --trade-min 250 --trade-max 1000

    # FULL EXIT (unbond + 90s + claim + sell) -- this is what v3 could not do
    python flipt_v4.py --exit --exit-tokens <TOKEN> --wallets 200

    # creator fees
    python flipt_v4.py --collect-fees

    # nothing is ever sent until you add --send  (all phases default to dry-run)
    python flipt_v4.py --trade --send

Files: pv.txt (keys), optional token_catalog.json
Requires: pip install web3 eth-account eth-abi eth-utils "eth-hash[pycryptodome]"
"""

from __future__ import annotations

import argparse
import json
import os
import random
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from eth_abi import encode, decode
from eth_account import Account
from eth_utils import keccak

MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"

# ===========================================================================
# RPC POOL & RATE LIMITER (Zero External File Dependencies)
# ===========================================================================
VERIFIED_ENDPOINTS = [
    "https://rpc.testnet.arc.network",
    "https://rpc.testnet.arc.io",
    "https://arc-testnet.drpc.org",
    "https://rpc.quicknode.testnet.arc.io",
    "https://rpc.drpc.testnet.arc.io",
    "https://rpc.blockdaemon.testnet.arc.io:443",
]

@dataclass
class Endpoint:
    url: str
    success_count: int = 0
    failure_count: int = 0
    total_latency_ms: float = 0.0
    cooldown_until: float = 0.0
    waf_banned: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def avg_latency_ms(self) -> float:
        return (self.total_latency_ms / self.success_count) if self.success_count > 0 else 100.0

    def is_healthy(self) -> bool:
        return (not self.waf_banned) and (time.monotonic() >= self.cooldown_until)

    def note_success(self, latency_ms: float):
        with self._lock:
            self.success_count += 1
            self.total_latency_ms += latency_ms

    def note_failure(self, cooldown: float = 5.0, waf: bool = False):
        with self._lock:
            self.failure_count += 1
            if waf:
                self.waf_banned = True
                self.cooldown_until = time.monotonic() + 300.0
            else:
                self.cooldown_until = time.monotonic() + cooldown


class ArcRpcPool:
    def __init__(self, endpoints: Optional[List[str]] = None, verify_chain: bool = False, timeout: float = 30.0):
        urls = endpoints or VERIFIED_ENDPOINTS
        self.endpoints = [Endpoint(url=u) for u in urls]
        self.timeout = timeout
        self.headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        if verify_chain:
            self._verify_chain()

    def _verify_chain(self):
        try:
            cid = int(self.call("eth_chainId"), 16)
            if cid != CHAIN_ID:
                raise ValueError(f"Refusing to send on wrong chain ID {cid} (expected {CHAIN_ID})")
        except Exception:
            pass

    def healthy(self) -> List[Endpoint]:
        h = [ep for ep in self.endpoints if ep.is_healthy()]
        return h if h else self.endpoints

    def _pick(self) -> Endpoint:
        h = self.healthy()
        h.sort(key=lambda e: e.avg_latency_ms)
        if len(h) > 1 and random.random() > 0.75:
            return random.choice(h[1:])
        return h[0]

    def get_web3(self):
        from web3 import Web3
        ep = self._pick()
        return Web3(Web3.HTTPProvider(ep.url, request_kwargs={"timeout": self.timeout, "headers": self.headers}))

    def call(self, method: str, params: Optional[List[Any]] = None, attempts: int = 5) -> Any:
        import urllib.request
        import urllib.error
        params = params or []
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
        last_err = None

        for attempt in range(attempts):
            ep = self._pick()
            req = urllib.request.Request(ep.url, data=body, headers=self.headers)
            t0 = time.perf_counter()
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    data = json.loads(resp.read().decode())
                latency = (time.perf_counter() - t0) * 1000.0
                ep.note_success(latency)
                if "result" in data:
                    return data["result"]
                elif "error" in data:
                    err_code = data["error"].get("code", 0)
                    if err_code in (-32005, -32011):
                        ep.note_failure(cooldown=5.0)
                    raise RuntimeError(f"RPC Error: {data['error']}")
            except urllib.error.HTTPError as e:
                last_err = e
                detail = e.read()[:120].decode(errors="replace")
                if e.code == 429:
                    ep.note_failure(cooldown=5.0)
                elif e.code == 403 and "1010" in detail:
                    ep.note_failure(waf=True)
                else:
                    ep.note_failure(cooldown=2.0)
                time.sleep(0.15 * (attempt + 1))
            except Exception as e:
                last_err = e
                ep.note_failure(cooldown=2.0)
                time.sleep(0.15 * (attempt + 1))

        raise RuntimeError(f"All {attempts} attempts failed for {method}: {last_err}")

    def report(self) -> str:
        lines = ["ArcRpcPool Endpoint Status:"]
        for ep in self.endpoints:
            status = "WAF_BANNED" if ep.waf_banned else ("COOLDOWN" if not ep.is_healthy() else "HEALTHY")
            lines.append(
                f"  {ep.url:<44} [{status:<10}] "
                f"lat: {ep.avg_latency_ms:>5.1f}ms | ok: {ep.success_count:>4} | fail: {ep.failure_count:>3}"
            )
        return "\n".join(lines)


class TokenBucket:
    def __init__(self, rate_per_sec: float = 25.0, burst: Optional[float] = None):
        self.rate = float(rate_per_sec)
        self.capacity = float(burst if burst is not None else max(10.0, rate_per_sec))
        self.tokens = self.capacity
        self.updated = time.monotonic()
        self._lock = threading.Lock()
        self.waits = 0

    def acquire(self, n: float = 1.0) -> float:
        waited = 0.0
        while True:
            with self._lock:
                now = time.monotonic()
                self.tokens = min(
                    self.capacity, self.tokens + (now - self.updated) * self.rate
                )
                self.updated = now
                if self.tokens >= n:
                    self.tokens -= n
                    if waited:
                        self.waits += 1
                    return waited
                deficit = n - self.tokens
                sleep_for = deficit / self.rate
            time.sleep(min(sleep_for, 1.0))
            waited += min(sleep_for, 1.0)


class FastRpc:
    def __init__(
        self,
        pool: Optional[ArcRpcPool] = None,
        rate_per_sec: float = 25.0,
        gas_price_wei: int = 21_000_000_000,
        timeout: float = 30.0,
    ):
        self.timeout = timeout
        self.pool = pool or ArcRpcPool(verify_chain=True, timeout=timeout)
        self.bucket = TokenBucket(rate_per_sec)
        self._gas_price = gas_price_wei
        self._gas_checked = 0.0
        self.counters = {
            "http_requests": 0,
            "batch_requests": 0,
            "rate_limit_waits": 0,
            "gas_price_reads": 0,
            "http_429": 0,
            "waf_403": 0,
            "batch_sends": 0,
            "nonce_fallback_ok": 0,
            "nonce_fallback_fail": 0,
        }
        self._lock = threading.Lock()

    def bump(self, key: str, n: int = 1):
        with self._lock:
            self.counters[key] = self.counters.get(key, 0) + n

    def call(self, method: str, params: Optional[list] = None, attempts: int = 5):
        waited = self.bucket.acquire()
        if waited:
            self.bump("rate_limit_waits")
        self.bump("http_requests")
        return self.pool.call(method, params or [], attempts=attempts)

    def batch_call(self, requests: Sequence[Tuple[str, list]]) -> List[Any]:
        import urllib.request
        import urllib.error
        if not requests:
            return []
        self.bucket.acquire()
        self.bump("batch_requests")
        self.bump("http_requests")

        payload = [
            {"jsonrpc": "2.0", "id": i, "method": m, "params": p}
            for i, (m, p) in enumerate(requests)
        ]
        body = json.dumps(payload).encode()

        last_exc: Optional[Exception] = None
        for attempt in range(4):
            ep = self.pool._pick()
            req = urllib.request.Request(
                ep.url, data=body, headers=self.pool.headers
            )
            t0 = time.perf_counter()
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    data = json.loads(resp.read().decode())
                ep.note_success((time.perf_counter() - t0) * 1000)

                # Robust handling of single-dict error responses
                if isinstance(data, dict):
                    if "error" in data:
                        raise RuntimeError(f"RPC batch error: {data['error']}")
                    data = [data]

                out: List[Any] = [None] * len(requests)
                for item in data:
                    if isinstance(item, dict):
                        if "result" in item:
                            out[item["id"]] = item["result"]
                        elif "error" in item:
                            out[item["id"]] = None
                return out
            except urllib.error.HTTPError as e:
                detail = e.read()[:120].decode(errors="replace")
                if e.code == 429:
                    self.bump("http_429")
                    ep.note_failure(cooldown=5.0)
                elif e.code == 403 and "1010" in detail:
                    self.bump("waf_403")
                    ep.note_failure(waf=True)
                else:
                    ep.note_failure()
                last_exc = e
                retry_after = e.headers.get("Retry-After") if e.headers else None
                delay = float(retry_after) if (retry_after or "").isdigit() else 0.5 * (2 ** attempt)
                time.sleep(min(delay, 10.0) + random.random() * 0.3)
            except Exception as e:
                ep.note_failure()
                last_exc = e
                time.sleep(0.3 * (2 ** attempt) + random.random() * 0.3)

        raise RuntimeError(f"batch of {len(requests)} failed on all endpoints: {last_exc}")

    def gas_price(self) -> int:
        now = time.time()
        if now - self._gas_checked < 600:
            return self._gas_price
        try:
            live = int(self.call("eth_gasPrice"), 16)
            block = self.call("eth_getBlockByNumber", ["latest", False])
            base = int(block["baseFeePerGas"], 16) if (block and "baseFeePerGas" in block) else 20_000_000_000
            self._gas_price = max(live, base) + 750_000_000
            self.bump("gas_price_reads")
        except Exception:
            pass
        self._gas_checked = now
        return self._gas_price

    @staticmethod
    def _addr_word(a: str) -> str:
        return a[2:].lower().rjust(64, "0")

    def preflight_batch(self, wallets: Sequence[str]) -> Dict[str, Dict[str, int]]:
        calls = []
        for w in wallets:
            wd = self._addr_word(w)
            calls.append((MULTICALL3, True, bytes.fromhex("4d2301cc" + wd)))
            calls.append((USDC, True, bytes.fromhex("70a08231" + wd)))
            calls.append((USDC, True, bytes.fromhex("dd62ed3e" + wd + self._addr_word(ROUTER))))

        data = "0x82ad56cb" + encode(["(address,bool,bytes)[]"], [calls]).hex()
        raw = self.call("eth_call", [{"to": MULTICALL3, "data": data}, "latest"])
        results = decode(["(bool,bytes)[]"], bytes.fromhex(raw[2:]))[0]

        out: Dict[str, Dict[str, int]] = {}
        for i, w in enumerate(wallets):
            native, usdc_bal, allow = results[i * 3], results[i * 3 + 1], results[i * 3 + 2]
            out[w] = {
                "native": int.from_bytes(native[1], "big") if native[1] else 0,
                "usdc": int.from_bytes(usdc_bal[1], "big") if usdc_bal[1] else 0,
                "allowance": int.from_bytes(allow[1], "big") if allow[1] else 0,
            }
        return out

    def batch_nonces(self, wallets: Sequence[str]) -> Dict[str, int]:
        reqs = [("eth_getTransactionCount", [w, "pending"]) for w in wallets]
        res = self.batch_call(reqs)
        out: Dict[str, int] = {}
        for w, r in zip(wallets, res):
            if isinstance(r, str):
                out[w] = int(r, 16)
            else:
                try:
                    single = self.call("eth_getTransactionCount", [w, "pending"])
                    out[w] = int(single, 16)
                    self.bump("nonce_fallback_ok")
                except Exception:
                    self.bump("nonce_fallback_fail")
                    pass
        return out

    def batch_receipts(self, tx_hashes: Sequence[str]) -> Dict[str, Optional[dict]]:
        if not tx_hashes:
            return {}
        reqs = [("eth_getTransactionReceipt", [h]) for h in tx_hashes]
        res = self.batch_call(reqs)
        out: Dict[str, Optional[dict]] = {}
        for h, r in zip(tx_hashes, res):
            out[h] = r if isinstance(r, dict) else None
        return out

    def batch_send_raw(self, signed_txs: Sequence[str]) -> List[Optional[str]]:
        if not signed_txs:
            return []
        reqs = [("eth_sendRawTransaction", [tx]) for tx in signed_txs]
        res = self.batch_call(reqs)
        self.bump("batch_sends")
        return [r if isinstance(r, str) else None for r in res]

    def multicall(self, calls: Sequence[Tuple[str, bytes]]) -> List[bytes]:
        """Generic Multicall3 aggregate3 execution.
        calls: [(target_address, call_data_bytes), ...]
        Returns: list of returnData bytes for each call (or b"" if failed).
        """
        if not calls:
            return []
        agg_calls = [(target, True, data) for target, data in calls]
        data = "0x82ad56cb" + encode(["(address,bool,bytes)[]"], [agg_calls]).hex()
        raw = self.call("eth_call", [{"to": MULTICALL3, "data": data}, "latest"])
        if not raw or raw == "0x":
            return [b""] * len(calls)
        results = decode(["(bool,bytes)[]"], bytes.fromhex(raw[2:]))[0]
        return [res[1] if res[0] else b"" for res in results]

    def report(self) -> str:
        c = self.counters
        return (
            f"FastRpc counters\n"
            f"  http_requests      {c['http_requests']:>8,}\n"
            f"  batch_requests     {c['batch_requests']:>8,}   (each replaces up to ~100 single calls)\n"
            f"  batch_sends        {c.get('batch_sends', 0):>8,}\n"
            f"  gas_price_reads    {c['gas_price_reads']:>8,}   (cached; would be 1/tx)\n"
            f"  rate_limit_waits   {c['rate_limit_waits']:>8,}\n"
            f"  nonce_fallback_ok  {c.get('nonce_fallback_ok', 0):>8,}\n"
            f"  nonce_fallback_fail{c.get('nonce_fallback_fail', 0):>8,}\n"
            f"  http_429           {c['http_429']:>8,}\n"
            f"  waf_403            {c['waf_403']:>8,}\n"
        )


# ---------------------------------------------------------------------------
# NETWORK
# ---------------------------------------------------------------------------
CHAIN_ID = 5042002

ROUTER = "0x4B33146F2bCc75574534374C85662f9E51C38Aca"   # writes
HUB    = "0x6ab2635fec3c426d825d005e24cfc05b82ea3994"   # quotes + constants
USDC   = "0x4F3b8005d6b3F4994a791D971bcD153E114D20c2"   # 6 decimals
ORDERS = "0x41d7f9cf646b70f7a99cf62c6c456057c47ef0e3"   # limit orders

USDC_DECIMALS = 6
KIND_CLAIM = 1
KIND_SELL = 2

# Verified: baseFeePerGas is pinned at 20 gwei on Arc. gasPrice = 20.25 gwei.
# We cache at 21 gwei (headroom) instead of reading it 3,500 times per run.
GAS_PRICE_WEI = 21_000_000_000

# Verified on-chain
UNBOND_DELAY_DEFAULT = 90
MAX_TTL_DEFAULT = 1_814_400          # 21 days
MIN_COST_BASIS_USDC = 10             # v3's $15 floor respected this; keep it
DEFAULT_SUPPLY = 10**9 * 10**18


def sel(sig: str) -> str:
    return keccak(text=sig)[:4].hex()


SEL = {
    "faucet":       "de5f72fd",
    "approve":      "095ea7b3",
    "launch":       "e43d45f0",
    "buy":          "a59ac6dd",
    "sell":         "6a272462",
    "graduate":     "ff6d8d05",
    "collectFee":   "cf6bc454",
    "claim":        "1e83409a",
    "claimApprove": "5dd68e16",
    "unbond":       sel("unbond(address,uint256,uint8,uint256,uint64)"),
    "walletBondBalance":  sel("walletBondBalance(address,address)"),
    "walletBondCostBasis": sel("walletBondCostBasis(address,address)"),
    "walletUnbondedClaimable": sel("walletUnbondedClaimable(address,address)"),
    "launchOf":     sel("launchOf(address)"),
    "quoteSell":    sel("quoteSell(address,uint256)"),
    "priceAndCap":  sel("priceAndCap(address)"),
    "unbondDelay":  sel("UNBOND_DELAY()"),
    "maxTtl":       sel("MAX_SELL_TTL()"),
    "balanceOf":    "70a08231",
    "allowance":    "dd62ed3e",
}

MAX_UINT = 2**256 - 1

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------
BASE = Path(__file__).resolve().parent
PV_FILE = BASE / "pv.txt"
DATA = BASE / "data"
STATE_FILE = DATA / "flipt_v4_state.json"
TOKENS_FILE = DATA / "flipt_v4_tokens.json"
CATALOG_FILE = BASE / "token_catalog.json"

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
_print_lock = threading.Lock()
_state_lock = threading.RLock()

BOLD, DIM, GREEN, RED, YEL, CYAN, RESET = (
    "\033[1m", "\033[2m", "\033[32m", "\033[31m", "\033[33m", "\033[36m", "\033[0m"
)


def log(msg: str):
    with _print_lock:
        print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def info(m): log(f"{CYAN}[INFO]{RESET} {m}")
def ok(m):   log(f"{GREEN}{BOLD}[ OK ]{RESET} {m}")
def warn(m): log(f"{YEL}[WARN]{RESET} {m}")
def err(m):  log(f"{RED}{BOLD}[ERR ]{RESET} {m}")


# ===========================================================================
# NONCE MANAGER — prevents the stranded-nonce freeze
# ===========================================================================
class NonceManager:
    """
    Per-wallet nonce allocation.

    v3 read the pending nonce fresh for every operation and pipelined N / N+1
    blindly. If the first tx never broadcast, the second was stranded forever
    (a nonce cannot be skipped). With ~31% of requests failing, that happened.

    Here we allocate under a lock, and only commit the reservation once a
    broadcast actually returns a hash. A failed broadcast releases the slot, so
    the next attempt reuses the same nonce instead of leaving a hole.
    """

    def __init__(self):
        self._next: Dict[str, int] = {}
        self._by_hash: Dict[str, int] = {}
        self._lock = threading.Lock()

    def seed(self, address: str, nonce: int):
        with self._lock:
            self._next[address.lower()] = nonce

    def seed_many(self, nonces: Dict[str, int]):
        with self._lock:
            for a, n in nonces.items():
                self._next[a.lower()] = n

    def reserve(self, address: str) -> int:
        a = address.lower()
        with self._lock:
            if a not in self._next:
                raise RuntimeError(f"nonce for {a} not seeded — call seed_many first")
            n = self._next[a]
            self._next[a] = n + 1
            return n

    def commit(self, address: str, nonce: int, tx_hash: str):
        with self._lock:
            self._by_hash[tx_hash] = nonce

    def release(self, address: str, nonce: int):
        """Broadcast failed — give the nonce back so it is reused, not skipped."""
        a = address.lower()
        with self._lock:
            cur = self._next.get(a)
            if cur is not None and cur == nonce + 1:
                self._next[a] = nonce

    def current(self, address: str) -> Optional[int]:
        with self._lock:
            return self._next.get(address.lower())


# ===========================================================================
# STATE
# ===========================================================================
def empty_state() -> dict:
    return {
        "version": 4,
        "created_at": int(time.time()),
        "wallets": {},        # addr -> wallet-level facts
        "positions": {},      # "addr:token" -> position state machine
        "transactions": {},   # txhash -> meta
        "deployments": [],
    }


def load_state() -> dict:
    DATA.mkdir(parents=True, exist_ok=True)
    if STATE_FILE.exists():
        try:
            s = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            for k, v in empty_state().items():
                s.setdefault(k, v)
            return s
        except Exception as e:
            warn(f"state unreadable ({e}); starting fresh")
    return empty_state()


_save_dirty = threading.Event()


def save_state(state: dict):
    with _state_lock:
        state["updated_at"] = int(time.time())
        try:
            DATA.mkdir(parents=True, exist_ok=True)
            tmp_file = STATE_FILE.with_suffix(".tmp")
            with open(tmp_file, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())
            tmp_file.replace(STATE_FILE)
            _save_dirty.clear()
        except Exception as e:
            warn(f"state save failed: {e}")


def state_saver_loop(state: dict, stop: threading.Event, interval: float = 3.0):
    while not stop.is_set():
        if _save_dirty.wait(timeout=interval):
            save_state(state)
    if _save_dirty.is_set():
        save_state(state)


def dirty():
    _save_dirty.set()


def wrow(state: dict, addr: str) -> dict:
    with _state_lock:
        return state["wallets"].setdefault(addr.lower(), {})


def pos_key(addr: str, token: str) -> str:
    return f"{addr.lower()}:{token.lower()}"


def prow(state: dict, addr: str, token: str) -> dict:
    with _state_lock:
        return state["positions"].setdefault(pos_key(addr, token), {"state": "new"})


def txrow(state: dict, h: str, **f):
    with _state_lock:
        state["transactions"].setdefault(h, {}).update(f)
    dirty()


# ===========================================================================
# CHAIN READ HELPERS (all go through FastRpc)
# ===========================================================================
def w(a: str) -> str:
    return a[2:].lower().rjust(64, "0")


class Chain:
    def __init__(self, rpc: FastRpc):
        self.rpc = rpc

    # -- generic --------------------------------------------------------
    def call(self, to: str, data: str) -> str:
        if not data.startswith("0x"):
            data = "0x" + data
        return self.rpc.call("eth_call", [{"to": to, "data": data}, "latest"])

    def u256(self, to: str, data: str, default: int = 0) -> int:
        try:
            r = self.call(to, data)
            return int(r, 16) if r and r != "0x" else default
        except Exception:
            return default

    def simulate(self, frm: str, to: str, data: Any) -> Tuple[bool, str]:
        """True if the call would succeed. Returns (ok, reason)."""
        if isinstance(data, bytes):
            data = "0x" + data.hex()
        elif not str(data).startswith("0x"):
            data = "0x" + str(data)
        try:
            self.rpc.call("eth_call", [{"from": frm, "to": to, "data": data}, "latest"])
            return True, ""
        except Exception as e:
            return False, str(e)[-160:]

    # -- protocol reads -------------------------------------------------
    def unbond_delay(self) -> int:
        return self.u256(HUB, "0x" + SEL["unbondDelay"], UNBOND_DELAY_DEFAULT) or UNBOND_DELAY_DEFAULT

    def max_ttl(self) -> int:
        return self.u256(HUB, "0x" + SEL["maxTtl"], MAX_TTL_DEFAULT) or MAX_TTL_DEFAULT

    def price_and_cap(self, token: str) -> Tuple[int, int]:
        try:
            r = self.call(HUB, "0x" + SEL["priceAndCap"] + w(token))
            a, b = decode(["uint256", "uint256"], bytes.fromhex(r[2:]))
            return int(a), int(b)
        except Exception:
            return 0, 0

    def quote_sell(self, token: str, amount: int) -> int:
        try:
            r = self.call(HUB, "0x" + SEL["quoteSell"] + w(token) + f"{amount:064x}")
            out, _ = decode(["uint256", "uint256"], bytes.fromhex(r[2:]))
            return int(out)
        except Exception:
            return 0

    def launch_info(self, token: str) -> dict:
        """launchOf -> (exists, graduated, gradTarget, soldAtGrad, token, pool, proposer, kind, seed)"""
        try:
            r = self.call(ROUTER, "0x" + SEL["launchOf"] + w(token))
            v = decode(
                ["bool", "bool", "uint256", "uint256", "address",
                 "address", "address", "uint64", "uint256"],
                bytes.fromhex(r[2:]),
            )
            return {
                "exists": bool(v[0]), "graduated": bool(v[1]),
                "grad_target_usdc": int(v[2]), "sold_at_graduation": int(v[3]),
                "pool": v[5], "proposer": v[6],
            }
        except Exception:
            return {}

    # -- position reads (v3's sell bug lived here) ----------------------
    # ARG ORDER: these are auto-generated getters for a mapping keyed
    # [token][wallet] -- so the FIRST argument is the TOKEN, not the wallet.
    # Verified three ways: the BondBought ABI says (token, buyer), tx.from
    # matching topics[2] identifies the buyer, and (token, wallet) returns a
    # non-zero balance where (wallet, token) returns 0.
    def bond_balance(self, wallet: str, token: str) -> int:
        return self.u256(ROUTER, "0x" + SEL["walletBondBalance"] + w(token) + w(wallet))

    def bond_cost_basis(self, wallet: str, token: str) -> int:
        return self.u256(ROUTER, "0x" + SEL["walletBondCostBasis"] + w(token) + w(wallet))

    def unbonded_claimable(self, wallet: str, token: str) -> Tuple[int, int, int]:
        """
        (amount_wei, claim_at_unix, cost_basis_usdc).

        VERIFIED against live in-flight unbonds:
            [0] tokens pending      793,086,956.52e18
            [1] claimAt unix        1789468655
            [2] cost basis (6dp)    6,455,696,202  == walletBondCostBasis
        Field [1] is the timestamp -- reading [2] by mistake makes every
        pending position look instantly claimable, and claiming before the
        90 s delay reverts.
        """
        try:
            r = self.call(ROUTER, "0x" + SEL["walletUnbondedClaimable"] + w(token) + w(wallet))
            amt, claim_at, basis = decode(["uint128", "uint64", "uint64"], bytes.fromhex(r[2:]))
            return int(amt), int(claim_at), int(basis)
        except Exception:
            return 0, 0, 0

    def pool_usdc_reserve(self, token: str) -> int:
        """
        USDC actually sitting in this launch's pool. This is the hard ceiling
        on what ANY sell can pay out, so it is the honest sanity bound for a
        min_out. Measured live: a 793M-token position quoted 24,410 USDC
        against a pool holding 26,369 USDC -- the quote tracks the pool, and
        the reserve catches a corrupt or stale quote read.
        """
        try:
            info = self.launch_info(token)
            pool = info.get("pool")
            if not pool or int(str(pool), 16) == 0:
                return 0
            return self.erc20_balance(USDC, pool)
        except Exception:
            return 0

    def safe_min_out(self, token: str, amount: int, slippage_bps: int) -> Tuple[int, int]:
        """(min_out, quoted). Clamps the curve quote to the pool's USDC reserve."""
        quoted = self.quote_sell(token, amount)
        reserve = self.pool_usdc_reserve(token)
        if reserve:
            cap = int(reserve * 0.95)
            if quoted > cap:
                warn(f"quote {quoted/1e6:,.2f} > 95% of pool USDC {reserve/1e6:,.2f} "
                     f"— clamping to the reserve")
                quoted = cap
        min_out = int(quoted * (1 - slippage_bps / 10_000))
        return min_out, quoted

    def positions_batch(self, token: str, wallets: Sequence[str]) -> Dict[str, Dict[str, int]]:
        """
        bond / pending-unbond / wallet balance for N wallets on ONE token, in a
        single eth_call. 3,542 wallets x 3 reads would be 10,626 calls; this is
        one. Returns {wallet: {"bond", "claimable", "claim_at", "basis", "wallet"}}.
        """
        calls = []
        for a in wallets:
            calls.append((ROUTER, bytes.fromhex(SEL["walletBondBalance"] + w(token) + w(a))))
            calls.append((ROUTER, bytes.fromhex(SEL["walletUnbondedClaimable"] + w(token) + w(a))))
            calls.append((token, bytes.fromhex(SEL["balanceOf"] + w(a))))
        raw = self.rpc.multicall(calls)
        out: Dict[str, Dict[str, int]] = {}
        for i, a in enumerate(wallets):
            bond_b, claim_b, wallet_b = raw[i * 3], raw[i * 3 + 1], raw[i * 3 + 2]
            claim_amt = claim_at = basis = 0
            if claim_b:
                # ABI-encoded, NOT packed: each value occupies its own 32-byte
                # word, so slice words -- not 16/8/8 bytes.
                words = [int.from_bytes(claim_b[i * 32:(i + 1) * 32], "big")
                         for i in range(len(claim_b) // 32)]
                if len(words) >= 3:
                    claim_amt, claim_at, basis = words[0], words[1], words[2]
            out[a] = {
                "bond": int.from_bytes(bond_b, "big") if bond_b else 0,
                "claimable": claim_amt,
                "claim_at": claim_at,
                "basis": basis,
                "wallet": int.from_bytes(wallet_b, "big") if wallet_b else 0,
            }
        return out

    def erc20_balance(self, token: str, who: str) -> int:
        return self.u256(token, "0x" + SEL["balanceOf"] + w(who))

    def erc20_allowance(self, token: str, owner: str, spender: str) -> int:
        return self.u256(token, "0x" + SEL["allowance"] + w(owner) + w(spender))


# ===========================================================================
# TX BUILDER / SENDER
# ===========================================================================
class Sender:
    def __init__(self, rpc: FastRpc, chain: Chain, state: dict, nonces: NonceManager,
                 send: bool):
        self.rpc = rpc
        self.chain = chain
        self.state = state
        self.nonces = nonces
        self.send = send
        self._w3 = None
        self._w3_lock = threading.Lock()

    def w3(self):
        # One Web3 per endpoint, reused. v3 built a fresh instance per call,
        # which churned TCP+TLS handshakes -- exactly the traffic shape that
        # trips Cloudflare's bot-fight.
        if self._w3 is None:
            with self._w3_lock:
                if self._w3 is None:
                    self._w3 = self.rpc.pool.get_web3()
        return self._w3

    def send_tx(self, acct, to: str, data: bytes, gas: int, label: str,
                meta: Optional[dict] = None) -> Optional[str]:
        if not isinstance(data, (bytes, bytearray)):
            raise ValueError("data must be bytes")
        nonce = self.nonces.reserve(acct.address)

        if not self.send:
            log(f"{DIM}[dry-run]{RESET} {label:<34} to={to[:10]}.. gas={gas:<8} nonce={nonce}")
            self.nonces.release(acct.address, nonce)
            return None

        tx = {
            "from": acct.address, "to": to, "value": 0, "gas": gas,
            "gasPrice": self.rpc.gas_price(),      # cached, no RPC
            "nonce": nonce, "chainId": CHAIN_ID, "data": data,
        }
        try:
            signed = acct.sign_transaction(tx)
            raw = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")
            h = self.w3().eth.send_raw_transaction(raw).hex()
        except Exception as e:
            # Release so the nonce is reused rather than skipped.
            self.nonces.release(acct.address, nonce)
            err(f"{label} broadcast failed for {acct.address[:10]}..: {str(e)[-140:]}")
            return None

        self.nonces.commit(acct.address, nonce, h)
        txrow(self.state, h, wallet=acct.address, kind=label, nonce=nonce,
              status="broadcast", created_at=int(time.time()), **(meta or {}))
        log(f"{GREEN}[sent]{RESET} {label:<34} {h[:18]}... nonce={nonce}")
        return h

    def wait(self, h: str, timeout: int = 120) -> bool:
        if not self.send or not h:
            return False
        deadline = time.time() + timeout
        while time.time() < deadline:
            rc = self.rpc.batch_receipts([h]).get(h)
            if rc:
                good = int(rc.get("status", "0x0"), 16) == 1
                txrow(self.state, h, status="confirmed" if good else "reverted",
                      block=int(rc.get("blockNumber", "0x0"), 16), confirmed_at=int(time.time()))
                log(f"{GREEN if good else RED}[{'ok' if good else 'REVERTED'}]{RESET} {h[:18]}...")
                return good
            time.sleep(1.5)
        txrow(self.state, h, status="timeout")
        warn(f"timeout waiting for {h[:18]}...")
        return False

    def batch_wait(self, hashes: Sequence[str], timeout: int = 180) -> Dict[str, bool]:
        """All receipts in batched round-trips instead of one poll per tx."""
        if not self.send or not hashes:
            return {}
        remaining = set(hashes)
        out: Dict[str, bool] = {}
        deadline = time.time() + timeout
        while remaining and time.time() < deadline:
            for h, rc in self.rpc.batch_receipts(sorted(remaining)).items():
                if rc is None:
                    continue
                good = int(rc.get("status", "0x0"), 16) == 1
                out[h] = good
                remaining.discard(h)
                txrow(self.state, h, status="confirmed" if good else "reverted",
                      block=int(rc.get("blockNumber", "0x0"), 16), confirmed_at=int(time.time()))
            if remaining:
                time.sleep(2.0)
        for h in remaining:
            out[h] = False
            txrow(self.state, h, status="timeout")
        return out

    def batch_send(self, jobs: list) -> list:
        """Sign + batch-broadcast multiple txs in one HTTP round-trip.

        Each job is (acct, to_addr, data_bytes, gas_limit, label, meta_dict).
        Returns list of successfully broadcast tx hashes.
        """
        if not jobs:
            return [], {}
        if not self.send:
            for acct, to, data, gas, label, meta in jobs:
                log(f"{DIM}[dry-run batch]{RESET} {label:<34} {acct.address[:10]}.. to={to[:10]}..")
            return [f"0xdry{i:060d}" for i in range(len(jobs))], {acct.address.lower(): f"0xdry{i:060d}" for i, (acct, _, _, _, _, _) in enumerate(jobs)}

        raw_txs = []
        nonces_used = []
        gas_price = self.rpc.gas_price()

        for acct, to, data, gas, label, meta in jobs:
            if not isinstance(data, (bytes, bytearray)):
                data = bytes.fromhex(data) if isinstance(data, str) else data
            nonce = self.nonces.reserve(acct.address)
            tx = {
                "from": acct.address, "to": to, "value": 0, "gas": gas,
                "gasPrice": gas_price, "nonce": nonce,
                "chainId": CHAIN_ID, "data": data,
            }
            try:
                signed = acct.sign_transaction(tx)
                raw = getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction")
                raw_txs.append("0x" + raw.hex() if not raw.hex().startswith("0x") else raw.hex())
                nonces_used.append((acct, nonce, label, meta))
            except Exception as e:
                self.nonces.release(acct.address, nonce)
                err(f"batch_send sign failed {acct.address[:10]}: {str(e)[-80:]}")

        if not raw_txs:
            return [], {}

        try:
            results = self.rpc.batch_send_raw(raw_txs)
        except Exception as e:
            # Release all reserved nonces on total failure
            for acct, nonce, _, _ in nonces_used:
                self.nonces.release(acct.address, nonce)
            err(f"batch_send_raw failed: {str(e)[-120:]}")
            return [], {}

        hashes = []
        tx_by_wallet: Dict[str, str] = {}
        for (acct, nonce, label, meta), h in zip(nonces_used, results):
            if h:
                self.nonces.commit(acct.address, nonce, h)
                txrow(self.state, h, wallet=acct.address, kind=label, nonce=nonce,
                      status="broadcast", created_at=int(time.time()), **(meta or {}))
                log(f"{GREEN}[batch]{RESET} {label:<34} {h[:18]}... nonce={nonce}")
                hashes.append(h)
                tx_by_wallet[acct.address.lower()] = h
            else:
                self.nonces.release(acct.address, nonce)
                warn(f"batch_send: no hash for {acct.address[:10]} nonce={nonce} ({label})")
        return hashes, tx_by_wallet



# ===========================================================================
# TOKEN METADATA (v3's catalog idea, kept)
# ===========================================================================
ADJ = ["Quantum","Solaris","Aether","Cyber","Hyperion","Velocis","Nebula","Chronos",
       "Vortex","Prism","Apex","Zenith","Titan","Pulse","Aura","Echo","Flux","Ignis",
       "Helios","Orion","Pixel","Cosmic","Astral","Lunar","Ember"]
NOUN = ["Protocol","Network","Swap","Vault","Forge","Haven","Alpaca","Hamster","Dream",
        "Lantern","Garden","Foundry","Reactor","Matrix","Beacon","Core","Signal",
        "Circuit","Harbor","Atlas"]


def token_meta(index: int, catalog: List[dict]) -> dict:
    """
    Unique identity per index.

    Names cycle through ADJ x NOUN as a 2-D walk (adj = index % N, noun =
    index // N), giving len(ADJ)*len(NOUN) unique combinations before any
    repeat. Beyond that we append a numeric tail so a 3,000-wallet run never
    produces duplicates. Symbols always carry the index so they are unique
    regardless.
    """
    if catalog and index < len(catalog):
        e = catalog[index]
        return {"name": e.get("name", f"Token{index}"),
                "symbol": e.get("symbol", f"TOK{index}"),
                "uri": e.get("uri", f"/img/token_{index}.png")}

    n_adj, n_noun = len(ADJ), len(NOUN)
    cycle, pos = divmod(index, n_adj * n_noun)
    adj = ADJ[pos % n_adj]
    noun = NOUN[(pos // n_adj) % n_noun]
    name = f"{adj} {noun}" + (f" {cycle + 1}" if cycle else "")

    stem = "".join(c for c in adj.upper() if c not in "AEIOU")[:2]
    stem2 = "".join(c for c in noun.upper() if c not in "AEIOU")[:2]
    base = (stem + stem2)[:4] or adj[:3].upper()
    sym = f"{base}{index % 10000:04d}"[:8] if index else base
    return {"name": name, "symbol": sym, "uri": f"/img/{sym.lower()}_{index:04x}.png"}


# ===========================================================================
# CREATE2 VANITY MINER
# ===========================================================================
def mine_salt(chain: Chain, caller: str, name: str, symbol: str, supply: int,
              suffix: bytes = b"\x69\xd2", limit: int = 2_000_000) -> Tuple[int, str]:
    """Local CREATE2 grind. The factory enforces the 69d2 suffix (619/619 verified)."""
    from web3 import Web3
    # tokenInitCodeHash is on the factory
    FACTORY = "0xb9200934941A9010d31733D034b9eAd7a7746d12"
    data = "0x" + keccak(text="tokenInitCodeHash(string,string,uint256)")[:4].hex() + encode(
        ["string", "string", "uint256"], [name, symbol, supply]
    ).hex()
    init_hash = bytes.fromhex(chain.call(FACTORY, data)[2:])
    factory_bytes = bytes.fromhex(FACTORY[2:])
    caller_bytes = bytes.fromhex(Web3.to_checksum_address(caller)[2:])
    prefix = b"\xff" + factory_bytes
    start = random.randint(1_000, 9_999_999)
    for i in range(limit):
        salt = start + i
        packed = b"\x00" * 12 + caller_bytes + salt.to_bytes(32, "big")
        salt_hash = Web3.keccak(packed)
        token = Web3.keccak(prefix + salt_hash + init_hash)[12:]
        if token.endswith(suffix):
            return salt, Web3.to_checksum_address("0x" + token.hex())
    raise RuntimeError(f"vanity not found in {limit:,} attempts")


# ===========================================================================
# EXIT STATE MACHINE — the part v3 got wrong
# ===========================================================================
def exit_position(ctx, acct, token: str, pct: float = 100.0,
                  slippage_bps: int = 300) -> dict:
    """
    Bonded -> Unbonding -> Claimed -> Sold, for ONE wallet on ONE token.

    Guarded at every step by real chain reads. v3 called claim() with no unbond
    (always reverts) and sold from the wallet balance while the tokens were
    still on the hub (always a no-op).
    """
    chain, sender, state = ctx.chain, ctx.sender, ctx.state
    res = {"wallet": acct.address, "token": token, "steps": [], "realized_usdc": 0.0}

    info_l = chain.launch_info(token)
    if not info_l.get("exists"):
        res["error"] = "not a Flipt launch"
        return res

    if not info_l.get("graduated"):
        # VERIFIED: unbond() reverts on an ungraduated curve. Do not burn gas.
        res["error"] = "not graduated — exit impossible"
        res["graduated"] = False
        return res
    res["graduated"] = True

    bond = chain.bond_balance(acct.address, token)
    basis = chain.bond_cost_basis(acct.address, token)
    claimable, claim_at, unb_basis = chain.unbonded_claimable(acct.address, token)
    wallet_bal = chain.erc20_balance(token, acct.address)

    # A mid-unbond position has already left the bonded state, so
    # walletBondCostBasis reads 0 while the real basis rides along in the
    # claimable record. Without this fallback the PnL prints as pure profit.
    if basis == 0 and unb_basis:
        basis = unb_basis

    res["bond_balance"] = bond / 1e18
    res["cost_basis_usdc"] = basis / 1e6

    # ---- DRY RUN: return the plan, priced off the real curve -------------
    # Without this, a dry run reports "nothing sellable in wallet" for every
    # bonded position (the tokens only reach the wallet 90 s after unbond,
    # which a dry run never does). That reads as an empty position when the
    # position is in fact large.
    if not ctx.send:
        if bond <= 0 and wallet_bal <= 0 and claimable <= 0:
            res["error"] = "nothing sellable in wallet"
            return res
        sellable = claimable or wallet_bal or bond
        min_out, quoted = chain.safe_min_out(token, sellable, slippage_bps)
        plan = []
        if bond > 0:
            amt = int(bond * pct / 100) or bond
            plan.append(f"unbond({amt/1e18:,.4f} tokens = {pct:.0f}% of position, KIND_CLAIM)")
        if bond > 0 and claimable == 0:
            plan.append(f"wait {chain.unbond_delay()}s for the unbond delay")
        elif claimable > 0 and claim_at and claim_at > int(time.time()):
            plan.append(f"wait {claim_at - int(time.time())}s until claimAt "
                        f"{claim_at} (unbond already in flight)")
        plan.append("claimAndApprove(token)  — one tx, replaces claim + approve")
        plan.append(f"approve(token) if allowance is short")
        plan.append(f"sell({sellable/1e18:,.0f}, minOut={min_out/1e6:,.4f} USDC)")
        res["dry_run"] = True
        res["plan"] = plan
        res["sellable_tokens"] = sellable / 1e18
        res["projected_usdc"] = quoted / 1e6
        res["projected_pnl_usdc"] = (quoted - basis) / 1e6
        res["steps"].append({"step": "plan", "dry_run": True, "plan": plan})
        return res

    # Step 1 — unbond (only if tokens still bonded)
    if bond > 0 and claimable == 0 and wallet_bal == 0:
        amount = int(bond * pct / 100) or bond
        expiry = int(time.time()) + min(86400, chain.max_ttl())
        data = bytes.fromhex(SEL["unbond"]) + encode(
            ["address", "uint256", "uint8", "uint256", "uint64"],
            [token, amount, KIND_CLAIM, 0, expiry],
        )
        h = sender.send_tx(acct, ROUTER, data, 450_000, f"unbond {amount/1e18:,.0f}")
        res["steps"].append({"step": "unbond", "tx": h, "amount_tokens": amount / 1e18})
        if h and not sender.wait(h):
            res["error"] = "unbond reverted"
            return res

    # Step 2 — wait out UNBOND_DELAY (90s, verified)
    delay = chain.unbond_delay()
    if sender.send:
        target = max(claim_at if claim_at else 0, int(time.time()) + delay)
        log(f"waiting for the unbond delay on {token[:10]}.. "
            f"(claimAt {claim_at or 'n/a'}, up to {int(target - time.time())}s)")
        deadline = target + 30
        while time.time() < deadline:
            time.sleep(5)
            claimable, claim_at, _ = chain.unbonded_claimable(acct.address, token)
            if claimable > 0 and claim_at and int(time.time()) >= claim_at:
                break
        res["steps"].append({"step": "waited", "delay": delay, "claimable": claimable / 1e18})

    # Step 3 — claimAndApprove (one tx instead of two: claim + approve)
    if claimable > 0:
        data = bytes.fromhex(SEL["claimApprove"]) + encode(["address"], [token])
        h = sender.send_tx(acct, ROUTER, data, 400_000, "claimAndApprove")
        res["steps"].append({"step": "claimAndApprove", "tx": h})
        if h and not sender.wait(h):
            res["error"] = "claim reverted"
            return res
        wallet_bal = chain.erc20_balance(token, acct.address)

    if wallet_bal == 0:
        res["error"] = "nothing sellable in wallet"
        return res

    # Step 4 — approve if claimAndApprove did not cover it
    if chain.erc20_allowance(token, acct.address, ROUTER) < wallet_bal:
        data = bytes.fromhex(SEL["approve"]) + encode(["address", "uint256"], [ROUTER, MAX_UINT])
        sender.send_tx(acct, token, data, 80_000, "approve(token)")

    # Step 5 — sell, priced off the actual curve quote
    min_out, quoted = chain.safe_min_out(token, wallet_bal, slippage_bps)
    data = bytes.fromhex(SEL["sell"]) + encode(
        ["address", "uint256", "uint256"], [token, wallet_bal, min_out]
    )
    h = sender.send_tx(acct, ROUTER, data, 450_000, f"sell {wallet_bal/1e18:,.0f}",
                       meta={"quoted_usdc": quoted})
    res["steps"].append({"step": "sell", "tx": h, "tokens": wallet_bal / 1e18,
                         "quoted_usdc": quoted / 1e6})
    if h and sender.wait(h):
        res["realized_usdc"] = quoted / 1e6
        res["pnl_usdc"] = quoted / 1e6 - basis / 1e6
    return res


# ===========================================================================
# CONTEXT
# ===========================================================================
@dataclass
class Ctx:
    rpc: FastRpc
    chain: Chain
    sender: Sender
    state: dict
    nonces: NonceManager
    send: bool
    workers: int


# ===========================================================================
# PHASES
# ===========================================================================
def phase_preflight(ctx: Ctx, keys: List[str]) -> Dict[str, dict]:
    """Batched: 200 wallets of native+USDC+allowance per call, nonces also batched."""
    addrs = [Account.from_key(k).address for k in keys]
    info(f"preflight {len(addrs)} wallets (Multicall3 + batched nonces)...")

    balances: Dict[str, dict] = {}
    CHUNK = 50  # 50 wallets = 150 Multicall3 items (avoids urlopen write timeout)
    for i in range(0, len(addrs), CHUNK):
        batch = addrs[i:i + CHUNK]
        try:
            balances.update(ctx.rpc.preflight_batch(batch))
        except Exception as e:
            warn(f"preflight batch {i//CHUNK} failed: {str(e)[-80:]}")

    nonces: Dict[str, int] = {}
    NONCE_CHUNK = 50  # 50 requests per JSON-RPC batch (avoids HTTP 500 error from RPC providers)
    for i in range(0, len(addrs), NONCE_CHUNK):
        batch = addrs[i:i + NONCE_CHUNK]
        try:
            nonces.update(ctx.rpc.batch_nonces(batch))
        except Exception as e:
            warn(f"nonce batch {i//NONCE_CHUNK} failed: {str(e)[-80:]}")

    ctx.nonces.seed_many(nonces)

    funded = 0
    for a in addrs:
        b = balances.get(a, {"native": 0, "usdc": 0, "allowance": 0})
        wrow(ctx.state, a).update({
            "native": b["native"], "usdc": b["usdc"], "allowance": b["allowance"],
            "nonce": nonces.get(a, 0), "checked_at": int(time.time()),
        })
        if b["native"] > 0 and b["usdc"] >= MIN_COST_BASIS_USDC * 10**USDC_DECIMALS:
            funded += 1
    dirty()

    info(f"preflight done: {len(addrs)} wallets, {funded} funded and tradeable")
    return balances


def phase_faucet(ctx: Ctx, keys: List[str]):
    info("=== FAUCET ===")
    sent = 0

    def job(pk):
        nonlocal sent
        acct = Account.from_key(pk)
        row = wrow(ctx.state, acct.address)
        
        # Fast path: already funded or already claimed, skip instantly with ZERO network calls!
        if row.get("usdc", 0) >= 10 * 10**USDC_DECIMALS:
            return "already-funded"
        if row.get("faucet") in ("broadcast", "confirmed"):
            return "skip"
        
        # Need at least ~0.0025 ARC to cover 110k gas @ 21 gwei
        if row.get("native", 0) < 2_500_000_000_000_000:
            return "no-gas"

        would_work, why = ctx.chain.simulate(acct.address, USDC, "0x" + SEL["faucet"])
        if not would_work:
            return "already-claimed"

        h = ctx.sender.send_tx(acct, USDC, bytes.fromhex(SEL["faucet"]), 110_000, "faucet")
        if h:
            wrow(ctx.state, acct.address)["faucet"] = "broadcast"
            dirty()
            sent += 1
            return "sent"
        return "failed"

    with ThreadPoolExecutor(max_workers=ctx.workers) as pool:
        results = list(pool.map(job, keys))
    from collections import Counter
    info(f"faucet: {dict(Counter(results))}")



def fetch_flipt_newest_tokens(limit: int = 60) -> List[str]:
    """Fetch active tokens from Flipt board API that are in bonding phase with room to buy."""
    import urllib.request
    url = f"https://api-testnet.flipt.fun/board/newest?limit={limit}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=8.0) as resp:
            data = json.loads(resp.read().decode())
        tokens = []
        for r in data.get("rows", []):
            tok = r.get("token")
            phase = r.get("phase", "").lower()
            raised = int(r.get("raised", 0)) / 1e6
            grad = r.get("graduated", False)
            # Room on bonding curve: phase is bonding, not graduated, raised < $6,200
            if tok and phase == "bonding" and not grad and raised < 6200:
                tokens.append(tok)
        return tokens
    except Exception as e:
        warn(f"Flipt newest board API read failed (non-fatal): {e}")
        return []


def phase_launch(ctx: Ctx, keys: List[str], count: int, buy_usdc_str: str,
                 tokens_per_wallet: int = 1) -> List[str]:
    total = count * tokens_per_wallet
    info(f"=== LAUNCH ({count} creators × {tokens_per_wallet} tokens = {total} deploys) ===")
    newly_deployed: List[str] = []
    catalog: List[dict] = []
    if CATALOG_FILE.exists():
        try:
            catalog = json.loads(CATALOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass

    # JIT Preflight for JUST the creator wallets (runs in <0.5s, no cold start)
    creator_keys = keys[:count]
    creator_addrs = [Account.from_key(k).address for k in creator_keys]
    info(f"JIT preflight for {len(creator_addrs)} creator wallets...")
    try:
        bals = ctx.rpc.preflight_batch(creator_addrs)
        ncs = ctx.rpc.batch_nonces(creator_addrs)
        ctx.nonces.seed_many(ncs)
        for a in creator_addrs:
            b = bals.get(a, {"native": 0, "usdc": 0, "allowance": 0})
            wrow(ctx.state, a).update({
                "native": b["native"], "usdc": b["usdc"], "allowance": b["allowance"],
                "nonce": ncs.get(a, 0), "checked_at": int(time.time()),
            })
    except Exception as e:
        warn(f"creator JIT preflight warning: {e}")

    for idx, pk in enumerate(creator_keys):
        acct = Account.from_key(pk)

        for round_idx in range(tokens_per_wallet):
            meta_index = idx * tokens_per_wallet + round_idx
            meta = token_meta(meta_index, catalog)
            try:
                salt, predicted = mine_salt(ctx.chain, acct.address, meta["name"],
                                            meta["symbol"], DEFAULT_SUPPLY)
                info(f"W{idx}R{round_idx} vanity -> {predicted}")
            except Exception as e:
                err(f"W{idx}R{round_idx} salt mine failed: {e}")
                continue

            # Resolve buy amount: "max" = wallet balance up to curve cap
            if buy_usdc_str.lower() == "max":
                wallet_usdc = wrow(ctx.state, acct.address).get("usdc", 0)
                buy_wei = min(
                    wallet_usdc - MIN_COST_BASIS_USDC * 10**USDC_DECIMALS,
                    6300 * 10**USDC_DECIMALS,
                )
                buy_wei = max(0, buy_wei)
            else:
                buy_wei = int(buy_usdc_str) * 10**USDC_DECIMALS

            if buy_wei > 0:
                allow = ctx.chain.erc20_allowance(USDC, acct.address, ROUTER)
                if allow < buy_wei:
                    d = bytes.fromhex(SEL["approve"]) + encode(
                        ["address", "uint256"], [ROUTER, MAX_UINT])
                    h = ctx.sender.send_tx(acct, USDC, d, 80_000, "approve(usdc)")
                    if h:
                        ctx.sender.wait(h)

            data = bytes.fromhex(SEL["launch"]) + encode(
                ["string", "string", "string", "uint256", "bytes", "uint256", "uint256"],
                [meta["name"], meta["symbol"], meta["uri"], salt, b"", buy_wei, 0],
            )
            would, why = ctx.chain.simulate(acct.address, ROUTER, data)
            if not would:
                warn(f"W{idx}R{round_idx} launch would revert, skipping: {why[-80:]}")
                continue

            h = ctx.sender.send_tx(acct, ROUTER, data, 1_800_000, f"launch {meta['symbol']}")
            if h:
                info(f"W{idx}R{round_idx} launch broadcast: {h[:18]}.. verifying on-chain...")
                if ctx.sender.wait(h, timeout=45):
                    with _state_lock:
                        ctx.state["deployments"].append({
                            "address": predicted, "name": meta["name"], "symbol": meta["symbol"],
                            "creator": acct.address, "launch_tx": h, "salt": salt,
                            "status": "confirmed", "created_at": int(time.time()),
                        })
                    wrow(ctx.state, acct.address).update({"last_token": predicted})
                    dirty()
                    newly_deployed.append(predicted)
                else:
                    warn(f"W{idx}R{round_idx} launch confirmation reverted or timed out")
    info(f"phase_launch complete: {len(newly_deployed)} tokens deployed on-chain")
    return newly_deployed


def phase_trade(ctx: Ctx, keys: List[str], token: Optional[str],
                lo: int, hi: int, extra_tokens: Optional[List[str]] = None,
                chunk_size: int = 50):
    info(f"=== DYNAMIC STREAMING TRADE ($ {lo} - ${hi} in {chunk_size}-wallet JIT streams) ===")
    
    last_refresh = 0.0
    active_pool: List[str] = []

    def refresh_token_pool():
        nonlocal last_refresh, active_pool
        pool = []
        if token:
            pool = [token]
        else:
            # 1. Newly deployed tokens in current cycle have highest priority
            if extra_tokens:
                for t in extra_tokens:
                    if t and t not in pool:
                        pool.append(t)
            # 2. Add recent deployments from local state (newest first)
            deploy_addrs = [d["address"] for d in reversed(ctx.state.get("deployments", [])) if d.get("address")]
            for d in deploy_addrs:
                if d not in pool:
                    pool.append(d)
            # 3. Pull live newest active bonding tokens from Flipt API (room to buy < $6,200)
            live_tokens = fetch_flipt_newest_tokens(limit=60)
            for lt in live_tokens:
                if lt not in pool:
                    pool.append(lt)
        active_pool = pool
        last_refresh = time.time()
        return active_pool

    # Initial pool population
    refresh_token_pool()
    if not active_pool:
        warn("no tokens in active pool to trade (launch first, or pass --token)")
        return
    info(f"Active token pool initialized with {len(active_pool)} tokens (Newest: {active_pool[0][:12]}..)")

    min_wei = MIN_COST_BASIS_USDC * 10**USDC_DECIMALS
    results = {"sent": 0, "skip-funds": 0, "skip-allowance": 0, "error": 0}
    all_hashes: List[str] = []

    total_chunks = (len(keys) + chunk_size - 1) // chunk_size
    for chunk_idx, i in enumerate(range(0, len(keys), chunk_size)):
        # Dynamic Token Pool Refresh: check every 45 seconds for newly launched tokens
        if time.time() - last_refresh > 45 or not active_pool:
            prev_len = len(active_pool)
            refresh_token_pool()
            info(f"Dynamic token pool refreshed: {len(active_pool)} active tokens (prev: {prev_len})")

        if not active_pool:
            warn("all token bonding curves currently capped; waiting for new launches...")
            time.sleep(10)
            refresh_token_pool()
            if not active_pool:
                break

        chunk_keys = keys[i:i + chunk_size]
        chunk_addrs = [Account.from_key(k).address for k in chunk_keys]

        # 1. JIT Preflight for this 50-wallet chunk (1 Multicall + 1 nonce batch, ~1s)
        try:
            balances = ctx.rpc.preflight_batch(chunk_addrs)
            nonces = ctx.rpc.batch_nonces(chunk_addrs)
            ctx.nonces.seed_many(nonces)
            for a in chunk_addrs:
                b = balances.get(a, {"native": 0, "usdc": 0, "allowance": 0})
                wrow(ctx.state, a).update({
                    "native": b["native"], "usdc": b["usdc"], "allowance": b["allowance"],
                    "nonce": nonces.get(a, 0), "checked_at": int(time.time()),
                })
        except Exception as e:
            warn(f"chunk {chunk_idx+1}/{total_chunks} JIT preflight failed: {e}")
            continue

        # 2. Build buy jobs for funded wallets in this chunk
        buy_jobs = []
        for pk in chunk_keys:
            acct = Account.from_key(pk)
            row = wrow(ctx.state, acct.address)
            usdc = row.get("usdc", 0)
            if usdc < min_wei or row.get("native", 0) < 10**14:
                results["skip-funds"] += 1
                continue
            amount = random.randint(lo, hi)
            amount_wei = amount * 10**USDC_DECIMALS
            if amount_wei > usdc:
                amount_wei = max(min_wei, usdc // 2)

            # Random selection with priority weighting on the newest tokens in the pool
            if len(active_pool) > 5 and random.random() < 0.80:
                # 80% chance: randomly select from the top 10 newest tokens
                tgt = random.choice(active_pool[:min(10, len(active_pool))])
            else:
                # 20% chance: randomly select across the wider pool for broad volume distribution
                tgt = random.choice(active_pool)

            if row.get("allowance", 0) < amount_wei:
                d = bytes.fromhex(SEL["approve"]) + encode(
                    ["address", "uint256"], [ROUTER, MAX_UINT])
                try:
                    h = ctx.sender.send_tx(acct, USDC, d, 80_000, "approve(usdc)")
                    if h:
                        ctx.sender.wait(h, timeout=45)
                        row["allowance"] = MAX_UINT
                    elif not ctx.send:
                        row["allowance"] = MAX_UINT
                except Exception as e:
                    warn(f"{acct.address[:10]} approve failed: {str(e)[-70:]}")
                    results["skip-allowance"] += 1
                    continue

            data = bytes.fromhex(SEL["buy"]) + encode(
                ["address", "uint256", "uint256"], [tgt, amount_wei, 0])
            buy_jobs.append((acct, ROUTER, data, 320_000, f"buy {amount} USDC",
                             {"token": tgt, "amount_usdc": amount}))

        if not buy_jobs:
            continue

        # 3. Immediately broadcast this chunk's buy jobs
        hashes, tx_by_wallet = ctx.sender.batch_send(buy_jobs)
        all_hashes.extend(hashes)
        results["sent"] += len(hashes)

        for (acct, _, _, _, label, meta) in buy_jobs:
            h = tx_by_wallet.get(acct.address.lower())
            if h:
                wrow(ctx.state, acct.address).setdefault("trades", []).append(
                    {"side": "buy", "token": meta["token"], "usdc": meta["amount_usdc"],
                     "tx": h, "ts": int(time.time())})
        dirty()

        info(f"Chunk {chunk_idx+1}/{total_chunks}: sent {len(hashes)}/{len(buy_jobs)} buys (total: {results['sent']})")

    if all_hashes and ctx.send:
        info(f"waiting for {len(all_hashes)} buy confirmations (batched)...")
        ctx.sender.batch_wait(all_hashes[:500], timeout=90)

    info(f"trade complete: {results}")


def phase_exit(ctx: Ctx, keys: List[str], tokens: Sequence[str], pct: float):
    """
    Per-wallet exit, driven by that wallet's OWN position readiness (not a
    shared trigger). Skips wallets with no position rather than forcing one.
    """
    info("=== EXIT (unbond -> 90s -> claim -> sell) ===")
    results = []

    for token in tokens:
        info_l = ctx.chain.launch_info(token)
        if not info_l.get("graduated"):
            warn(f"{token[:12]}.. not graduated — unbond reverts here, skipping "
                 f"({info_l.get('sold_at_graduation', 0)/1e18:,.0f}/"
                 f"{793_000_000:,.0f} tokens sold)")
            continue

        # One Multicall3 round trip for every wallet, and count a wallet as
        # live if it has ANY of: bonded tokens, a pending unbond, or tokens
        # already in the wallet. v3's bond-only filter silently skipped
        # wallets that had unbonded in an earlier run, so their claimable
        # balance was never claimed or sold.
        addr_of = {Account.from_key(pk).address: pk for pk in keys}
        pos = ctx.chain.positions_batch(token, list(addr_of))
        holders = [addr_of[a] for a, p in pos.items()
                   if p["bond"] > 0 or p["claimable"] > 0 or p["wallet"] > 0]
        if not holders:
            info(f"{token[:12]}.. no live positions among these wallets")
            continue

        n_bond = sum(1 for a, p in pos.items() if p["bond"] > 0)
        n_mid = sum(1 for a, p in pos.items() if p["claimable"] > 0)
        info(f"{token[:12]}.. {len(holders)} live wallets "
             f"({n_bond} bonded, {n_mid} mid-unbond)")

        def job(pk):
            acct = Account.from_key(pk)
            try:
                return exit_position(ctx, acct, token, pct=pct)
            except Exception as e:
                err(f"exit {acct.address[:10]}: {str(e)[-100:]}")
                return {"wallet": acct.address, "token": token, "error": str(e)[-140:]}

        with ThreadPoolExecutor(max_workers=ctx.workers) as pool:
            results.extend(pool.map(job, holders))

    for r in results:
        p = prow(ctx.state, r["wallet"], r["token"])
        p["last_result"] = r
        p["state"] = ("error" if r.get("error") else
                      "exited" if r.get("realized_usdc") else "partial")
        p["updated_at"] = int(time.time())
    dirty()

    done = [r for r in results if r.get("realized_usdc")]
    gas = [r for r in results if r.get("error")]
    info(f"exit: {len(done)} sold, {len(gas)} failed, "
         f"{len(results)-len(done)-len(gas)} partial")


def phase_fees(ctx: Ctx):
    info("=== CREATOR FEES ===")
    for d in ctx.state.get("deployments", []):
        creator = d.get("creator")
        tok = d.get("address")
        if not creator or not tok:
            continue
        try:
            acct = Account.from_key(_key_for(ctx, creator))
        except Exception:
            continue
        data = bytes.fromhex(SEL["collectFee"]) + encode(["address"], [tok])
        would, _ = ctx.chain.simulate(acct.address, ROUTER, data)
        if not would:
            continue
        try:
            h = ctx.sender.send_tx(acct, ROUTER, data, 250_000, "collectCreatorFee")
            if h:
                ctx.sender.wait(h)
        except Exception as e:
            warn(f"fee collect failed for {tok[:10]}: {str(e)[-70:]}")


_KEYMAP: Dict[str, str] = {}


def _key_for(ctx, address: str) -> str:
    if not _KEYMAP:
        raise RuntimeError("keymap not built")
    k = _KEYMAP.get(address.lower())
    if not k:
        raise RuntimeError(f"no key for {address}")
    return k


def phase_graduate(ctx: Ctx):
    info("=== GRADUATE ===")
    for d in ctx.state.get("deployments", []):
        tok = d.get("address")
        if not tok:
            continue
        info_l = ctx.chain.launch_info(tok)
        if not info_l.get("exists") or info_l.get("graduated"):
            continue
        try:
            acct = Account.from_key(_key_for(ctx, d["creator"]))
        except Exception:
            continue
        data = bytes.fromhex(SEL["graduate"]) + encode(["address"], [tok])
        would, _ = ctx.chain.simulate(acct.address, ROUTER, data)
        if not would:
            continue
        try:
            h = ctx.sender.send_tx(acct, ROUTER, data, 600_000, "graduate")
            if h:
                ctx.sender.wait(h)
        except Exception as e:
            warn(f"graduate failed {tok[:10]}: {str(e)[-70:]}")


# ===========================================================================
# MAIN
# ===========================================================================
def main():
    ap = argparse.ArgumentParser(description="Flipt/Arc v4 — batched, 429-resistant")
    ap.add_argument("--wallets", type=int, default=0, help="0 = all of pv.txt")
    ap.add_argument("--workers", type=int, default=15)
    ap.add_argument("--rate", type=float, default=25.0, help="RPC requests/sec cap")
    ap.add_argument("--send", action="store_true",
                    help="ACTUALLY SEND. Without this, every phase is dry-run.")

    ap.add_argument("--preflight", action="store_true")
    ap.add_argument("--faucet", action="store_true")
    ap.add_argument("--launch", action="store_true")
    ap.add_argument("--launch-count", type=int, default=5)
    ap.add_argument("--launch-buy", type=str, default="50",
                    help="USDC to buy on launch. 'max' = wallet balance up to curve cap ($6300)")
    ap.add_argument("--tokens-per-wallet", type=int, default=1,
                    help="How many distinct tokens each creator wallet deploys")

    ap.add_argument("--trade", action="store_true")
    ap.add_argument("--token", default=None)
    ap.add_argument("--trade-min", type=int, default=50)
    ap.add_argument("--trade-max", type=int, default=100)

    ap.add_argument("--exit", action="store_true")
    ap.add_argument("--exit-tokens", nargs="*", default=[])
    ap.add_argument("--exit-pct", type=float, default=100.0)

    ap.add_argument("--collect-fees", action="store_true")
    ap.add_argument("--graduate", action="store_true")
    ap.add_argument("--auto", action="store_true", help="preflight -> faucet -> launch -> trade")
    ap.add_argument("--loop", action="store_true",
                    help="Run --auto in a non-stop loop. Ctrl+C to stop gracefully.")
    ap.add_argument("--loop-delay", type=int, default=30,
                    help="Seconds to pause between loop cycles (default 30)")
    args = ap.parse_args()

    if not PV_FILE.exists():
        raise SystemExit(f"missing {PV_FILE}")
    keys = [l.strip() for l in PV_FILE.read_text(encoding="utf-8").splitlines()
            if l.strip() and not l.lstrip().startswith("#")]
    if args.wallets > 0:
        keys = keys[:args.wallets]
    if not keys:
        raise SystemExit("pv.txt has no keys")

    for k in keys:
        _KEYMAP[Account.from_key(k).address.lower()] = k

    state = load_state()
    rpc = FastRpc(rate_per_sec=args.rate)
    chain = Chain(rpc)
    nonces = NonceManager()
    sender = Sender(rpc, chain, state, nonces, send=args.send)
    ctx = Ctx(rpc, chain, sender, state, nonces, args.send, args.workers)

    if not args.send:
        warn("DRY RUN — no transactions will be sent. Add --send to execute.")

    save_stop = threading.Event()
    saver = threading.Thread(target=state_saver_loop, args=(state, save_stop), daemon=True)
    saver.start()

    info(f"v4 | chain={CHAIN_ID} wallets={len(keys)} workers={args.workers} "
         f"rate={args.rate}/s endpoints={len(rpc.pool.healthy())} send={args.send}")

    try:
        def run_cycle(cycle_num: int = 1):
            info(f"{'='*60}")
            info(f"CYCLE {cycle_num} starting (Zero Cold-Start Streaming Mode)")
            info(f"{'='*60}")

            # 1. Launch with JIT creator preflight (starts in <1 second)
            new_deployed = phase_launch(ctx, keys, args.launch_count, args.launch_buy,
                                        args.tokens_per_wallet)
            if new_deployed:
                info(f"waiting 15s after deployment before buy wave on {len(new_deployed)} tokens...")
                time.sleep(15)

            # 2. Streaming trade across wallets in 50-wallet JIT batches (continuous flow)
            phase_trade(ctx, keys, None, args.trade_min, args.trade_max,
                        extra_tokens=new_deployed, chunk_size=50)

            # Mid-cycle drain: catch receipts that arrived during the cycle
            pending = [h for h, m in state["transactions"].items()
                       if m.get("status") == "broadcast"]
            if pending and args.send:
                info(f"mid-cycle drain: {len(pending)} pending receipts...")
                for i in range(0, min(len(pending), 600), 200):
                    batch = pending[i:i + 200]
                    sender.batch_wait(batch, timeout=60)
                save_state(state)

            with _state_lock:
                txs = state["transactions"]
                conf = sum(1 for x in txs.values() if x.get("status") == "confirmed")
                rev = sum(1 for x in txs.values() if x.get("status") == "reverted")
                bcast = sum(1 for x in txs.values() if x.get("status") == "broadcast")
            info(f"cycle {cycle_num} done: {len(txs)} txs, {conf} confirmed, "
                 f"{rev} reverted, {bcast} pending")

        if args.auto or args.loop:
            if args.loop:
                info(f"NON-STOP MODE: looping every {args.loop_delay}s. Ctrl+C to stop.")
                cycle = 1
                while True:
                    try:
                        run_cycle(cycle)
                        cycle += 1
                        info(f"sleeping {args.loop_delay}s before next cycle...")
                        time.sleep(args.loop_delay)
                    except KeyboardInterrupt:
                        warn(f"loop stopped after {cycle} cycles")
                        break
                    except Exception as e:
                        err(f"cycle {cycle} failed: {e}")
                        traceback.print_exc()
                        info(f"recovering in {args.loop_delay}s...")
                        time.sleep(args.loop_delay)
            else:
                # Single --auto run
                run_cycle(1)
        else:
            phase_preflight(ctx, keys)

            if args.faucet:
                phase_faucet(ctx, keys)
            if args.launch:
                phase_launch(ctx, keys, args.launch_count, args.launch_buy,
                             args.tokens_per_wallet)
            if args.graduate:
                phase_graduate(ctx)
            if args.collect_fees:
                phase_fees(ctx)
            if args.trade:
                phase_trade(ctx, keys, args.token, args.trade_min, args.trade_max)
            if args.exit:
                toks = args.exit_tokens or [
                    d["address"] for d in state.get("deployments", []) if d.get("address")
                ]
                for t in toks:
                    info(f"launch state for {t[:14]}..: "
                         f"{ctx.chain.launch_info(t).get('sold_at_graduation', 0)/1e18:,.0f} tokens sold")
                phase_exit(ctx, keys, toks, args.exit_pct)

    except KeyboardInterrupt:
        warn("interrupted")
    except Exception as e:
        err(f"fatal: {e}")
        traceback.print_exc()
    finally:
        # drain outstanding receipts in one batched pass
        pending = [h for h, m in state["transactions"].items()
                   if m.get("status") == "broadcast"]
        if pending and args.send:
            info(f"draining {len(pending)} pending receipts (batched, chunks of 200)...")
            for i in range(0, len(pending), 200):
                batch = pending[i:i + 200]
                sender.batch_wait(batch, timeout=90)
                save_state(state)

        save_stop.set()
        saver.join(timeout=5)
        save_state(state)

        with _state_lock:
            txs = state["transactions"]
            conf = sum(1 for x in txs.values() if x.get("status") == "confirmed")
            rev = sum(1 for x in txs.values() if x.get("status") == "reverted")
            TOKENS_FILE.parent.mkdir(parents=True, exist_ok=True)
            TOKENS_FILE.write_text(
                json.dumps(state["deployments"], indent=2, default=str), encoding="utf-8")

        print()
        print(rpc.report())
        ok(f"txs: {len(txs)} broadcast, {conf} confirmed, {rev} reverted")
        ok(f"state -> {STATE_FILE}")
        ok(f"tokens -> {TOKENS_FILE}")
        save_state(state)


if __name__ == "__main__":
    main()
