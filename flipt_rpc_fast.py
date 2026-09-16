#!/usr/bin/env python3
"""
flipt_rpc_fast.py — 429-proof RPC layer for Flipt/Arc testnet.

Drop-in replacement for the RPC parts of flipt_final_v3.py.
"""

from __future__ import annotations

import json
import random
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from arc_rpc_pool import ArcRpcPool, VERIFIED_ENDPOINTS
except ImportError:
    raise SystemExit(
        "arc_rpc_pool.py not found next to this file.\n"
        "Copy the real arc_rpc_pool.py into this folder."
    )

CHAIN_ID = 5042002

DEFAULT_GAS_PRICE_WEI = 21_000_000_000      # 21 gwei
GAS_PRICE_REFRESH_SECS = 600                # re-check every 10 min, ~free

MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
USDC = "0x4F3b8005d6b3F4994a791D971bcD153E114D20c2"
ROUTER = "0x4B33146F2bCc75574534374C85662f9E51C38Aca"

SELECTOR_GET_ETH_BALANCE = "4d2301cc"
SELECTOR_BALANCE_OF = "70a08231"
SELECTOR_ALLOWANCE = "dd62ed3e"
SELECTOR_AGGREGATE3 = "82ad56cb"


# ---------------------------------------------------------------------------
# TOKEN BUCKET
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# FAST RPC
# ---------------------------------------------------------------------------
class FastRpc:
    def __init__(
        self,
        pool: Optional[ArcRpcPool] = None,
        rate_per_sec: float = 25.0,
        gas_price_wei: int = DEFAULT_GAS_PRICE_WEI,
        timeout: float = 20.0,
    ):
        self.pool = pool or ArcRpcPool(verify_chain=True)
        self.bucket = TokenBucket(rate_per_sec)
        self._gas_price = gas_price_wei
        self._gas_checked = 0.0
        self.timeout = timeout
        self.counters = {
            "http_requests": 0,
            "batch_requests": 0,
            "rate_limit_waits": 0,
            "gas_price_reads": 0,
            "http_429": 0,
            "waf_403": 0,
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
                out: List[Any] = [None] * len(requests)
                for item in data:
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
        if now - self._gas_checked < GAS_PRICE_REFRESH_SECS:
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
        from eth_abi import encode, decode

        calls = []
        for w in wallets:
            wd = self._addr_word(w)
            calls.append((MULTICALL3, True, bytes.fromhex(SELECTOR_GET_ETH_BALANCE + wd)))
            calls.append((USDC, True, bytes.fromhex(SELECTOR_BALANCE_OF + wd)))
            calls.append(
                (
                    USDC,
                    True,
                    bytes.fromhex(
                        SELECTOR_ALLOWANCE + wd + self._addr_word(ROUTER)
                    ),
                )
            )

        data = "0x" + SELECTOR_AGGREGATE3 + encode(
            ["(address,bool,bytes)[]"], [calls]
        ).hex()
        raw = self.call("eth_call", [{"to": MULTICALL3, "data": data}, "latest"])
        results = decode(["(bool,bytes)[]"], bytes.fromhex(raw[2:]))[0]

        out: Dict[str, Dict[str, int]] = {}
        for i, w in enumerate(wallets):
            native, usdc, allow = results[i * 3], results[i * 3 + 1], results[i * 3 + 2]
            out[w] = {
                "native": int.from_bytes(native[1], "big") if native[1] else 0,
                "usdc": int.from_bytes(usdc[1], "big") if usdc[1] else 0,
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
                # Fallback: individual call for wallets that failed in batch.
                # Seeding nonce 0 on a wallet with real nonce 2694 causes every
                # tx to revert with "nonce too low". Skipping is safer.
                try:
                    single = self.call("eth_getTransactionCount", [w, "pending"])
                    out[w] = int(single, 16)
                    self.bump("nonce_fallback_ok")
                except Exception:
                    self.bump("nonce_fallback_fail")
                    pass  # Skip — no seed is safer than nonce 0
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


    def multicall(self, calls: Sequence[Tuple[str, bytes]]) -> List[bytes]:
        """Generic Multicall3 aggregate3 execution.
        calls: [(target_address, call_data_bytes), ...]
        Returns: list of returnData bytes for each call (or b"" if failed).
        """
        if not calls:
            return []
        agg_calls = [(target, True, data) for target, data in calls]
        data = "0x" + SELECTOR_AGGREGATE3 + encode(
            ["(address,bool,bytes)[]"], [agg_calls]
        ).hex()
        raw = self.call("eth_call", [{"to": MULTICALL3, "data": data}, "latest"])
        if not raw or raw == "0x":
            return [b""] * len(calls)
        results = decode(["(bool,bytes)[]"], bytes.fromhex(raw[2:]))[0]
        return [res[1] if res[0] else b"" for res in results]

    def batch_send_raw(self, signed_txs: Sequence[str]) -> List[Optional[str]]:
        """Send multiple signed raw txs in one HTTP batch. Returns list of tx hashes.
        Verified empirically: Arc testnet RPCs accept batched eth_sendRawTransaction."""
        if not signed_txs:
            return []
        reqs = [("eth_sendRawTransaction", [tx]) for tx in signed_txs]
        res = self.batch_call(reqs)
        self.bump("batch_sends")
        return [r if isinstance(r, str) else None for r in res]

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


if __name__ == "__main__":
    rpc = FastRpc(rate_per_sec=25)
    print(rpc.pool.report())
    print()

    print("gas price (cached):", rpc.gas_price() / 1e9, "gwei")
    t0 = time.time()
    print("second read:", rpc.gas_price() / 1e9, "gwei  (no RPC)",
          f"{(time.time()-t0)*1000:.2f}ms")

    from eth_utils import keccak

    def synth(i):
        return "0x" + keccak(text=f"w{i}").hex()[-40:]

    wallets = [synth(i) for i in range(200)]
    t0 = time.perf_counter()
    pf = rpc.preflight_batch(wallets)
    print(f"\npreflight 200 wallets: {len(pf)} results in "
          f"{(time.perf_counter()-t0)*1000:.0f}ms  [1 call]")

    t0 = time.perf_counter()
    nonces = rpc.batch_nonces(wallets)
    print(f"nonces 200 wallets:    {len(nonces)} results in "
          f"{(time.perf_counter()-t0)*1000:.0f}ms  [1 call]")

    print()
    print(rpc.report())
