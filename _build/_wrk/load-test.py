#!/usr/bin/env python3
"""
API Load Tester — simulates concurrent users hitting the gateway.

Usage:
    python load-test.py addresses.txt                         # defaults: 50 concurrent, producer endpoint
    python load-test.py addresses.txt --concurrency 200       # 200 simulated users
    python load-test.py addresses.txt --endpoint bmap         # test bmap/get (read path)
    python load-test.py addresses.txt --target aws            # hit AWS instead of localhost
    python load-test.py addresses.txt --wave-window 0.3       # tighter stagger (0.3s waves)
    python load-test.py addresses.txt --sig-cnt 100           # 100 sigs per address
    python load-test.py addresses.txt --dry-run               # show plan, don't send
"""

import asyncio
import aiohttp
import argparse
import json
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from statistics import mean, median, quantiles

# ── Targets ──────────────────────────────────────────────────────────────────

TARGETS = {
    "local":  "http://localhost:5100",
    "aws":    "https://svcs.the16oracles.io",
}

DEFAULT_FILE = "mint-addresses-near-10k-process.txt"

API_KEY = "admin_master_key"

# ── Result tracking ──────────────────────────────────────────────────────────

@dataclass
class RequestResult:
    address: str
    status: int
    latency_ms: float
    error: str | None = None
    request_id: str | None = None

@dataclass
class Stats:
    results: list[RequestResult] = field(default_factory=list)
    start_time: float = 0.0
    end_time: float = 0.0

    @property
    def total(self):       return len(self.results)
    @property
    def successes(self):   return [r for r in self.results if 200 <= r.status < 300]
    @property
    def errors(self):      return [r for r in self.results if r.status >= 400 or r.error]
    @property
    def latencies(self):   return [r.latency_ms for r in self.results if r.error is None]
    @property
    def elapsed_s(self):   return self.end_time - self.start_time

    def summary(self):
        lats = sorted(self.latencies)
        if not lats:
            return "No successful requests."

        q = quantiles(lats, n=100) if len(lats) >= 2 else lats
        p50 = q[49]  if len(q) > 49 else lats[0]
        p95 = q[94]  if len(q) > 94 else lats[-1]
        p99 = q[98]  if len(q) > 98 else lats[-1]

        status_counts = {}
        for r in self.results:
            key = r.status if not r.error else f"ERR"
            status_counts[key] = status_counts.get(key, 0) + 1

        lines = [
            "",
            "=" * 60,
            f"  LOAD TEST RESULTS",
            "=" * 60,
            f"  Total requests:    {self.total}",
            f"  Successful (2xx):  {len(self.successes)}",
            f"  Errors:            {len(self.errors)}",
            f"  Error rate:        {len(self.errors)/self.total*100:.1f}%",
            f"  Wall time:         {self.elapsed_s:.1f}s",
            f"  Throughput:        {self.total/self.elapsed_s:.1f} req/s",
            "",
            f"  Latency (ms):",
            f"    min:    {min(lats):>8.0f}",
            f"    p50:    {p50:>8.0f}",
            f"    p95:    {p95:>8.0f}",
            f"    p99:    {p99:>8.0f}",
            f"    max:    {max(lats):>8.0f}",
            f"    mean:   {mean(lats):>8.0f}",
            "",
            f"  Status code distribution:",
        ]
        for code, cnt in sorted(status_counts.items(), key=lambda x: str(x[0])):
            lines.append(f"    {code}: {cnt}")

        # Show slowest 5
        if len(lats) >= 5:
            lines.append("")
            lines.append(f"  Slowest 5 requests:")
            slowest = sorted(self.results, key=lambda r: r.latency_ms, reverse=True)[:5]
            for r in slowest:
                lines.append(f"    {r.latency_ms:>7.0f}ms  {r.status}  {r.address[:20]}...")

        # Show errors (first 10)
        errs = [r for r in self.results if r.error]
        if errs:
            lines.append("")
            lines.append(f"  First {min(10, len(errs))} errors:")
            for r in errs[:10]:
                lines.append(f"    {r.address[:20]}... -> {r.error}")

        lines.append("=" * 60)
        return "\n".join(lines)


# ── Request builders ─────────────────────────────────────────────────────────

def build_producer_request(address: str, sig_cnt: int, priority: int) -> tuple[str, dict]:
    """POST /api/trigger/producer — async pipeline trigger."""
    path = "/api/trigger/producer"
    body = {
        "batch": {
            "filters": {
                "addresses": [address]
            },
            "address_sig_cnt": sig_cnt
        }
    }
    return path, body

def build_bmap_request(address: str, sig_cnt: int, priority: int) -> tuple[str, dict | None]:
    """GET /api/bmap/get — synchronous read (heavier)."""
    path = f"/api/bmap/get?address={address}&limit={sig_cnt}"
    return path, None

def build_wallet_txs_request(address: str, sig_cnt: int, priority: int) -> tuple[str, dict | None]:
    """GET /api/bmap/v3.1/get-wallet-txs — synchronous read."""
    path = f"/api/bmap/v3.1/get-wallet-txs?wallet={address}&limit={sig_cnt}"
    return path, None

ENDPOINTS = {
    "producer":    build_producer_request,
    "bmap":        build_bmap_request,
    "wallet-txs":  build_wallet_txs_request,
}

# ── Worker ───────────────────────────────────────────────────────────────────

async def send_request(
    session: aiohttp.ClientSession,
    base_url: str,
    address: str,
    endpoint_builder,
    sig_cnt: int,
    priority: int,
    stats: Stats,
    sem: asyncio.Semaphore,
    worker_id: int,
):
    async with sem:
        path, body = endpoint_builder(address, sig_cnt, priority)
        url = f"{base_url}{path}"
        headers = {
            "X-API-Key": API_KEY,
            "X-Request-Id": str(uuid.uuid4()),
            "X-Correlation-Id": f"loadtest-{int(time.time())}",
            "X-Priority": str(priority),
        }

        t0 = time.perf_counter()
        try:
            if body is not None:
                headers["Content-Type"] = "application/json"
                async with session.post(url, json=body, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    status = resp.status
                    resp_text = await resp.text()
            else:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    status = resp.status
                    resp_text = await resp.text()

            latency_ms = (time.perf_counter() - t0) * 1000

            # Extract request_id from response if present
            req_id = None
            try:
                rj = json.loads(resp_text)
                req_id = rj.get("request_id")
            except:
                pass

            result = RequestResult(address=address, status=status, latency_ms=latency_ms, request_id=req_id)

        except asyncio.TimeoutError:
            latency_ms = (time.perf_counter() - t0) * 1000
            result = RequestResult(address=address, status=0, latency_ms=latency_ms, error="TIMEOUT (30s)")

        except Exception as e:
            latency_ms = (time.perf_counter() - t0) * 1000
            result = RequestResult(address=address, status=0, latency_ms=latency_ms, error=str(e)[:100])

        stats.results.append(result)

        # Live progress every 50 requests
        n = len(stats.results)
        if n % 50 == 0 or n == 1:
            elapsed = time.perf_counter() - stats.start_time
            err_cnt = len(stats.errors)
            print(f"  [{n:>5} sent] {n/elapsed:.0f} req/s | last: {latency_ms:.0f}ms {result.status} | errors: {err_cnt}", flush=True)


# ── Wave scheduler ───────────────────────────────────────────────────────────

async def run_load_test(addresses, args):
    endpoint_builder = ENDPOINTS[args.endpoint]
    base_url = TARGETS[args.target]
    concurrency = args.concurrency
    wave_window = args.wave_window

    # Split addresses into waves
    # Each wave: `concurrency` requests staggered within `wave_window` seconds
    waves = [addresses[i:i+concurrency] for i in range(0, len(addresses), concurrency)]

    print(f"\n  Target:       {base_url}")
    print(f"  Endpoint:     {args.endpoint}")
    print(f"  Addresses:    {len(addresses)}")
    print(f"  Concurrency:  {concurrency}")
    print(f"  Waves:        {len(waves)} x {wave_window}s window")
    print(f"  sig_cnt:      {args.sig_cnt}")
    print(f"  Priority:     {args.priority}")
    print()

    if args.dry_run:
        print("  [DRY RUN] Would send the above. Exiting.")
        return None

    stats = Stats()
    sem = asyncio.Semaphore(concurrency)

    connector = aiohttp.TCPConnector(limit=concurrency, limit_per_host=concurrency)
    async with aiohttp.ClientSession(connector=connector) as session:
        stats.start_time = time.perf_counter()

        for wave_idx, wave_addrs in enumerate(waves):
            print(f"  -- Wave {wave_idx+1}/{len(waves)} ({len(wave_addrs)} requests) --")

            tasks = []
            for i, addr in enumerate(wave_addrs):
                # Stagger: random delay within the wave window
                delay = random.uniform(0, wave_window)
                tasks.append(
                    staggered_send(
                        delay, session, base_url, addr, endpoint_builder,
                        args.sig_cnt, args.priority, stats, sem, i
                    )
                )

            await asyncio.gather(*tasks)

            # Brief pause between waves (let the system breathe)
            if wave_idx < len(waves) - 1:
                await asyncio.sleep(0.1)

        stats.end_time = time.perf_counter()

    return stats


async def staggered_send(delay, session, base_url, address, endpoint_builder, sig_cnt, priority, stats, sem, worker_id):
    """Wait a random delay within the wave window, then fire."""
    await asyncio.sleep(delay)
    await send_request(session, base_url, address, endpoint_builder, sig_cnt, priority, stats, sem, worker_id)


# ── CLI ──────────────────────────────────────────────────────────────────────

def load_addresses(filepath: str) -> list[str]:
    """Load addresses from file, one per line. Skip blanks and comments."""
    lines = Path(filepath).read_text().strip().splitlines()
    addrs = [l.strip() for l in lines if l.strip() and not l.strip().startswith("#")]
    return addrs


def main():
    parser = argparse.ArgumentParser(description="API Load Tester")
    parser.add_argument("file", nargs="?", default=DEFAULT_FILE,
                        help=f"Text file with one address per line (default: {DEFAULT_FILE})")
    parser.add_argument("--concurrency", "-c", type=int, default=50,
                        help="Simulated concurrent users per wave (default: 50)")
    parser.add_argument("--wave-window", "-w", type=float, default=0.5,
                        help="Seconds to stagger requests within each wave (default: 0.5)")
    parser.add_argument("--endpoint", "-e", choices=ENDPOINTS.keys(), default="producer",
                        help="Which endpoint to hit (default: producer)")
    parser.add_argument("--target", "-t", choices=TARGETS.keys(), default="aws",
                        help="Target environment (default: aws)")
    parser.add_argument("--sig-cnt", "-s", type=int, default=20,
                        help="Signatures per address / limit (default: 20)")
    parser.add_argument("--priority", "-p", type=int, default=5,
                        help="X-Priority header value 1-10 (default: 5)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan and exit without sending")
    parser.add_argument("--shuffle", action="store_true",
                        help="Randomize address order")
    parser.add_argument("--max", "-m", type=int, default=0,
                        help="Max addresses to use from file (0=all)")
    parser.add_argument("--output", "-o", type=str, default=None,
                        help="Write detailed results to JSON file")

    args = parser.parse_args()

    # Load addresses
    addresses = load_addresses(args.file)
    if not addresses:
        print(f"No addresses found in {args.file}")
        sys.exit(1)

    if args.shuffle:
        random.shuffle(addresses)

    if args.max > 0:
        addresses = addresses[:args.max]

    print(f"\n  Loaded {len(addresses)} addresses from {args.file}")

    # Run
    stats = asyncio.run(run_load_test(addresses, args))

    if stats:
        print(stats.summary())

        # Optional JSON output
        if args.output:
            out = {
                "config": {
                    "target": TARGETS[args.target],
                    "endpoint": args.endpoint,
                    "concurrency": args.concurrency,
                    "wave_window": args.wave_window,
                    "sig_cnt": args.sig_cnt,
                    "total_addresses": len(addresses),
                },
                "summary": {
                    "total": stats.total,
                    "successes": len(stats.successes),
                    "errors": len(stats.errors),
                    "error_rate_pct": round(len(stats.errors)/stats.total*100, 2) if stats.total else 0,
                    "elapsed_s": round(stats.elapsed_s, 2),
                    "throughput_rps": round(stats.total/stats.elapsed_s, 2) if stats.elapsed_s else 0,
                    "latency_ms": {
                        "min": round(min(stats.latencies), 1) if stats.latencies else 0,
                        "mean": round(mean(stats.latencies), 1) if stats.latencies else 0,
                        "max": round(max(stats.latencies), 1) if stats.latencies else 0,
                    }
                },
                "results": [
                    {
                        "address": r.address,
                        "status": r.status,
                        "latency_ms": round(r.latency_ms, 1),
                        "error": r.error,
                        "request_id": r.request_id,
                    }
                    for r in stats.results
                ],
            }
            Path(args.output).write_text(json.dumps(out, indent=2))
            print(f"\n  Detailed results written to {args.output}")


if __name__ == "__main__":
    main()
