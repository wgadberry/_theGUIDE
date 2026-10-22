"""
Benchmark: fetch 10K tx sigs across 10 Chainstack RPC endpoints.
Each address uses the RPC_URL at its ordinal position.
Pages 1000 sigs at a time (RPC max), up to 1000 per address = 10K total.
"""

import json, time, requests
from concurrent.futures import ThreadPoolExecutor, as_completed

ADDRESSES = [
    "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN",
    "Gxm9i54HtJivCkD58sDE6GndTRGAspw1QwVDok1RauBw",
    "2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv",
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump",
    "21nnfR4TkbZNLwvRrqEseAbz7P3kxKjaV7KuboLJpump",
    "GoLDppdjB1vDTPSGxyMJFqdnj134yH6Prg9eqsGDiw6A",
    "LegiuxbrPh7SKEpnTN1nB2AjtrmSGP7XJtYZV4spump",
    "CHSBRJMtGGFDzXACSRffhQZdx4eNdqGG77xg5Ntumoon",
    "Ha4oiA7TJtoE1jPk8m6r5GPYm1ERmT9oKU5CZAGTpump",
]

with open("guide-config.json") as f:
    cfg = json.load(f)

RPC_URLS = cfg["RPC_URLs"]

TARGET_PER_ADDR = 1000  # 1000 sigs per address = 10K total


def fetch_page(session, rpc_url, address, limit=1000, before=None):
    params = {"limit": limit}
    if before:
        params["before"] = before
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "getSignaturesForAddress",
        "params": [address, params],
    }
    resp = session.post(rpc_url, json=payload, timeout=30,
                        headers={"Authorization": "Bearer nZzP3hOu.OfBtyFjPKcRjcWx6TU1ZBbKexe8eBLle"})
    resp.raise_for_status()
    return resp.json()


def bench_address(idx, address, rpc_url):
    """Fetch TARGET_PER_ADDR sigs for one address, return stats."""
    session = requests.Session()
    total_sigs = 0
    pages = 0
    before = None
    error = None
    t0 = time.perf_counter()

    try:
        while total_sigs < TARGET_PER_ADDR:
            remaining = TARGET_PER_ADDR - total_sigs
            limit = min(1000, remaining)
            data = fetch_page(session, rpc_url, address, limit=limit, before=before)

            result = data.get("result", [])
            if not result:
                break

            total_sigs += len(result)
            pages += 1
            before = result[-1]["signature"]

            if len(result) < limit:
                break  # no more sigs
    except Exception as e:
        error = str(e)[:60]

    elapsed = time.perf_counter() - t0
    session.close()
    return {
        "idx": idx,
        "address": address[:8] + "...",
        "node": f"Node_{idx:02d}",
        "sigs": total_sigs,
        "pages": pages,
        "elapsed": elapsed,
        "sigs_per_sec": total_sigs / elapsed if elapsed > 0 else 0,
        "error": error,
    }


def main():
    print(f"Benchmarking 10 Chainstack endpoints — {TARGET_PER_ADDR} sigs/address\n")
    print(f"{'Node':<10} {'Address':<14} {'Sigs':>6} {'Pages':>6} {'Time':>8} {'Sigs/s':>10}  {'Status'}")
    print("-" * 75)

    grand_t0 = time.perf_counter()
    results = []

    # Run all 10 in parallel
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {
            pool.submit(bench_address, i, addr, RPC_URLS[i]): i
            for i, addr in enumerate(ADDRESSES)
        }
        for fut in as_completed(futures):
            r = fut.result()
            results.append(r)
            status = r["error"] if r["error"] else "OK"
            print(
                f"{r['node']:<10} {r['address']:<14} {r['sigs']:>6} {r['pages']:>6} "
                f"{r['elapsed']:>7.2f}s {r['sigs_per_sec']:>9.0f}  {status}"
            )

    grand_elapsed = time.perf_counter() - grand_t0
    ok_results = [r for r in results if not r["error"]]
    total_sigs = sum(r["sigs"] for r in results)
    ok_sigs = sum(r["sigs"] for r in ok_results)

    print("-" * 75)
    print(f"TOTAL: {total_sigs:,} sigs in {grand_elapsed:.2f}s wall-clock "
          f"({total_sigs/grand_elapsed:.0f} sigs/s effective)")
    if ok_results:
        print(f"OK endpoints: {len(ok_results)}/10, avg {sum(r['elapsed'] for r in ok_results)/len(ok_results):.2f}s")
    failed = [r for r in results if r["error"]]
    if failed:
        print(f"FAILED: {', '.join(r['node'] for r in failed)}")


if __name__ == "__main__":
    main()
