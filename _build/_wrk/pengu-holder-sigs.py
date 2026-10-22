#!/usr/bin/env python3
"""
pengu-holder-sigs.py — Fetch all SPL transfer signatures for PENGU top 80 holders.

Uses Solscan to get top 80 holders, then Chainstack RPC (getSignaturesForAddress)
to page through each holder's full transaction history. Deduplicates signatures
across all holders and writes results to a local JSON file.

Usage:
    python pengu-holder-sigs.py [--max-sigs 5000] [--max-pages 50] [--threads 5]
"""

import argparse
import json
import os
import sys
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, 'guide-config.json')

with open(CONFIG_PATH) as f:
    CFG = json.load(f)

RPC_URL = CFG['RPC_URL']
SOLSCAN_API = CFG.get('SOLSCAN_API', 'https://pro-api.solscan.io/v2.0')
SOLSCAN_TOKEN = CFG.get('SOLSCAN_TOKEN', '')

PENGU_MINT = '2zMMhcVQEXDtdE6vsFS7S7D5oUodfJHE8vd1gnBouauv'
OUTPUT_FILE = os.path.join(SCRIPT_DIR, 'pengu_holder_sigs.json')


# ── Solscan: Fetch Top 80 Holders ───────────────────────────────────
def fetch_holders(mint: str, pages: int = 2, page_size: int = 40) -> list:
    """Fetch top holders from Solscan /v2.0/token/holders."""
    headers = {'token': SOLSCAN_TOKEN}
    all_holders = []
    for page in range(1, pages + 1):
        try:
            resp = requests.get(
                f'{SOLSCAN_API}/token/holders',
                params={'address': mint, 'page_size': page_size, 'page': page},
                headers=headers, timeout=15)
            if resp.status_code != 200:
                print(f"  [WARN] Solscan page {page} returned {resp.status_code}")
                continue
            items = resp.json().get('data', {}).get('items', [])
            all_holders.extend(items)
            print(f"  Page {page}: {len(items)} holders")
        except Exception as e:
            print(f"  [ERR] Solscan page {page}: {e}")
    return all_holders


# ── RPC: Fetch Signatures ───────────────────────────────────────────
def fetch_signatures_rpc(session, address, limit=1000, before=None):
    """Single RPC call to getSignaturesForAddress."""
    params_obj = {"limit": min(limit, 1000)}
    if before:
        params_obj["before"] = before

    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "getSignaturesForAddress",
        "params": [address, params_obj]
    }

    for attempt in range(3):
        try:
            resp = session.post(RPC_URL, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if 'error' in data:
                print(f"    [RPC ERR] {data['error']}")
                return []
            return data.get('result', [])
        except (requests.ConnectionError, requests.exceptions.ConnectionError):
            if attempt < 2:
                session.close()
                time.sleep(0.5)
                continue
            raise
        except Exception as e:
            if attempt < 2:
                time.sleep(0.5)
                continue
            print(f"    [RPC ERR] {e}")
            return []


def fetch_all_sigs_for_address(address: str, max_sigs: int, max_pages: int,
                               delay: float = 0.2) -> dict:
    """Fetch all signatures for one address, return stats + sig set."""
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})

    sigs = set()
    before = None
    pages = 0
    errors = 0
    oldest_ts = None
    newest_ts = None

    while len(sigs) < max_sigs and pages < max_pages:
        results = fetch_signatures_rpc(session, address, limit=1000, before=before)
        if not results:
            break

        pages += 1
        for sig_obj in results:
            sig = sig_obj.get('signature')
            if not sig:
                continue
            if sig_obj.get('err') is not None:
                errors += 1
                continue
            sigs.add(sig)
            bt = sig_obj.get('blockTime', 0)
            if bt:
                if newest_ts is None or bt > newest_ts:
                    newest_ts = bt
                if oldest_ts is None or bt < oldest_ts:
                    oldest_ts = bt

        before = results[-1].get('signature')

        if len(results) < 1000:
            break  # no more pages

        if delay > 0:
            time.sleep(delay)

    session.close()
    return {
        'address': address,
        'sig_count': len(sigs),
        'pages': pages,
        'errors': errors,
        'oldest_ts': oldest_ts,
        'newest_ts': newest_ts,
        'sigs': sigs,
    }


# ── Main ────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description='Fetch PENGU holder signatures')
    parser.add_argument('--max-sigs', type=int, default=10000,
                        help='Max signatures per holder (default: 10000)')
    parser.add_argument('--max-pages', type=int, default=100,
                        help='Max RPC pages per holder (default: 100)')
    parser.add_argument('--threads', type=int, default=5,
                        help='Parallel threads (default: 5)')
    parser.add_argument('--delay', type=float, default=0.2,
                        help='Delay between RPC pages in seconds (default: 0.2)')
    parser.add_argument('--mint', type=str, default=PENGU_MINT,
                        help='Token mint address')
    args = parser.parse_args()

    t0 = time.time()
    print(f"=== PENGU Holder Signature Fetcher ===")
    print(f"Mint: {args.mint}")
    print(f"Max sigs/holder: {args.max_sigs}, Max pages/holder: {args.max_pages}")
    print(f"Threads: {args.threads}, Delay: {args.delay}s")
    print()

    # Step 1: Get top 80 holders
    print("[1/3] Fetching top 80 holders from Solscan...")
    holders = fetch_holders(args.mint)
    if not holders:
        print("ERROR: No holders found!")
        sys.exit(1)

    # Deduplicate by owner address
    seen_owners = set()
    unique_holders = []
    for h in holders:
        owner = h.get('owner', '')
        if owner and owner not in seen_owners:
            seen_owners.add(owner)
            unique_holders.append({
                'owner': owner,
                'amount': h.get('amount', 0),
                'percentage': h.get('percentage', 0),
            })

    print(f"  {len(unique_holders)} unique holder addresses")
    print()

    # Step 2: Fetch signatures for each holder in parallel
    print(f"[2/3] Fetching signatures ({args.threads} threads)...")
    all_sigs = set()
    holder_stats = []
    completed = 0

    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        futures = {
            pool.submit(
                fetch_all_sigs_for_address,
                h['owner'], args.max_sigs, args.max_pages, args.delay
            ): h for h in unique_holders
        }

        for f in as_completed(futures):
            h = futures[f]
            try:
                result = f.result()
                completed += 1
                before_dedup = len(all_sigs)
                all_sigs.update(result['sigs'])
                new_sigs = len(all_sigs) - before_dedup

                oldest = (datetime.utcfromtimestamp(result['oldest_ts']).strftime('%Y-%m-%d')
                          if result['oldest_ts'] else 'N/A')
                newest = (datetime.utcfromtimestamp(result['newest_ts']).strftime('%Y-%m-%d')
                          if result['newest_ts'] else 'N/A')

                holder_stats.append({
                    'address': result['address'],
                    'sig_count': result['sig_count'],
                    'new_unique': new_sigs,
                    'pages': result['pages'],
                    'errors': result['errors'],
                    'oldest': oldest,
                    'newest': newest,
                    'percentage': h['percentage'],
                })

                print(f"  [{completed}/{len(unique_holders)}] "
                      f"{result['address'][:8]}… — "
                      f"{result['sig_count']:,} sigs ({new_sigs:,} new) "
                      f"in {result['pages']} pages "
                      f"({oldest} → {newest})")

            except Exception as e:
                completed += 1
                print(f"  [{completed}/{len(unique_holders)}] "
                      f"{h['owner'][:8]}… — ERROR: {e}")

    elapsed = time.time() - t0
    print()

    # Step 3: Write results
    print(f"[3/3] Writing results...")

    # Sort holder stats by percentage descending
    holder_stats.sort(key=lambda x: x['percentage'], reverse=True)

    output = {
        'mint': args.mint,
        'generated_at': datetime.utcnow().isoformat(),
        'elapsed_seconds': round(elapsed, 1),
        'config': {
            'max_sigs_per_holder': args.max_sigs,
            'max_pages_per_holder': args.max_pages,
            'threads': args.threads,
            'delay': args.delay,
        },
        'summary': {
            'total_holders': len(unique_holders),
            'total_unique_sigs': len(all_sigs),
            'total_sigs_before_dedup': sum(h['sig_count'] for h in holder_stats),
            'dedup_ratio': round(
                1 - len(all_sigs) / max(1, sum(h['sig_count'] for h in holder_stats)),
                3),
        },
        'holders': holder_stats,
        'signatures': sorted(list(all_sigs)),  # sorted for deterministic output
    }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    print(f"  Wrote {OUTPUT_FILE}")
    print()

    # Summary
    total_before = sum(h['sig_count'] for h in holder_stats)
    print(f"=== Summary ===")
    print(f"  Holders:           {len(unique_holders)}")
    print(f"  Total sigs found:  {total_before:,}")
    print(f"  After dedup:       {len(all_sigs):,}")
    print(f"  Dedup ratio:       {output['summary']['dedup_ratio']:.1%}")
    print(f"  Elapsed:           {elapsed:.1f}s")
    print()

    # Top 10 holders by sig count
    print("  Top 10 by sig count:")
    for h in sorted(holder_stats, key=lambda x: x['sig_count'], reverse=True)[:10]:
        print(f"    {h['address'][:12]}… — {h['sig_count']:>8,} sigs "
              f"({h['pages']:>3} pages, {h['oldest']} → {h['newest']})")


if __name__ == '__main__':
    main()
