#!/usr/bin/env python3
"""
Guide Load Programs - Fetch and load Solana programs from Solscan API

Fetches program list from Solscan Pro API, paginates through all programs,
and ensures they exist in tx_address (with address_type='program') and tx_program.

Optimized: batch upserts per page (1 commit per page instead of per-row),
parallel Solscan fetches, eliminated redundant SELECTs.

Usage:
    python guide-load-programs.py
    python guide-load-programs.py --dry-run
    python guide-load-programs.py --max-pages 5
    python guide-load-programs.py --page-size 40 --delay .1 --db-host 34.196.15.116 --db-port 3397
"""

import argparse
import json
import os
import sys
import time
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

try:
    import mysql.connector
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

# ─── Config ──────────────────────────────────────────────────────────────────

_config_candidates = [
    os.path.join(os.path.dirname(__file__), 'guide-config.json'),
    os.path.join(os.getcwd(), 'guide-config.json'),
]
_cfg = {}
for _cp in _config_candidates:
    if os.path.exists(_cp):
        with open(_cp) as f:
            _cfg = json.load(f)
        break

SOLSCAN_API_URL = _cfg.get('SOLSCAN_API', 'https://pro-api.solscan.io/v2.0') + '/program/list'
SOLSCAN_TOKEN = _cfg.get('SOLSCAN_TOKEN', '')

# Known program names
KNOWN_PROGRAMS: Dict[str, Tuple[str, str]] = {
    # System programs
    "11111111111111111111111111111111": ("System Program", "system"),
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA": ("Token Program", "token"),
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb": ("Token-2022 Program", "token"),
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL": ("Associated Token Account Program", "token"),
    "ComputeBudget111111111111111111111111111111": ("Compute Budget Program", "compute"),
    "Vote111111111111111111111111111111111111111": ("Vote Program", "system"),
    "Stake11111111111111111111111111111111111111": ("Stake Program", "system"),
    "Config1111111111111111111111111111111111111": ("Config Program", "system"),
    # DEXes
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": ("Jupiter Aggregator v6", "dex"),
    "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB": ("Jupiter Aggregator v4", "dex"),
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": ("Orca Whirlpool", "dex"),
    "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP": ("Orca Swap v2", "dex"),
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": ("Raydium CLMM", "dex"),
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": ("Raydium AMM v4", "dex"),
    "routeUGWgWzqBWFcrCfv8tritsqukccJPu3q5GPP3xS": ("Raydium Route", "router"),
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": ("Meteora DLMM", "dex"),
    "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB": ("Meteora Pools", "dex"),
    # Pump.fun
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": ("Pump.fun", "dex"),
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA": ("Pump.fun AMM", "dex"),
    # Routers
    "ARu4n5mFdZogZAravu7CcizaojWnS6oqka37gdLT5SZn": ("OKX Router", "router"),
    "HV1KXxWFaSeriyFvXyx48FqG9BoFbfinB8njCJonqP7K": ("OKX Dex", "router"),
    # Lending
    "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo": ("Solend", "lending"),
    "MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA": ("Marginfi", "lending"),
    "KLend2g3cP87ber41GXTBEvEv4P8TZkNGRBVYUdFo6H": ("Kamino Lending", "lending"),
    # NFT
    "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s": ("Metaplex Token Metadata", "nft"),
    "M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K": ("Magic Eden v2", "nft"),
    "TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN": ("Tensor Swap", "nft"),
    # Other
    "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH": ("Drift Protocol", "dex"),
    "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY": ("Phoenix", "dex"),
    "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX": ("OpenBook v1", "dex"),
    "opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb": ("OpenBook v2", "dex"),
}


def fetch_programs_page(page: int, page_size: int = 40) -> Tuple[List[dict], int]:
    """Fetch a page of programs from Solscan API."""
    params = {
        "sort_by": "num_txs",
        "sort_order": "desc",
        "page": page,
        "page_size": page_size,
    }
    headers = {"token": SOLSCAN_TOKEN}
    response = requests.get(SOLSCAN_API_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()
    if not data.get("success"):
        raise Exception(f"API error: {data.get('errors', {}).get('message', 'Unknown error')}")
    result = data.get("data", {})
    return result.get("data", []), result.get("total", 0)


def process_page_batch(cursor, conn, programs: List[dict], stats: dict):
    """Process an entire page of programs in one batch — single commit."""
    if not programs:
        return

    # Step 1: Batch upsert all addresses
    addr_values = []
    for prog_data in programs:
        address = prog_data.get("program", "")
        if not address:
            continue
        known = KNOWN_PROGRAMS.get(address)
        label = known[0] if known else None
        addr_values.append((address, "program", label))

    if not addr_values:
        return

    # Batch INSERT IGNORE for addresses
    cursor.executemany("""
        INSERT INTO tx_address (address, address_type, label)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            address_type = CASE
                WHEN address_type IN ('unknown', '') OR address_type IS NULL
                THEN VALUES(address_type) ELSE address_type END,
            label = COALESCE(label, VALUES(label))
    """, addr_values)

    # Step 2: Fetch all address IDs in one query
    addresses = [v[0] for v in addr_values]
    placeholders = ','.join(['%s'] * len(addresses))
    cursor.execute(
        f"SELECT id, address FROM tx_address WHERE address IN ({placeholders})",
        addresses)
    addr_id_map = {row[1]: row[0] for row in cursor.fetchall()}

    # Step 3: Batch upsert all programs
    prog_values = []
    for prog_data in programs:
        address = prog_data.get("program", "")
        addr_id = addr_id_map.get(address)
        if not addr_id:
            continue
        known = KNOWN_PROGRAMS.get(address)
        name = known[0] if known else None
        prog_type = known[1] if known else "other"
        prog_values.append((addr_id, name, prog_type))

    if prog_values:
        cursor.executemany("""
            INSERT INTO tx_program (program_address_id, name, program_type)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = COALESCE(VALUES(name), name),
                program_type = CASE
                    WHEN program_type = 'other'
                    THEN COALESCE(VALUES(program_type), program_type)
                    ELSE program_type END
        """, prog_values)

    # Single commit for the entire page
    conn.commit()

    stats["programs_fetched"] += len(addr_values)
    stats["addresses_upserted"] += len(addr_values)
    stats["programs_upserted"] += len(prog_values)


def main():
    parser = argparse.ArgumentParser(description='Fetch and load Solana programs from Solscan API')
    parser.add_argument('--db-host', default=_cfg.get('DB_HOST', 'localhost'), help='MySQL host')
    parser.add_argument('--db-port', type=int, default=_cfg.get('DB_PORT', 3396), help='MySQL port')
    parser.add_argument('--db-user', default=_cfg.get('DB_USER', 'root'), help='MySQL user')
    parser.add_argument('--db-pass', default=_cfg.get('DB_PASSWORD', 'rootpassword'), help='MySQL password')
    parser.add_argument('--db-name', default=_cfg.get('DB_NAME', 't16o_db'), help='MySQL database')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
    parser.add_argument('--max-pages', type=int, default=0, help='Max pages (0=all)')
    parser.add_argument('--page-size', type=int, default=40, choices=[10, 20, 30, 40])
    parser.add_argument('--delay', type=float, default=0.3, help='Delay between API calls (seconds)')
    parser.add_argument('--prefetch', type=int, default=3, help='Pages to prefetch in parallel')
    args = parser.parse_args()

    if not HAS_MYSQL and not args.dry_run:
        print("Error: mysql-connector-python not installed")
        return 1

    stats = {
        "pages_fetched": 0,
        "programs_fetched": 0,
        "addresses_upserted": 0,
        "programs_upserted": 0,
        "errors": 0,
    }

    t0 = time.time()

    # Fetch first page to get total count
    print("Fetching program list from Solscan API...")
    programs_page1, total = fetch_programs_page(1, args.page_size)

    total_pages = (total + args.page_size - 1) // args.page_size
    if args.max_pages > 0:
        total_pages = min(total_pages, args.max_pages)

    print(f"Total programs: {total}")
    print(f"Pages to fetch: {total_pages} (page_size={args.page_size})")

    if args.dry_run:
        print(f"\n[DRY RUN] Sample from page 1:")
        for p in programs_page1[:10]:
            addr = p.get("program", "")
            known = KNOWN_PROGRAMS.get(addr)
            name = known[0] if known else "(unknown)"
            print(f"  {addr[:24]}… — {name}")
        if len(programs_page1) > 10:
            print(f"  … and {len(programs_page1) - 10} more")
        return 0

    conn = mysql.connector.connect(
        host=args.db_host, port=args.db_port,
        user=args.db_user, password=args.db_pass,
        database=args.db_name)
    cursor = conn.cursor()

    try:
        # Process page 1
        process_page_batch(cursor, conn, programs_page1, stats)
        stats["pages_fetched"] += 1
        print(f"  Page 1/{total_pages} — {len(programs_page1)} programs ({stats['programs_fetched']} total)")

        # Process remaining pages with prefetch
        page = 2
        while page <= total_pages:
            # Prefetch next N pages in parallel
            end_page = min(page + args.prefetch, total_pages + 1)
            prefetch_pages = list(range(page, end_page))

            fetched = {}
            with ThreadPoolExecutor(max_workers=args.prefetch) as pool:
                futures = {pool.submit(fetch_programs_page, p, args.page_size): p
                           for p in prefetch_pages}
                for f in as_completed(futures):
                    p = futures[f]
                    try:
                        programs, _ = f.result()
                        fetched[p] = programs
                    except Exception as e:
                        print(f"  [!] Page {p} fetch error: {e}")
                        stats["errors"] += 1

            # Process fetched pages in order
            for p in prefetch_pages:
                programs = fetched.get(p, [])
                if programs:
                    process_page_batch(cursor, conn, programs, stats)
                    stats["pages_fetched"] += 1
                    print(f"  Page {p}/{total_pages} — {len(programs)} programs "
                          f"({stats['programs_fetched']} total)")

            page = end_page
            if page <= total_pages:
                time.sleep(args.delay)

    except KeyboardInterrupt:
        print("\nInterrupted")
    except Exception as e:
        print(f"\nError: {e}")
        stats["errors"] += 1
    finally:
        cursor.close()
        conn.close()

    elapsed = time.time() - t0
    print(f"\n{'='*50}")
    print(f"  Pages fetched:       {stats['pages_fetched']}")
    print(f"  Programs processed:  {stats['programs_fetched']}")
    print(f"  Addresses upserted:  {stats['addresses_upserted']}")
    print(f"  Programs upserted:   {stats['programs_upserted']}")
    print(f"  Errors:              {stats['errors']}")
    print(f"  Elapsed:             {elapsed:.1f}s ({stats['programs_fetched']/max(elapsed,1):.0f} programs/sec)")
    print(f"{'='*50}")

    return 0 if stats["errors"] == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
