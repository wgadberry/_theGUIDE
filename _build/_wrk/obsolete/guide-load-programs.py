#!/usr/bin/env python3
"""
Guide Load Programs - Fetch and load Solana programs from Solscan API

Fetches program list from Solscan Pro API, paginates through all programs,
and ensures they exist in tx_address (with address_type='program') and tx_program.

Usage:
    python guide-load-programs.py
    python guide-load-programs.py --dry-run
    python guide-load-programs.py --max-pages 5
"""

import argparse
import json
import sys
import time
from typing import Dict, List, Optional, Tuple

import requests

try:
    import mysql.connector
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

# Solscan API config
SOLSCAN_API_URL = "https://pro-api.solscan.io/v2.0/program/list"
SOLSCAN_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjcmVhdGVkQXQiOjE3NTg0MzQzODI5MTEsImVtYWlsIjoid2dhZGJlcnJ5QGdtYWlsLmNvbSIsImFjdGlvbiI6InRva2VuLWFwaSIsImFwaVZlcnNpb24iOiJ2MiIsImlhdCI6MTc1ODQzNDM4Mn0.d_ZYyfZGaFdLfYXsuB_M2rT_Q6WkwpxVp0ZqNE_zUAk"

# Known program names (add more as needed)
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

    # Routers/Aggregators
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

    # Other notable programs
    "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH": ("Drift Protocol", "dex"),
    "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY": ("Phoenix", "dex"),
    "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX": ("OpenBook v1", "dex"),
    "opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb": ("OpenBook v2", "dex"),
}


def fetch_programs_page(page: int, page_size: int = 40) -> Tuple[List[dict], int]:
    """
    Fetch a page of programs from Solscan API
    Returns (programs_list, total_count)
    """
    params = {
        "sort_by": "num_txs",
        "sort_order": "desc",
        "page": page,
        "page_size": page_size
    }
    headers = {"token": SOLSCAN_TOKEN}

    response = requests.get(SOLSCAN_API_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()
    if not data.get("success"):
        raise Exception(f"API error: {data.get('errors', {}).get('message', 'Unknown error')}")

    result = data.get("data", {})
    programs = result.get("data", [])
    total = result.get("total", 0)

    return programs, total


def ensure_address(cursor, conn, address: str, address_type: str = "program", label: str = None) -> int:
    """
    Ensure address exists in tx_address with correct type, returns address_id
    Uses INSERT ... ON DUPLICATE KEY UPDATE for atomicity
    """
    cursor.execute("""
        INSERT INTO tx_address (address, address_type, label)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            address_type = CASE WHEN address_type IN ('unknown', '') OR address_type IS NULL THEN VALUES(address_type) ELSE address_type END,
            label = COALESCE(label, VALUES(label))
    """, (address, address_type, label))
    conn.commit()

    # Get the id (either inserted or existing)
    cursor.execute("SELECT id FROM tx_address WHERE address = %s", (address,))
    return cursor.fetchone()[0]


def ensure_program(cursor, conn, address_id: int, name: str = None, program_type: str = "other") -> int:
    """
    Ensure program exists in tx_program, returns program_id
    Uses INSERT ... ON DUPLICATE KEY UPDATE for atomicity
    """
    cursor.execute("""
        INSERT INTO tx_program (program_address_id, name, program_type)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = COALESCE(VALUES(name), name),
            program_type = CASE WHEN program_type = 'other' THEN COALESCE(VALUES(program_type), program_type) ELSE program_type END
    """, (address_id, name, program_type))
    conn.commit()

    # Get the id (either inserted or existing)
    cursor.execute("SELECT id FROM tx_program WHERE program_address_id = %s", (address_id,))
    return cursor.fetchone()[0]


def main():
    parser = argparse.ArgumentParser(description='Fetch and load Solana programs from Solscan API')
    parser.add_argument('--db-host', default='localhost', help='MySQL host')
    parser.add_argument('--db-port', type=int, default=3396, help='MySQL port')
    parser.add_argument('--db-user', default='root', help='MySQL user')
    parser.add_argument('--db-pass', default='rootpassword', help='MySQL password')
    parser.add_argument('--db-name', default='t16o_db', help='MySQL database')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without writing')
    parser.add_argument('--max-pages', type=int, default=0, help='Max pages to fetch (0=all)')
    parser.add_argument('--page-size', type=int, default=40, choices=[10, 20, 30, 40], help='Page size')
    parser.add_argument('--delay', type=float, default=0.5, help='Delay between API calls (seconds)')

    args = parser.parse_args()

    if not HAS_MYSQL and not args.dry_run:
        print("Error: mysql-connector-python not installed")
        return 1

    # Stats
    stats = {
        "pages_fetched": 0,
        "programs_fetched": 0,
        "addresses_created": 0,
        "addresses_updated": 0,
        "programs_created": 0,
        "programs_updated": 0,
        "errors": 0
    }

    conn = None
    cursor = None

    if not args.dry_run:
        print(f"Connecting to MySQL {args.db_host}:{args.db_port}/{args.db_name}...")
        conn = mysql.connector.connect(
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_pass,
            database=args.db_name
        )
        cursor = conn.cursor()

    try:
        # Fetch first page to get total count
        print("Fetching program list from Solscan API...")
        programs, total = fetch_programs_page(1, args.page_size)

        total_pages = (total + args.page_size - 1) // args.page_size
        if args.max_pages > 0:
            total_pages = min(total_pages, args.max_pages)

        print(f"Total programs: {total}")
        print(f"Pages to fetch: {total_pages} (page_size={args.page_size})")
        print()

        if args.dry_run:
            print("[DRY RUN] Would process programs:")
            for p in programs[:10]:
                addr = p.get("program", "")
                known = KNOWN_PROGRAMS.get(addr)
                name = known[0] if known else "(unknown)"
                print(f"  {addr[:20]}... - {name}")
            if len(programs) > 10:
                print(f"  ... and {len(programs) - 10} more on this page")
            print(f"\nWould fetch {total_pages} pages total")
            return 0

        # Process all pages
        page = 1
        while page <= total_pages:
            if page > 1:
                time.sleep(args.delay)
                programs, _ = fetch_programs_page(page, args.page_size)

            stats["pages_fetched"] += 1

            for prog_data in programs:
                address = prog_data.get("program", "")
                if not address:
                    continue

                stats["programs_fetched"] += 1

                # Check if we have known info for this program
                known = KNOWN_PROGRAMS.get(address)
                name = known[0] if known else None
                prog_type = known[1] if known else "other"

                try:
                    # Check if address exists before to track created vs updated
                    cursor.execute("SELECT id FROM tx_address WHERE address = %s", (address,))
                    existed = cursor.fetchone() is not None

                    # Ensure address exists
                    addr_id = ensure_address(cursor, conn, address, "program", name)

                    if existed:
                        stats["addresses_updated"] += 1
                    else:
                        stats["addresses_created"] += 1

                    # Check if program exists before
                    cursor.execute("SELECT id FROM tx_program WHERE program_address_id = %s", (addr_id,))
                    prog_existed = cursor.fetchone() is not None

                    # Ensure program exists
                    ensure_program(cursor, conn, addr_id, name, prog_type)

                    if prog_existed:
                        stats["programs_updated"] += 1
                    else:
                        stats["programs_created"] += 1

                except Exception as e:
                    stats["errors"] += 1
                    print(f"  [!] Error processing {address[:20]}...: {e}")

            print(f"  Page {page}/{total_pages} - {len(programs)} programs processed")
            page += 1

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")
        stats["errors"] += 1
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    # Print summary
    print()
    print("=" * 50)
    print("Summary:")
    print(f"  Pages fetched:      {stats['pages_fetched']}")
    print(f"  Programs fetched:   {stats['programs_fetched']}")
    print(f"  Addresses created:  {stats['addresses_created']}")
    print(f"  Addresses updated:  {stats['addresses_updated']}")
    print(f"  Programs created:   {stats['programs_created']}")
    print(f"  Programs updated:   {stats['programs_updated']}")
    print(f"  Errors:             {stats['errors']}")
    print("=" * 50)

    return 0 if stats["errors"] == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
