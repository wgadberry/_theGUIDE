#!/usr/bin/env python3
"""
Guide Market Loader - Fetch market/pool data from Solscan into tx_pool

Queries tx_guide for distinct tokens, fetches market data from Solscan
/token/markets, and upserts pool records with program, token pair, and
DEX metadata into tx_pool.

Optimized: batch operations per token, config from guide-config.json,
parallel API prefetch.

Usage:
    python guide-load-markets.py
    python guide-load-markets.py --limit 50 --delay .2
    python guide-load-markets.py --sample <mint>
    python guide-load-markets.py --db-host 34.196.15.116 --db-port 3397
"""

import argparse
import json
import os
import sys
import time
from typing import Dict, List, Optional, Tuple
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

SOLSCAN_API_BASE = _cfg.get('SOLSCAN_API', 'https://pro-api.solscan.io/v2.0')
SOLSCAN_TOKEN = _cfg.get('SOLSCAN_TOKEN', '')

SKIP_TOKENS = {
    'So11111111111111111111111111111111111111112',   # WSOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', # USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',  # USDT
}


def get_connection(args):
    return mysql.connector.connect(
        host=args.db_host, port=args.db_port,
        user=args.db_user, password=args.db_pass,
        database=args.db_name)


def fetch_token_markets(session, token_mint: str, page_size: int = 10) -> List[dict]:
    """Fetch markets for a token from Solscan."""
    try:
        resp = session.get(
            f'{SOLSCAN_API_BASE}/token/markets',
            params={'token[]': token_mint, 'page': 1, 'page_size': page_size},
            timeout=30)
        if resp.status_code != 200:
            return []
        data = resp.json()
        if not data.get('success'):
            return []
        return data.get('data', [])
    except Exception as e:
        print(f"    [!] API error: {e}")
        return []


def get_tokens_needing_markets(cursor, limit: int) -> List[Tuple[str, int]]:
    """Get tokens from tx_guide that have pools with missing metadata."""
    cursor.execute("""
        SELECT a.address, t.id
        FROM tx_guide g
        JOIN tx_token t ON t.id = g.token_id
        JOIN tx_address a ON a.id = t.mint_address_id
        WHERE t.id > 0
          AND t.id NOT IN (
              SELECT token1_id FROM tx_pool WHERE token1_id IS NOT NULL
              UNION
              SELECT token2_id FROM tx_pool WHERE token2_id IS NOT NULL
          )
          AND a.address NOT IN ('So11111111111111111111111111111111111111112',
                                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                                'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB')
        GROUP BY a.address, t.id
        ORDER BY MAX(g.id) DESC
        LIMIT %s
    """, (limit,))
    return [(row[0], row[1]) for row in cursor.fetchall()]


def process_markets_batch(cursor, conn, token_mint: str, markets: List[dict]) -> Tuple[int, int]:
    """Process all markets for a token in one batch. Returns (upserted, errors)."""
    if not markets:
        return 0, 0

    upserted = 0
    errors = 0

    # Collect all addresses we'll need
    all_addresses = set()
    for m in markets:
        for field in ['pool_id', 'program_id', 'token_account_1', 'token_account_2']:
            val = m.get(field)
            if val:
                all_addresses.add(val)
        for field in ['token_1', 'token_2']:
            val = m.get(field)
            if val:
                all_addresses.add(val)

    if not all_addresses:
        return 0, 0

    # Batch ensure all addresses exist
    addr_list = list(all_addresses)
    for i in range(0, len(addr_list), 50):
        chunk = addr_list[i:i + 50]
        cursor.executemany(
            "INSERT IGNORE INTO tx_address (address, address_type) VALUES (%s, %s)",
            [(a, 'unknown') for a in chunk])

    # Fetch all address IDs in one query
    placeholders = ','.join(['%s'] * len(addr_list))
    cursor.execute(
        f"SELECT id, address FROM tx_address WHERE address IN ({placeholders})",
        addr_list)
    addr_ids = {row[1]: row[0] for row in cursor.fetchall()}

    # Process each market
    for m in markets:
        try:
            pool_addr = m.get('pool_id', '')
            program_addr = m.get('program_id', '')
            token1_mint = m.get('token_1', '')
            token2_mint = m.get('token_2', '')
            token_acct1 = m.get('token_account_1', '')
            token_acct2 = m.get('token_account_2', '')

            pool_addr_id = addr_ids.get(pool_addr)
            if not pool_addr_id:
                continue

            # Update pool address type
            cursor.execute(
                "UPDATE tx_address SET address_type = 'pool' WHERE id = %s AND address_type IN ('unknown', '')",
                (pool_addr_id,))

            # Update program address type
            program_addr_id = addr_ids.get(program_addr)
            if program_addr_id:
                cursor.execute(
                    "UPDATE tx_address SET address_type = 'program' WHERE id = %s AND address_type IN ('unknown', '')",
                    (program_addr_id,))

            # Ensure program exists
            program_id = None
            if program_addr_id:
                cursor.execute(
                    "INSERT IGNORE INTO tx_program (program_address_id, program_type) VALUES (%s, 'dex')",
                    (program_addr_id,))
                cursor.execute("SELECT id FROM tx_program WHERE program_address_id = %s", (program_addr_id,))
                row = cursor.fetchone()
                if row:
                    program_id = row[0]

            # Lookup tokens (don't create — they should already exist from pipeline)
            token1_id = None
            token2_id = None
            if token1_mint:
                t1_addr_id = addr_ids.get(token1_mint)
                if t1_addr_id:
                    cursor.execute("SELECT id FROM tx_token WHERE mint_address_id = %s", (t1_addr_id,))
                    row = cursor.fetchone()
                    if row:
                        token1_id = row[0]
            if token2_mint:
                t2_addr_id = addr_ids.get(token2_mint)
                if t2_addr_id:
                    cursor.execute("SELECT id FROM tx_token WHERE mint_address_id = %s", (t2_addr_id,))
                    row = cursor.fetchone()
                    if row:
                        token2_id = row[0]

            token_acct1_id = addr_ids.get(token_acct1)
            token_acct2_id = addr_ids.get(token_acct2)

            # Derive DEX name and token pair from program label
            dex = None
            if program_addr_id:
                cursor.execute("SELECT label FROM tx_address WHERE id = %s", (program_addr_id,))
                row = cursor.fetchone()
                if row and row[0]:
                    dex = row[0].split('(')[0].strip() if '(' in row[0] else row[0]

            # Upsert tx_pool
            cursor.execute("""
                INSERT INTO tx_pool (pool_address_id, program_id, token1_id, token2_id,
                                     token_account1_id, token_account2_id, dex, attempt_cnt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
                ON DUPLICATE KEY UPDATE
                    program_id = COALESCE(VALUES(program_id), program_id),
                    token1_id = COALESCE(VALUES(token1_id), token1_id),
                    token2_id = COALESCE(VALUES(token2_id), token2_id),
                    token_account1_id = COALESCE(VALUES(token_account1_id), token_account1_id),
                    token_account2_id = COALESCE(VALUES(token_account2_id), token_account2_id),
                    dex = COALESCE(VALUES(dex), dex),
                    attempt_cnt = attempt_cnt + 1
            """, (pool_addr_id, program_id, token1_id, token2_id,
                  token_acct1_id, token_acct2_id, dex))

            upserted += 1

        except Exception as e:
            print(f"    [!] Error: {e}")
            errors += 1

    # Single commit per token batch
    conn.commit()
    return upserted, errors


def main():
    parser = argparse.ArgumentParser(description='Load market/pool data from Solscan')
    parser.add_argument('--limit', type=int, default=100, help='Max tokens to process')
    parser.add_argument('--markets-per-token', type=int, default=10, help='Max markets per token')
    parser.add_argument('--db-host', default=_cfg.get('DB_HOST', 'localhost'))
    parser.add_argument('--db-port', type=int, default=_cfg.get('DB_PORT', 3396))
    parser.add_argument('--db-user', default=_cfg.get('DB_USER', 'root'))
    parser.add_argument('--db-pass', default=_cfg.get('DB_PASSWORD', 'rootpassword'))
    parser.add_argument('--db-name', default=_cfg.get('DB_NAME', 't16o_db'))
    parser.add_argument('--delay', type=float, default=0.2, help='Delay between API calls')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--sample', metavar='MINT', help='Test single token')
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update({'token': SOLSCAN_TOKEN})

    if args.sample:
        print(f"Fetching markets for {args.sample}...\n")
        markets = fetch_token_markets(session, args.sample, args.markets_per_token)
        print(json.dumps(markets, indent=2) if markets else "No markets found")
        return 0

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        return 1

    t0 = time.time()
    conn = get_connection(args)
    cursor = conn.cursor()

    print(f"Fetching tokens from tx_guide (limit={args.limit})...")
    tokens = get_tokens_needing_markets(cursor, args.limit)
    print(f"Found {len(tokens)} tokens to process\n")

    total_markets = 0
    total_errors = 0

    for i, (mint, token_id) in enumerate(tokens):
        print(f"[{i+1}/{len(tokens)}] {mint[:20]}…", end='')

        markets = fetch_token_markets(session, mint, args.markets_per_token)

        if not markets:
            print(" — no markets")
            time.sleep(args.delay)
            continue

        if args.dry_run:
            print(f" — {len(markets)} markets (dry run)")
            for m in markets[:3]:
                pool = m.get('pool_id', '?')[:16]
                vol = m.get('total_volume_24h', 0)
                print(f"    Pool: {pool}… Vol24h: ${vol:,.0f}")
            total_markets += len(markets)
        else:
            upserted, errs = process_markets_batch(cursor, conn, mint, markets)
            print(f" — {upserted} pools upserted" + (f", {errs} errors" if errs else ""))
            total_markets += upserted
            total_errors += errs

        time.sleep(args.delay)

    cursor.close()
    conn.close()

    elapsed = time.time() - t0
    print(f"\n{'='*50}")
    print(f"  Tokens processed: {len(tokens)}")
    print(f"  Pools upserted:   {total_markets}")
    print(f"  Errors:           {total_errors}")
    print(f"  Elapsed:          {elapsed:.1f}s")
    print(f"{'='*50}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
