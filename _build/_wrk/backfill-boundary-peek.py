#!/usr/bin/env python3
"""
Backfill boundary_sig for tx_boundary rows where it's NULL.

Reads each row missing a boundary_sig, peeks the N+1th signature via RPC
(one call per row), and updates the row in place. No re-fetching or cascading.

Usage:
    python backfill-boundary-peek.py                # Process all NULL boundary rows
    python backfill-boundary-peek.py --dry-run      # Preview only
    python backfill-boundary-peek.py --limit 50     # Process first 50 rows
"""

import argparse
import os
import sys
import requests
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from t16o_exchange.guide.common.config import get_db_config, get_rpc_config

import mysql.connector

DB_CONFIG = get_db_config()
RPC_URL = get_rpc_config()['url']


def get_db_connection():
    return mysql.connector.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        autocommit=False
    )


def fetch_peek_sig(session, address, before_sig):
    """Fetch the N+1th signature: one sig older than before_sig for this address."""
    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "getSignaturesForAddress",
        "params": [address, {"limit": 1, "before": before_sig}]
    }
    for attempt in range(3):
        try:
            resp = session.post(RPC_URL, json=payload)
            resp.raise_for_status()
            result = resp.json().get('result', [])
            if result and result[0].get('signature'):
                return result[0]['signature'], result[0].get('blockTime')
            return None, None
        except (requests.ConnectionError, requests.exceptions.ConnectionError):
            if attempt < 2:
                session.close()
                time.sleep(0.5)
                continue
            raise


def main():
    parser = argparse.ArgumentParser(description='Backfill boundary_sig via RPC peek')
    parser.add_argument('--dry-run', action='store_true', help='Preview only, no DB writes')
    parser.add_argument('--limit', type=int, default=0, help='Max rows to process (0=all)')
    args = parser.parse_args()

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Find boundary rows missing boundary_sig (backfilled rows without peek)
    query = """
        SELECT b.id, b.address_id, b.oldest_fetched_sig, b.oldest_fetched_block_time,
               b.latest_fetched_sig, b.request_log_id,
               a.address
        FROM tx_boundary b
        JOIN tx_address a ON a.id = b.address_id
        WHERE b.boundary_sig IS NULL
        ORDER BY b.id
    """
    if args.limit > 0:
        query += f" LIMIT {args.limit}"

    cursor.execute(query)
    rows = cursor.fetchall()

    print(f"Found {len(rows)} boundary rows to peek")
    if not rows:
        return

    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})

    updated = 0
    end_of_history = 0
    errors = 0

    for i, row in enumerate(rows):
        bid = row['id']
        address = row['address']
        oldest_sig = row['oldest_fetched_sig']

        try:
            peek_sig, peek_bt = fetch_peek_sig(session, address, oldest_sig)

            if args.dry_run:
                status = f"peek={peek_sig[:20]}... bt={peek_bt}" if peek_sig else "END OF HISTORY"
                print(f"  [{i+1}/{len(rows)}] boundary {bid}: {address[:12]}.. oldest={oldest_sig[:12]}.. -> {status}")
                continue

            if peek_sig:
                cursor.execute("""
                    UPDATE tx_boundary
                    SET boundary_sig = %s, boundary_block_time = %s
                    WHERE id = %s
                """, (peek_sig, peek_bt, bid))
                conn.commit()
                updated += 1
                print(f"  [{i+1}/{len(rows)}] boundary {bid}: peek={peek_sig[:20]}.. bt={peek_bt}")
            else:
                # End of history — leave boundary_sig NULL, but mark as known end
                # We could set a flag here if needed
                end_of_history += 1
                print(f"  [{i+1}/{len(rows)}] boundary {bid}: END OF HISTORY for {address[:12]}..")

            # Rate limit: ~2 RPC calls per second
            if (i + 1) % 2 == 0:
                time.sleep(0.5)

        except Exception as e:
            errors += 1
            print(f"  [{i+1}/{len(rows)}] boundary {bid}: ERROR - {e}")

    print(f"\nDone. Updated: {updated}, End of history: {end_of_history}, Errors: {errors}")

    cursor.close()
    conn.close()
    session.close()


if __name__ == '__main__':
    main()
