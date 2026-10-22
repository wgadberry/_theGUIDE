#!/usr/bin/env python3
"""
Backfill tx_boundary gaps by sending gap-fill requests to the producer via the gateway API.

Reads gap boundary rows from the DB, sends each gap as a producer trigger request
with before/until time boundaries so the producer fetches only the missing range.

Usage:
    python backfill-gaps.py                  # Process all gaps
    python backfill-gaps.py --dry-run        # Preview only
    python backfill-gaps.py --limit 10       # Process first 10 gaps
    python backfill-gaps.py --delay 5        # 5 seconds between requests (default 3)
"""

import argparse
import json
import os
import sys
import requests
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from t16o_exchange.guide.common.config import get_db_config

import mysql.connector

DB_CONFIG = get_db_config()
GATEWAY_URL = os.environ.get('GATEWAY_URL', 'https://svcs.the16oracles.io')
API_KEY = os.environ.get('API_KEY', 'admin_master_key')


def get_db_connection():
    return mysql.connector.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        autocommit=False
    )


def get_gaps(cursor, limit=0):
    """Find time gaps between boundary sessions — where no row's requested range covers the space."""
    query = """
        SELECT
            b.id AS boundary_id,
            a.address,
            b.oldest_fetched_block_time AS gap_start_bt,
            COALESCE(b.request_until_block_time, b.oldest_fetched_block_time) AS effective_until_bt,
            next_session.latest_fetched_sig AS next_latest_sig,
            next_session.latest_fetched_block_time AS next_latest_bt,
            COALESCE(next_session.request_before_block_time, next_session.latest_fetched_block_time) AS next_effective_before_bt
        FROM tx_boundary b
        JOIN tx_address a ON a.id = b.address_id
        -- Find the next session that starts after this one ends
        JOIN LATERAL (
            SELECT b2.latest_fetched_sig, b2.latest_fetched_block_time, b2.request_before_block_time
            FROM tx_boundary b2
            WHERE b2.address_id = b.address_id
              AND COALESCE(b2.request_before_block_time, b2.latest_fetched_block_time) >
                  COALESCE(b.request_until_block_time, b.oldest_fetched_block_time)
            ORDER BY COALESCE(b2.request_before_block_time, b2.latest_fetched_block_time) ASC
            LIMIT 1
        ) next_session ON TRUE
        -- Gap exists: space between this row's effective_until and next row's effective_before
        -- that no other row's requested range covers
        WHERE NOT EXISTS (
            SELECT 1 FROM tx_boundary b2
            WHERE b2.address_id = b.address_id
              AND b2.id != b.id
              AND COALESCE(b2.request_until_block_time, b2.oldest_fetched_block_time)
                  <= COALESCE(b.request_until_block_time, b.oldest_fetched_block_time)
              AND COALESCE(b2.request_before_block_time, b2.latest_fetched_block_time)
                  >= COALESCE(b.request_until_block_time, b.oldest_fetched_block_time)
          )
          -- Only gaps <= 24 hours
          AND (COALESCE(next_session.latest_fetched_block_time, UNIX_TIMESTAMP())
               - COALESCE(b.request_until_block_time, b.oldest_fetched_block_time)) <= 86400
        ORDER BY b.oldest_fetched_block_time ASC
    """
    if limit > 0:
        query += f" LIMIT {limit}"

    cursor.execute(query)
    return cursor.fetchall()


def trigger_producer(session, address, before_sig, until_bt, max_sigs=200):
    """Send a gap-fill request to the producer via the gateway API."""
    payload = {
        'action': 'process',
        'batch': {
            'filters': {
                'addresses': [address],
                'before': before_sig,        # Start fetching BEFORE this sig (going older)
                'until': {'block_time': until_bt} if until_bt else None  # Stop at this time (the next session's start)
            },
            'address_sig_cnt': max_sigs,
            'skip_boundary': True  # Don't create new boundary rows for gap-fills
        }
    }
    # Remove None until
    if payload['batch']['filters']['until'] is None:
        del payload['batch']['filters']['until']

    resp = session.post(
        f"{GATEWAY_URL}/api/trigger/producer",
        json=payload,
        headers={
            'X-API-Key': API_KEY,
            'Content-Type': 'application/json',
            'X-Priority': '3'  # Low priority — backfill
        },
        timeout=30
    )
    return resp.status_code, resp.json()


def main():
    parser = argparse.ArgumentParser(description='Backfill boundary gaps via producer')
    parser.add_argument('--dry-run', action='store_true', help='Preview only, no API calls')
    parser.add_argument('--limit', type=int, default=0, help='Max gaps to process (0=all)')
    parser.add_argument('--delay', type=float, default=3.0, help='Seconds between requests')
    parser.add_argument('--max-sigs', type=int, default=200, help='Max signatures per gap fill')
    args = parser.parse_args()

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    raw_gaps = get_gaps(cursor, args.limit)

    # Deduplicate: only one gap per address + time bucket (within 60s)
    seen = {}
    gaps = []
    for g in raw_gaps:
        key = (g['address'], g['effective_until_bt'] // 60)
        if key not in seen:
            seen[key] = True
            gaps.append(g)

    print(f"Found {len(raw_gaps)} gap rows, {len(raw_gaps) - len(gaps)} duplicates skipped, {len(gaps)} to fill")
    if not gaps:
        return

    session = requests.Session()
    sent = 0
    errors = 0

    for i, gap in enumerate(gaps):
        bid = gap['boundary_id']
        address = gap['address']
        gap_start = gap['effective_until_bt']  # lower bound of the gap (where coverage ends)
        next_bt = gap['next_latest_bt']        # upper bound of the gap (where next session starts)
        next_sig = gap.get('next_latest_sig')

        gap_desc = f"boundary {bid}: {address[:16]}.. gap {time.strftime('%Y-%m-%d %H:%M', time.gmtime(gap_start))} → {time.strftime('%Y-%m-%d %H:%M', time.gmtime(next_bt)) if next_bt else 'NOW'}"

        if args.dry_run:
            print(f"  [{i+1}/{len(gaps)}] {gap_desc}")
            print(f"           before_bt={next_bt} until_bt={gap_start}")
            continue

        try:
            # Fetch the gap: from next session's start going backward, stop at gap start
            status, data = trigger_producer(session, address, next_sig, gap_start, args.max_sigs)

            if status == 200 and data.get('success'):
                sent += 1
                req_id = data.get('request_id', '?')[:12]
                print(f"  [{i+1}/{len(gaps)}] {gap_desc} -> SENT (req: {req_id}..)")
            else:
                errors += 1
                err = data.get('error', f'HTTP {status}')
                print(f"  [{i+1}/{len(gaps)}] {gap_desc} -> FAILED: {err}")

            time.sleep(args.delay)

        except Exception as e:
            errors += 1
            print(f"  [{i+1}/{len(gaps)}] {gap_desc} -> ERROR: {e}")

    print(f"\nDone. Sent: {sent}, Errors: {errors}, Total gaps: {len(gaps)}")

    cursor.close()
    conn.close()
    session.close()


if __name__ == '__main__':
    main()
