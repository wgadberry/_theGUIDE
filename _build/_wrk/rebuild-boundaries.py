#!/usr/bin/env python3
"""
Rebuild tx_boundary records for all mints with guide data.
For each mint: finds the oldest tx_guide sig, peeks 1 more via RPC, records boundary.
Does NOT re-fetch or reprocess — just rebuilds the boundary peek data.
"""

import json
import os
import sys
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from t16o_exchange.guide.common.config import get_db_config, get_rpc_config

DB_CONFIG = get_db_config()
RPC_URL = get_rpc_config()['url']

try:
    import mysql.connector
except ImportError:
    print("mysql-connector-python required")
    sys.exit(1)


def fetch_sig_before(session, address, before_sig):
    """Peek 1 sig older than before_sig for this address via RPC."""
    payload = {
        "id": 1, "jsonrpc": "2.0",
        "method": "getSignaturesForAddress",
        "params": [address, {"limit": 1, "before": before_sig}]
    }
    try:
        resp = session.post(RPC_URL, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        sigs = data.get('result', [])
        if sigs and sigs[0].get('err') is None:
            return sigs[0]['signature'], sigs[0].get('blockTime')
        elif sigs:
            # errored sig — still a valid boundary
            return sigs[0]['signature'], sigs[0].get('blockTime')
        return None, None
    except Exception as e:
        return None, None


def process_mint(mint_address, mint_address_id, oldest_sig, oldest_block_time, total_guide_txs):
    """Process one mint: peek and insert boundary."""
    session = requests.Session()
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        peek_sig, peek_bt = fetch_sig_before(session, mint_address, oldest_sig)

        if peek_sig:
            cursor.execute("""
                INSERT INTO tx_boundary
                (address_id, oldest_fetched_sig, oldest_fetched_block_time,
                 boundary_sig, boundary_block_time, fetched_count)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (mint_address_id, oldest_sig, oldest_block_time, peek_sig, peek_bt, total_guide_txs))
        else:
            # End of history
            cursor.execute("""
                INSERT INTO tx_boundary
                (address_id, oldest_fetched_sig, oldest_fetched_block_time,
                 boundary_sig, boundary_block_time, fetched_count)
                VALUES (%s, %s, %s, NULL, NULL, %s)
            """, (mint_address_id, oldest_sig, oldest_block_time, total_guide_txs))

        conn.commit()
        return mint_address, 'peek' if peek_sig else 'end', total_guide_txs
    except Exception as e:
        return mint_address, f'error: {e}', 0
    finally:
        cursor.close()
        conn.close()
        session.close()


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("Loading mints with guide data...")
    cursor.execute("""
        SELECT a.id, a.address,
            (SELECT t.signature FROM tx_guide g2 JOIN tx t ON t.id = g2.tx_id
             WHERE g2.token_id = tk.id ORDER BY t.block_time ASC LIMIT 1) as oldest_sig,
            (SELECT t.block_time FROM tx_guide g2 JOIN tx t ON t.id = g2.tx_id
             WHERE g2.token_id = tk.id ORDER BY t.block_time ASC LIMIT 1) as oldest_bt,
            COUNT(DISTINCT g.tx_id) as tx_cnt
        FROM tx_guide g
        JOIN tx_token tk ON tk.id = g.token_id
        JOIN tx_address a ON a.id = tk.mint_address_id
        GROUP BY a.id, a.address, tk.id
        HAVING oldest_sig IS NOT NULL
        ORDER BY tx_cnt DESC
    """)
    mints = cursor.fetchall()
    cursor.close()
    conn.close()

    print(f"Found {len(mints)} mints to process")

    threads = 10
    completed = 0
    peeked = 0
    ended = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {
            executor.submit(process_mint, addr, addr_id, oldest_sig, oldest_bt, tx_cnt): addr
            for addr_id, addr, oldest_sig, oldest_bt, tx_cnt in mints
        }

        for future in as_completed(futures):
            addr, status, cnt = future.result()
            completed += 1
            if status == 'peek':
                peeked += 1
            elif status == 'end':
                ended += 1
            else:
                errors += 1

            if completed % 100 == 0 or completed == len(mints):
                print(f"  [{completed}/{len(mints)}] peeked={peeked} end={ended} errors={errors}")

    print(f"\nDone: {peeked} boundaries, {ended} end-of-history, {errors} errors")


if __name__ == '__main__':
    main()
