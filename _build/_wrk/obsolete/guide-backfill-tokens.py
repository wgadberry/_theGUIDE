#!/usr/bin/env python3
"""
Guide Backfill Tokens - Backfill missing token metadata from Solscan API

Fetches token info from /v2.0/token/meta for tokens missing:
- token_symbol
- token_name
- decimals

Enriches tx_token table.

Usage:
    python guide-backfill-tokens.py [--limit 100] [--delay 0.2]
    python guide-backfill-tokens.py --daemon --interval 60
"""

import argparse
import time
import requests

try:
    import mysql.connector
except ImportError:
    print("Error: mysql-connector-python not installed")
    exit(1)

# Solscan API config
SOLSCAN_API_BASE = "https://pro-api.solscan.io/v2.0"
SOLSCAN_API_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjcmVhdGVkQXQiOjE3NTg0MzQzODI5MTEsImVtYWlsIjoid2dhZGJlcnJ5QGdtYWlsLmNvbSIsImFjdGlvbiI6InRva2VuLWFwaSIsImFwaVZlcnNpb24iOiJ2MiIsImlhdCI6MTc1ODQzNDM4Mn0.d_ZYyfZGaFdLfYXsuB_M2rT_Q6WkwpxVp0ZqNE_zUAk"


def create_api_session() -> requests.Session:
    """Create a requests session with persistent connection and auth header"""
    session = requests.Session()
    session.headers.update({"token": SOLSCAN_API_TOKEN})
    return session


def fetch_token_meta(session: requests.Session, mint_address: str) -> dict:
    """Fetch token metadata from Solscan API"""
    url = f"{SOLSCAN_API_BASE}/token/meta"
    params = {"address": mint_address}

    response = session.get(url, params=params)
    response.raise_for_status()
    return response.json()


def get_tokens_missing_metadata(cursor, limit: int, max_attempts: int = 1) -> list:
    """Get tokens missing symbol, name, or decimals (excluding those with too many failed attempts)"""
    cursor.execute("""
        SELECT t.id, a.address as mint
        FROM tx_token t
        JOIN tx_address a ON a.id = t.mint_address_id
        WHERE (t.token_symbol IS NULL
           OR t.token_symbol = ''
           OR t.token_name IS NULL
           OR t.token_name = ''
           OR t.decimals IS NULL)
          AND COALESCE(t.attempt_cnt, 0) < %s
        LIMIT %s
    """, (max_attempts, limit))
    return cursor.fetchall()


def increment_attempt_count(cursor, conn, token_id: int):
    """Increment the attempt count for a token"""
    cursor.execute("""
        UPDATE tx_token
        SET attempt_cnt = COALESCE(attempt_cnt, 0) + 1
        WHERE id = %s
    """, (token_id,))
    conn.commit()


def update_token_metadata(cursor, conn, token_id: int, name: str, symbol: str,
                          icon: str, decimals: int) -> bool:
    """Update token with fetched metadata and reset attempt_cnt on success"""
    cursor.execute("""
        UPDATE tx_token
        SET token_name = COALESCE(%s, token_name),
            token_symbol = COALESCE(%s, token_symbol),
            token_icon = COALESCE(%s, token_icon),
            decimals = COALESCE(%s, decimals),
            attempt_cnt = 0
        WHERE id = %s
    """, (name, symbol, icon, decimals, token_id))
    conn.commit()
    return cursor.rowcount > 0


def create_db_connection(args):
    """Create a fresh database connection"""
    return mysql.connector.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_pass,
        database=args.db_name
    )


def main():
    parser = argparse.ArgumentParser(description='Backfill missing token metadata from Solscan')
    parser.add_argument('--limit', type=int, default=1000, help='Max tokens to process')
    parser.add_argument('--delay', type=float, default=0.2, help='Delay between API calls (seconds)')
    parser.add_argument('--max-attempts', type=int, default=1, help='Skip tokens with more than N failed attempts')
    parser.add_argument('--daemon', action='store_true', help='Run continuously as a daemon')
    parser.add_argument('--interval', type=int, default=60, help='Seconds between batches in daemon mode')
    parser.add_argument('--db-host', default='localhost', help='MySQL host')
    parser.add_argument('--db-port', type=int, default=3396, help='MySQL port')
    parser.add_argument('--db-user', default='root', help='MySQL user')
    parser.add_argument('--db-pass', default='rootpassword', help='MySQL password')
    parser.add_argument('--db-name', default='t16o_db', help='MySQL database')

    args = parser.parse_args()

    if args.daemon:
        print(f"Guide Backfill Tokens (Daemon Mode)")
        print(f"{'='*60}")
        print(f"Interval: {args.interval}s | Limit: {args.limit} | Max attempts: {args.max_attempts}")
        print(f"{'='*60}")

    # Create API session
    session = create_api_session()

    total_updated = 0
    total_failed = 0
    batch_num = 0
    conn = None
    cursor = None

    try:
        while True:
            batch_num += 1

            # Reconnect to database each batch (connections go stale during sleep)
            if conn:
                try:
                    cursor.close()
                    conn.close()
                except:
                    pass
            print(f"Connecting to MySQL {args.db_host}:{args.db_port}/{args.db_name}...")
            conn = create_db_connection(args)
            cursor = conn.cursor()

            # Get tokens needing metadata
            tokens = get_tokens_missing_metadata(cursor, args.limit, args.max_attempts)

            if not tokens:
                if args.daemon:
                    print(f"[Batch {batch_num}] No tokens to process, sleeping {args.interval}s...")
                    cursor.close()
                    conn.close()
                    conn = None
                    time.sleep(args.interval)
                    continue
                else:
                    print("Nothing to do!")
                    break

            if args.daemon:
                print(f"\n[Batch {batch_num}] Processing {len(tokens)} tokens...")

            else:
                print(f"Found {len(tokens)} tokens missing metadata (max_attempts={args.max_attempts})")

            updated = 0
            failed = 0

            for i, (token_id, mint) in enumerate(tokens, 1):
                print(f"  [{i}/{len(tokens)}] Fetching {mint[:16]}...", end=" ")

                try:
                    response = fetch_token_meta(session, mint)

                    if response.get('success') and response.get('data'):
                        data = response['data']
                        name = data.get('name')
                        symbol = data.get('symbol')
                        icon = data.get('icon')
                        decimals = data.get('decimals')

                        # Only update if we got actual metadata
                        if name or symbol or decimals is not None:
                            if update_token_metadata(cursor, conn, token_id, name, symbol, icon, decimals):
                                print(f"{symbol or 'unnamed'} ({decimals} dec)")
                                updated += 1
                            else:
                                print("no change")
                                increment_attempt_count(cursor, conn, token_id)
                                failed += 1
                        else:
                            print("empty data")
                            increment_attempt_count(cursor, conn, token_id)
                            failed += 1
                    else:
                        print("not found")
                        increment_attempt_count(cursor, conn, token_id)
                        failed += 1

                except requests.RequestException as e:
                    print(f"API error: {e}")
                    increment_attempt_count(cursor, conn, token_id)
                    failed += 1

                # Rate limiting
                if i < len(tokens) and args.delay > 0:
                    time.sleep(args.delay)

            total_updated += updated
            total_failed += failed

            if args.daemon:
                print(f"  [+] Batch complete: {updated} updated, {failed} failed")
                print(f"      Totals: {total_updated} updated, {total_failed} failed")
                print(f"      Sleeping {args.interval}s...")
                cursor.close()
                conn.close()
                conn = None
                time.sleep(args.interval)
            else:
                break

    except KeyboardInterrupt:
        print("\n\nShutting down...")

    finally:
        session.close()
        if conn:
            try:
                cursor.close()
                conn.close()
            except:
                pass

    print(f"\n{'='*60}")
    print(f"Done! Updated: {total_updated}, Failed: {total_failed}")
    print(f"{'='*60}")

    return 0


if __name__ == '__main__':
    exit(main())
