#!/usr/bin/env python3
"""
Guide Market Loader - Fetch market/pool data from Solscan into tx_token_market

Queries tx_guide for distinct tokens, then fetches market data for each from
Solscan /token/markets endpoint.

Usage:
    python guide-market-loader.py [--limit 100]
    python guide-market-loader.py --daemon --interval 300  # Run continuously
    python guide-market-loader.py --dry-run
    python guide-market-loader.py --sample <token_mint>  # Test single token
"""

import argparse
import requests
import time
import sys
from typing import Optional, Dict, List

try:
    import mysql.connector
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

# =============================================================================
# Configuration
# =============================================================================

SOLSCAN_API_BASE = "https://pro-api.solscan.io/v2.0"
SOLSCAN_API_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjcmVhdGVkQXQiOjE3NTg0MzQzODI5MTEsImVtYWlsIjoid2dhZGJlcnJ5QGdtYWlsLmNvbSIsImFjdGlvbiI6InRva2VuLWFwaSIsImFwaVZlcnNpb24iOiJ2MiIsImlhdCI6MTc1ODQzNDM4Mn0.d_ZYyfZGaFdLfYXsuB_M2rT_Q6WkwpxVp0ZqNE_zUAk"

# Skip these common tokens (too many markets)
SKIP_TOKENS = {
    'So11111111111111111111111111111111111111112',  # Wrapped SOL
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',  # USDC
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',  # USDT
}


# =============================================================================
# API Functions
# =============================================================================

def create_api_session() -> requests.Session:
    """Create requests session with Solscan auth"""
    session = requests.Session()
    session.headers.update({"token": SOLSCAN_API_TOKEN})
    return session


def fetch_token_markets(session: requests.Session, token_mint: str, page_size: int = 20) -> Optional[Dict]:
    """Fetch markets for a specific token from Solscan"""
    url = f"{SOLSCAN_API_BASE}/token/markets"
    params = {
        "token[]": token_mint,
        "page": 1,
        "page_size": page_size,
    }

    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"    [!] API Error: {e}")
        return None


# =============================================================================
# Database Functions
# =============================================================================

def get_tokens_from_guide(cursor, limit: int = 100) -> List[str]:
    """Get distinct token mints from tx_guide that need market data"""
    cursor.execute("""
        SELECT a.address, MAX(g.id) as max_id
        FROM tx_guide g
        JOIN tx_token t ON t.id = g.token_id
        JOIN tx_address a ON a.id = t.mint_address_id
        WHERE a.address NOT IN ('So11111111111111111111111111111111111111112',
                                'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
                                'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB')
        GROUP BY a.address
        ORDER BY max_id DESC
        LIMIT %s
    """, (limit,))
    return [row[0] for row in cursor.fetchall()]


def ensure_address(cursor, conn, address: str, addr_type: str = 'unknown') -> Optional[int]:
    """Ensure address exists, return id"""
    if not address:
        return None

    cursor.execute("SELECT id FROM tx_address WHERE address = %s", (address,))
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute(
        "INSERT INTO tx_address (address, address_type) VALUES (%s, %s)",
        (address, addr_type)
    )
    conn.commit()
    return cursor.lastrowid


def lookup_token(cursor, mint_address: str) -> Optional[int]:
    """Lookup existing token by mint address. Returns None if not found (does NOT create)."""
    if not mint_address:
        return None

    cursor.execute("""
        SELECT tk.id FROM tx_token tk
        JOIN tx_address a ON a.id = tk.mint_address_id
        WHERE a.address = %s
    """, (mint_address,))
    row = cursor.fetchone()
    return row[0] if row else None


def ensure_pool(cursor, conn, pool_address: str, program_id: int = None) -> Optional[int]:
    """Ensure pool exists, return id"""
    if not pool_address:
        return None

    addr_id = ensure_address(cursor, conn, pool_address, 'pool')

    cursor.execute("SELECT id FROM tx_pool WHERE pool_address_id = %s", (addr_id,))
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute("""
        INSERT INTO tx_pool (pool_address_id, program_id)
        VALUES (%s, %s)
    """, (addr_id, program_id))
    conn.commit()
    return cursor.lastrowid


def ensure_program(cursor, conn, program_address: str) -> Optional[int]:
    """Ensure program exists, return id"""
    if not program_address:
        return None

    addr_id = ensure_address(cursor, conn, program_address, 'program')

    cursor.execute("SELECT id FROM tx_program WHERE program_address_id = %s", (addr_id,))
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute("""
        INSERT INTO tx_program (program_address_id, program_type)
        VALUES (%s, 'dex')
    """, (addr_id,))
    conn.commit()
    return cursor.lastrowid


def upsert_market(cursor, conn, pool_id: int, program_id: int,
                  token1_id: int, token2_id: int,
                  token_account1_id: int, token_account2_id: int,
                  tvl: float, volume_24h: float, volume_prev_24h: float,
                  trades_24h: int, trades_prev_24h: int, traders_24h: int) -> bool:
    """Insert or update market data"""
    try:
        cursor.execute("""
            INSERT INTO tx_token_market
                (pool_id, program_id, token1_id, token2_id,
                 token_account1_id, token_account2_id,
                 total_tvl, total_volume_24h, total_volume_prev_24h,
                 total_trades_24h, total_trades_prev_24h, num_traders_24h)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                program_id = VALUES(program_id),
                token1_id = VALUES(token1_id),
                token2_id = VALUES(token2_id),
                token_account1_id = VALUES(token_account1_id),
                token_account2_id = VALUES(token_account2_id),
                total_tvl = VALUES(total_tvl),
                total_volume_24h = VALUES(total_volume_24h),
                total_volume_prev_24h = VALUES(total_volume_prev_24h),
                total_trades_24h = VALUES(total_trades_24h),
                total_trades_prev_24h = VALUES(total_trades_prev_24h),
                num_traders_24h = VALUES(num_traders_24h)
        """, (pool_id, program_id, token1_id, token2_id,
              token_account1_id, token_account2_id,
              tvl, volume_24h, volume_prev_24h,
              trades_24h, trades_prev_24h, traders_24h))
        conn.commit()
        return True
    except Exception as e:
        print(f"    [!] DB Error: {e}")
        conn.rollback()
        return False


def process_market_item(cursor, conn, item: Dict) -> bool:
    """Process a single market item from API response"""
    try:
        # API returns: pool_id, program_id, token_1, token_2, token_account_1, token_account_2
        pool_address = item.get('pool_id')
        program_address = item.get('program_id')
        token1_mint = item.get('token_1')
        token2_mint = item.get('token_2')
        token1_account = item.get('token_account_1')
        token2_account = item.get('token_account_2')

        # Stats
        tvl = item.get('total_tvl') or 0
        volume_24h = item.get('total_volume_24h') or 0
        volume_prev_24h = item.get('total_volume_prev_24h') or 0
        trades_24h = item.get('total_trades_24h') or 0
        trades_prev_24h = item.get('total_trades_prev_24h') or 0
        traders_24h = item.get('num_trader_24h') or 0

        if not pool_address:
            return False

        # Ensure entities exist
        program_id = ensure_program(cursor, conn, program_address) if program_address else None
        pool_id = ensure_pool(cursor, conn, pool_address, program_id)
        # Only lookup existing tokens - don't create new ones for market pairs
        token1_id = lookup_token(cursor, token1_mint) if token1_mint else None
        token2_id = lookup_token(cursor, token2_mint) if token2_mint else None
        token_account1_id = ensure_address(cursor, conn, token1_account, 'ata') if token1_account else None
        token_account2_id = ensure_address(cursor, conn, token2_account, 'ata') if token2_account else None

        # Upsert market data
        return upsert_market(
            cursor, conn, pool_id, program_id,
            token1_id, token2_id, token_account1_id, token_account2_id,
            tvl, volume_24h, volume_prev_24h,
            trades_24h, trades_prev_24h, traders_24h
        )

    except Exception as e:
        print(f"    [!] Error processing item: {e}")
        return False


# =============================================================================
# Sync Logic
# =============================================================================

def sync_markets(cursor, conn, session, args) -> tuple:
    """Run one sync cycle. Returns (tokens_processed, markets_saved, errors)"""
    tokens = get_tokens_from_guide(cursor, args.limit)

    if not tokens:
        return 0, 0, 0

    total_tokens = 0
    total_markets = 0
    total_errors = 0

    for i, token_mint in enumerate(tokens):
        print(f"[{i+1}/{len(tokens)}] {token_mint[:20]}...", end='')

        data = fetch_token_markets(session, token_mint, args.markets_per_token)

        if not data or not data.get('success'):
            print(f" - API error")
            total_errors += 1
            time.sleep(args.delay)
            continue

        markets = data.get('data', [])
        if not markets:
            print(f" - no markets")
            time.sleep(args.delay)
            continue

        print(f" - {len(markets)} markets")
        total_tokens += 1

        for market in markets:
            if args.dry_run:
                pool = market.get('pool_id', 'unknown')
                vol = market.get('total_volume_24h', 0)
                print(f"    [DRY] Pool: {pool[:20]}... Vol24h: ${vol:,.0f}")
                total_markets += 1
            else:
                if process_market_item(cursor, conn, market):
                    total_markets += 1
                else:
                    total_errors += 1

        time.sleep(args.delay)

    return total_tokens, total_markets, total_errors


# =============================================================================
# Main
# =============================================================================

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
    parser = argparse.ArgumentParser(description='Load market data from Solscan for tokens in tx_guide')
    parser.add_argument('--limit', type=int, default=100, help='Max tokens to process per cycle')
    parser.add_argument('--markets-per-token', type=int, default=10, help='Max markets per token')
    parser.add_argument('--db-host', default='localhost', help='MySQL host')
    parser.add_argument('--db-port', type=int, default=3396, help='MySQL port')
    parser.add_argument('--db-user', default='root', help='MySQL user')
    parser.add_argument('--db-pass', default='rootpassword', help='MySQL password')
    parser.add_argument('--db-name', default='t16o_db', help='MySQL database')
    parser.add_argument('--delay', type=float, default=0.2, help='Delay between API calls')
    parser.add_argument('--daemon', action='store_true', help='Run continuously')
    parser.add_argument('--interval', type=int, default=300, help='Seconds between sync cycles (daemon mode)')
    parser.add_argument('--dry-run', action='store_true', help='Fetch but do not save')
    parser.add_argument('--sample', metavar='MINT', help='Test single token mint address')

    args = parser.parse_args()

    # Create API session
    session = create_api_session()

    # Sample mode - just show API response for one token
    if args.sample:
        print(f"Fetching markets for {args.sample}...\n")
        data = fetch_token_markets(session, args.sample, args.markets_per_token)
        if data:
            import json
            print(json.dumps(data, indent=2))
        else:
            print("Failed to fetch data")
        return 0

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        return 1

    print(f"Guide Market Loader")
    print(f"{'='*50}")
    print(f"Token limit: {args.limit}")
    print(f"Markets per token: {args.markets_per_token}")
    if args.daemon:
        print(f"MODE: DAEMON (interval: {args.interval}s)")
    if args.dry_run:
        print(f"MODE: DRY RUN")
    print()

    # Stats
    grand_tokens = 0
    grand_markets = 0
    grand_errors = 0
    cycles = 0
    conn = None
    cursor = None

    try:
        while True:
            cycles += 1
            from datetime import datetime

            # Reconnect to database each cycle (connections go stale during sleep)
            if conn:
                try:
                    cursor.close()
                    conn.close()
                except:
                    pass
            print(f"Connecting to MySQL {args.db_host}:{args.db_port}/{args.db_name}...")
            conn = create_db_connection(args)
            cursor = conn.cursor()

            print(f"\n{'='*50}")
            print(f"Cycle {cycles} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*50}")

            tokens, markets, errors = sync_markets(cursor, conn, session, args)
            grand_tokens += tokens
            grand_markets += markets
            grand_errors += errors

            print(f"\nCycle {cycles} complete: {tokens} tokens, {markets} markets, {errors} errors")

            if not args.daemon:
                break

            # Close connection before sleeping
            cursor.close()
            conn.close()
            conn = None

            print(f"Sleeping {args.interval}s until next cycle...")
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")

    finally:
        session.close()
        if conn:
            try:
                cursor.close()
                conn.close()
            except:
                pass

    print(f"\n{'='*50}")
    print(f"Done! Total across {cycles} cycle(s):")
    print(f"  Tokens processed: {grand_tokens}")
    print(f"  Markets saved: {grand_markets}")
    print(f"  Errors: {grand_errors}")
    print(f"{'='*50}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
