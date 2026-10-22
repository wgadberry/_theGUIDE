#!/usr/bin/env python3
"""
Guide Sync Funding - Sync daemon for funding analysis tables

Keeps tx_funding_edge and tx_token_participant tables up to date
as new records are added to tx_guide.

Pipeline position:
    guide-shredder.py → tx_guide
                          ↓
              guide-sync-funding.py (this script)
                          ↓
              tx_funding_edge, tx_token_participant

Tracks last processed tx_guide.id in config table for efficient incremental updates.

Configuration:
    Copy guide-config.json.example to guide-config.json and fill in your API keys.
    Alternatively, set environment variables:
    - SOLSCAN_API_TOKEN: Solscan Pro API JWT token (for --db-sync-unknown)
    - MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

Usage:
    python guide-sync-funding.py                       # Sync both tables
    python guide-sync-funding.py --table funding       # Only tx_funding_edge
    python guide-sync-funding.py --table participant   # Only tx_token_participant
    python guide-sync-funding.py --daemon --interval 60  # Run continuously
    python guide-sync-funding.py --db-sync-unknown     # Fetch funders for unknown addresses via Solscan
"""

import argparse
import sys
import time
import random
import os
import json
import functools
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Callable, List, Tuple

import requests
import mysql.connector
from mysql.connector import Error


# =============================================================================
# Config Loading
# =============================================================================

def load_config() -> dict:
    """
    Load configuration from JSON file with environment variable fallback.
    """
    config = {}
    config_paths = [
        Path('./guide-config.json'),
        Path(__file__).parent / 'guide-config.json',
    ]

    for config_path in config_paths:
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                break
            except Exception:
                pass

    # Environment variable fallback/override
    env_mappings = {
        'SOLSCAN_API': 'SOLSCAN_API_URL',
        'SOLSCAN_TOKEN': 'SOLSCAN_API_TOKEN',
        'DB_HOST': 'MYSQL_HOST',
        'DB_PORT': 'MYSQL_PORT',
        'DB_USER': 'MYSQL_USER',
        'DB_PASSWORD': 'MYSQL_PASSWORD',
        'DB_NAME': 'MYSQL_DATABASE',
    }

    for config_key, env_var in env_mappings.items():
        if os.environ.get(env_var):
            config[config_key] = os.environ[env_var]
            if config_key == 'DB_PORT':
                config[config_key] = int(config[config_key])

    return config


_CONFIG = load_config()


# =============================================================================
# Retry Logic
# =============================================================================

def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    retryable_exceptions: tuple = (requests.RequestException, ConnectionError, TimeoutError)
) -> Callable:
    """Decorator for retrying functions with exponential backoff."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        break
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    delay = delay * (1 + random.random() * 0.5)
                    print(f"    Retry {attempt + 1}/{max_retries} after {delay:.1f}s: {e}")
                    time.sleep(delay)
            raise last_exception
        return wrapper
    return decorator

# Deadlock retry settings
MAX_DEADLOCK_RETRIES = 5
DEADLOCK_BASE_DELAY = 0.1  # seconds

# Database configuration (from config file or env vars)
DB_CONFIG = {
    'host': _CONFIG.get('DB_HOST', 'localhost'),
    'port': _CONFIG.get('DB_PORT', 3396),
    'user': _CONFIG.get('DB_USER', 'root'),
    'password': _CONFIG.get('DB_PASSWORD', ''),
    'database': _CONFIG.get('DB_NAME', 't16o_db')
}

# Solscan API configuration
SOLSCAN_API = _CONFIG.get('SOLSCAN_API', 'https://pro-api.solscan.io/v2.0')
SOLSCAN_TOKEN = _CONFIG.get('SOLSCAN_TOKEN', '')
SOLSCAN_DELAY = _CONFIG.get('SOLSCAN_DELAY', 0.25)  # 4 req/sec

# Config keys
CONFIG_TYPE = 'sync'
FUNDING_EDGE_KEY = 'funding_edge_last_guide_id'
TOKEN_PARTICIPANT_KEY = 'token_participant_last_guide_id'


def get_db_connection():
    """Create database connection."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)


def get_last_processed_id(cursor, config_key: str) -> int:
    """Get last processed tx_guide.id from config table."""
    cursor.execute("""
        SELECT config_value FROM config
        WHERE config_type = %s AND config_key = %s
    """, (CONFIG_TYPE, config_key))

    row = cursor.fetchone()
    if row:
        return int(row[0])
    return 0


def set_last_processed_id(cursor, conn, config_key: str, last_id: int):
    """Update last processed tx_guide.id in config table."""
    cursor.execute("""
        INSERT INTO config (config_type, config_key, config_value, value_type, description)
        VALUES (%s, %s, %s, 'int', %s)
        ON DUPLICATE KEY UPDATE
            config_value = VALUES(config_value),
            updated_at = CURRENT_TIMESTAMP
    """, (CONFIG_TYPE, config_key, str(last_id), f'Last processed tx_guide.id for {config_key}'))
    conn.commit()


def get_max_guide_id(cursor) -> int:
    """Get current max tx_guide.id."""
    cursor.execute("SELECT MAX(id) FROM tx_guide")
    row = cursor.fetchone()
    return row[0] if row[0] else 0


def execute_with_deadlock_retry(cursor, conn, query: str, params: tuple, max_retries: int = MAX_DEADLOCK_RETRIES) -> int:
    """
    Execute a query with deadlock retry logic.
    Returns rowcount on success.
    """
    for attempt in range(max_retries):
        try:
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount
        except mysql.connector.Error as e:
            # Error 1213 = Deadlock found, Error 1205 = Lock wait timeout
            if e.errno in (1213, 1205) and attempt < max_retries - 1:
                conn.rollback()
                # Exponential backoff with jitter
                delay = DEADLOCK_BASE_DELAY * (2 ** attempt) + random.uniform(0, 0.1)
                time.sleep(delay)
            else:
                raise
    return 0


def sync_funding_edges(cursor, conn, last_id: int, max_id: int, batch_size: int = 10000) -> int:
    """
    Sync new records to tx_funding_edge in batches with deadlock retry.
    Returns number of rows affected.
    """
    if last_id >= max_id:
        return 0

    total_rows = 0
    current_id = last_id

    query = """
        INSERT INTO tx_funding_edge (
            from_address_id,
            to_address_id,
            total_sol,
            total_tokens,
            transfer_count,
            first_transfer_time,
            last_transfer_time
        )
        SELECT
            g.from_address_id,
            g.to_address_id,
            SUM(CASE WHEN gt.type_code IN ('sol_transfer', 'wallet_funded')
                THEN g.amount / POW(10, COALESCE(g.decimals, 9))
                ELSE 0 END) as total_sol,
            SUM(CASE WHEN gt.type_code = 'spl_transfer'
                THEN g.amount / POW(10, COALESCE(g.decimals, 9))
                ELSE 0 END) as total_tokens,
            COUNT(*) as transfer_count,
            MIN(g.block_time) as first_transfer_time,
            MAX(g.block_time) as last_transfer_time
        FROM tx_guide g
        JOIN tx_address fa ON fa.id = g.from_address_id
        JOIN tx_address ta ON ta.id = g.to_address_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE g.id > %s AND g.id <= %s
          AND gt.type_code IN ('sol_transfer', 'spl_transfer', 'wallet_funded')
          AND fa.address_type IN ('wallet', 'unknown')
          AND ta.address_type IN ('wallet', 'unknown')
          AND fa.id != ta.id
        GROUP BY g.from_address_id, g.to_address_id
        ON DUPLICATE KEY UPDATE
            total_sol = total_sol + VALUES(total_sol),
            total_tokens = total_tokens + VALUES(total_tokens),
            transfer_count = transfer_count + VALUES(transfer_count),
            first_transfer_time = LEAST(first_transfer_time, VALUES(first_transfer_time)),
            last_transfer_time = GREATEST(last_transfer_time, VALUES(last_transfer_time))
    """

    while current_id < max_id:
        batch_end = min(current_id + batch_size, max_id)
        rows = execute_with_deadlock_retry(cursor, conn, query, (current_id, batch_end))
        total_rows += rows
        current_id = batch_end

    return total_rows


def sync_token_participants(cursor, conn, last_id: int, max_id: int, batch_size: int = 10000) -> int:
    """
    Sync new records to tx_token_participant in batches with deadlock retry.
    Returns number of rows affected.
    """
    if last_id >= max_id:
        return 0

    total_rows = 0
    current_id = last_id

    # Buys (swap_in - wallet receives tokens)
    query_buys = """
        INSERT INTO tx_token_participant (
            token_id, address_id, first_seen, last_seen,
            buy_count, buy_volume, net_position
        )
        SELECT
            g.token_id,
            g.to_address_id as address_id,
            MIN(g.block_time) as first_seen,
            MAX(g.block_time) as last_seen,
            COUNT(*) as buy_count,
            SUM(g.amount / POW(10, COALESCE(g.decimals, 9))) as buy_volume,
            SUM(g.amount / POW(10, COALESCE(g.decimals, 9))) as net_position
        FROM tx_guide g
        JOIN tx_address a ON a.id = g.to_address_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE g.id > %s AND g.id <= %s
          AND gt.type_code = 'swap_in'
          AND g.token_id IS NOT NULL
          AND a.address_type IN ('wallet', 'unknown')
        GROUP BY g.token_id, g.to_address_id
        ON DUPLICATE KEY UPDATE
            first_seen = LEAST(first_seen, VALUES(first_seen)),
            last_seen = GREATEST(last_seen, VALUES(last_seen)),
            buy_count = buy_count + VALUES(buy_count),
            buy_volume = buy_volume + VALUES(buy_volume),
            net_position = net_position + VALUES(buy_volume)
    """

    # Sells (swap_out - wallet sends tokens)
    query_sells = """
        INSERT INTO tx_token_participant (
            token_id, address_id, first_seen, last_seen,
            sell_count, sell_volume, net_position
        )
        SELECT
            g.token_id,
            g.from_address_id as address_id,
            MIN(g.block_time) as first_seen,
            MAX(g.block_time) as last_seen,
            COUNT(*) as sell_count,
            SUM(g.amount / POW(10, COALESCE(g.decimals, 9))) as sell_volume,
            -SUM(g.amount / POW(10, COALESCE(g.decimals, 9))) as net_position
        FROM tx_guide g
        JOIN tx_address a ON a.id = g.from_address_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE g.id > %s AND g.id <= %s
          AND gt.type_code = 'swap_out'
          AND g.token_id IS NOT NULL
          AND a.address_type IN ('wallet', 'unknown')
        GROUP BY g.token_id, g.from_address_id
        ON DUPLICATE KEY UPDATE
            first_seen = LEAST(first_seen, VALUES(first_seen)),
            last_seen = GREATEST(last_seen, VALUES(last_seen)),
            sell_count = sell_count + VALUES(sell_count),
            sell_volume = sell_volume + VALUES(sell_volume),
            net_position = net_position - VALUES(sell_volume)
    """

    # Transfers in (spl_transfer to wallet)
    query_xfer_in = """
        INSERT INTO tx_token_participant (
            token_id, address_id, first_seen, last_seen, transfer_in_count
        )
        SELECT
            g.token_id,
            g.to_address_id as address_id,
            MIN(g.block_time) as first_seen,
            MAX(g.block_time) as last_seen,
            COUNT(*) as transfer_in_count
        FROM tx_guide g
        JOIN tx_address a ON a.id = g.to_address_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE g.id > %s AND g.id <= %s
          AND gt.type_code = 'spl_transfer'
          AND g.token_id IS NOT NULL
          AND a.address_type IN ('wallet', 'unknown')
        GROUP BY g.token_id, g.to_address_id
        ON DUPLICATE KEY UPDATE
            first_seen = LEAST(first_seen, VALUES(first_seen)),
            last_seen = GREATEST(last_seen, VALUES(last_seen)),
            transfer_in_count = transfer_in_count + VALUES(transfer_in_count)
    """

    # Transfers out (spl_transfer from wallet)
    query_xfer_out = """
        INSERT INTO tx_token_participant (
            token_id, address_id, first_seen, last_seen, transfer_out_count
        )
        SELECT
            g.token_id,
            g.from_address_id as address_id,
            MIN(g.block_time) as first_seen,
            MAX(g.block_time) as last_seen,
            COUNT(*) as transfer_out_count
        FROM tx_guide g
        JOIN tx_address a ON a.id = g.from_address_id
        JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE g.id > %s AND g.id <= %s
          AND gt.type_code = 'spl_transfer'
          AND g.token_id IS NOT NULL
          AND a.address_type IN ('wallet', 'unknown')
        GROUP BY g.token_id, g.from_address_id
        ON DUPLICATE KEY UPDATE
            first_seen = LEAST(first_seen, VALUES(first_seen)),
            last_seen = GREATEST(last_seen, VALUES(last_seen)),
            transfer_out_count = transfer_out_count + VALUES(transfer_out_count)
    """

    while current_id < max_id:
        batch_end = min(current_id + batch_size, max_id)
        params = (current_id, batch_end)

        total_rows += execute_with_deadlock_retry(cursor, conn, query_buys, params)
        total_rows += execute_with_deadlock_retry(cursor, conn, query_sells, params)
        total_rows += execute_with_deadlock_retry(cursor, conn, query_xfer_in, params)
        total_rows += execute_with_deadlock_retry(cursor, conn, query_xfer_out, params)

        current_id = batch_end

    return total_rows


def backfill_funding_edges(cursor, conn) -> int:
    """
    Backfill tx_funding_edge records that have missing data.
    Updates edges where total_sol=0 or total_tokens=0 but tx_guide has data.
    Returns count of updated rows.
    """
    total_updated = 0

    # Backfill SOL transfers
    query_sol = """
        UPDATE tx_funding_edge fe
        JOIN (
            SELECT
                g.from_address_id,
                g.to_address_id,
                SUM(g.amount / POW(10, COALESCE(g.decimals, 9))) as sol_total,
                COUNT(*) as transfer_count,
                MIN(g.block_time) as first_time,
                MAX(g.block_time) as last_time
            FROM tx_guide g
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            WHERE gt.type_code IN ('sol_transfer', 'wallet_funded')
            GROUP BY g.from_address_id, g.to_address_id
        ) agg ON agg.from_address_id = fe.from_address_id
             AND agg.to_address_id = fe.to_address_id
        SET
            fe.total_sol = agg.sol_total,
            fe.transfer_count = fe.transfer_count + agg.transfer_count,
            fe.first_transfer_time = LEAST(COALESCE(fe.first_transfer_time, agg.first_time), agg.first_time),
            fe.last_transfer_time = GREATEST(COALESCE(fe.last_transfer_time, agg.last_time), agg.last_time)
        WHERE COALESCE(fe.total_sol, 0) = 0
    """
    cursor.execute(query_sol)
    total_updated += cursor.rowcount
    conn.commit()

    # Backfill token transfers
    query_tokens = """
        UPDATE tx_funding_edge fe
        JOIN (
            SELECT
                g.from_address_id,
                g.to_address_id,
                SUM(g.amount / POW(10, COALESCE(g.decimals, 9))) as token_total,
                COUNT(*) as transfer_count,
                MIN(g.block_time) as first_time,
                MAX(g.block_time) as last_time
            FROM tx_guide g
            JOIN tx_guide_type gt ON gt.id = g.edge_type_id
            WHERE gt.type_code = 'spl_transfer'
            GROUP BY g.from_address_id, g.to_address_id
        ) agg ON agg.from_address_id = fe.from_address_id
             AND agg.to_address_id = fe.to_address_id
        SET
            fe.total_tokens = agg.token_total,
            fe.transfer_count = fe.transfer_count + agg.transfer_count,
            fe.first_transfer_time = LEAST(COALESCE(fe.first_transfer_time, agg.first_time), agg.first_time),
            fe.last_transfer_time = GREATEST(COALESCE(fe.last_transfer_time, agg.last_time), agg.last_time)
        WHERE COALESCE(fe.total_tokens, 0) = 0
    """
    cursor.execute(query_tokens)
    total_updated += cursor.rowcount
    conn.commit()

    return total_updated


# =============================================================================
# Solscan API Functions for Unknown Funder Sync
# =============================================================================

def _is_retryable_error(e: Exception) -> bool:
    """Check if an HTTP error should be retried (only 5xx server errors)."""
    if isinstance(e, requests.exceptions.HTTPError):
        if e.response is not None:
            # Only retry on 5xx server errors, not 4xx client errors
            return e.response.status_code >= 500
    return True  # Retry connection errors, timeouts, etc.


@retry_with_backoff(max_retries=3, base_delay=1.0, max_delay=30.0)
def solscan_get_first_transfer(session: requests.Session, address: str) -> Optional[Dict]:
    """
    Get the first SOL transfer to this address (the funder) via Solscan API.
    Returns dict with funder address and transaction info, or None if not found.
    """
    if not SOLSCAN_TOKEN:
        raise ValueError("SOLSCAN_TOKEN not configured. Set in guide-config.json or SOLSCAN_API_TOKEN env var.")

    url = f"{SOLSCAN_API}/account/transfer"
    headers = {"token": SOLSCAN_TOKEN}
    params = {
        "address": address,
        "activity_type": "ACTIVITY_SPL_TRANSFER",  # Will also catch SOL
        "page": 1,
        "page_size": 1,
        "sort_by": "block_time",
        "sort_order": "asc"  # Oldest first
    }

    try:
        response = session.get(url, headers=headers, params=params, timeout=30)

        # Don't retry on 4xx client errors (400, 404, etc.)
        if response.status_code >= 400 and response.status_code < 500:
            return None

        response.raise_for_status()
        result = response.json()

        if not result.get('success') or not result.get('data'):
            return None

        transfers = result['data']
        if not transfers:
            return None

        # Find first incoming transfer (where this address is the recipient)
        for xfer in transfers:
            to_addr = xfer.get('to_address')
            from_addr = xfer.get('from_address')

            if to_addr == address and from_addr and from_addr != address:
                return {
                    'funder_address': from_addr,
                    'tx_hash': xfer.get('trans_id'),
                    'block_time': xfer.get('block_time'),
                    'amount': xfer.get('amount'),
                    'token': xfer.get('token_address', 'SOL')
                }

        return None

    except requests.exceptions.HTTPError as e:
        # Don't retry 4xx errors
        if e.response is not None and e.response.status_code < 500:
            return None
        raise
    except ValueError:
        return None


@retry_with_backoff(max_retries=3, base_delay=1.0, max_delay=30.0)
def solscan_get_sol_transfers(session: requests.Session, address: str) -> Optional[Dict]:
    """
    Get first SOL transfer to this address via Solscan sol-transfer endpoint.
    More reliable for finding the initial funding transaction.
    """
    if not SOLSCAN_TOKEN:
        raise ValueError("SOLSCAN_TOKEN not configured.")

    url = f"{SOLSCAN_API}/account/transfer"
    headers = {"token": SOLSCAN_TOKEN}
    params = {
        "address": address,
        "activity_type": "ACTIVITY_SOL_TRANSFER",
        "flow": "in",  # Only incoming
        "page": 1,
        "page_size": 1,
        "sort_by": "block_time",
        "sort_order": "asc"
    }

    try:
        response = session.get(url, headers=headers, params=params, timeout=30)

        # Don't retry on 4xx client errors (400, 404, etc.)
        if response.status_code >= 400 and response.status_code < 500:
            return None

        response.raise_for_status()
        result = response.json()

        if not result.get('success') or not result.get('data'):
            return None

        transfers = result['data']
        if not transfers:
            return None

        xfer = transfers[0]
        from_addr = xfer.get('from_address')

        if from_addr and from_addr != address:
            return {
                'funder_address': from_addr,
                'tx_hash': xfer.get('trans_id'),
                'block_time': xfer.get('block_time'),
                'amount': xfer.get('amount'),
                'token': 'SOL'
            }

        return None

    except requests.exceptions.HTTPError as e:
        # Don't retry 4xx errors
        if e.response is not None and e.response.status_code < 500:
            return None
        raise
    except ValueError:
        return None


def sync_unknown_funders(
    cursor,
    conn,
    limit: int = 1000,
    dry_run: bool = False,
    verbose: bool = True
) -> Dict[str, int]:
    """
    Scan tx_address for addresses with unknown funders and attempt to
    discover their funder via Solscan API.

    Targets addresses where:
    - funded_by_address_id IS NULL
    - init_tx_fetched = 0 (not yet attempted)
    - address_type IN ('wallet', 'unknown')

    Returns stats dict with counts.
    """
    stats = {
        'scanned': 0,
        'found': 0,
        'not_found': 0,
        'errors': 0,
        'skipped': 0
    }

    if not SOLSCAN_TOKEN:
        print("ERROR: SOLSCAN_TOKEN not configured. Set in guide-config.json or SOLSCAN_API_TOKEN env var.")
        return stats

    print(f"\n{'='*70}")
    print(f"SYNC UNKNOWN FUNDERS via Solscan API")
    print(f"{'='*70}")
    print(f"Dry run: {dry_run}")
    print(f"Limit: {limit}")
    print()

    # Query for addresses needing funder lookup
    query = """
        SELECT id, address
        FROM tx_address
        WHERE funded_by_address_id IS NULL
          AND init_tx_fetched = 0
          AND address_type IN ('wallet', 'unknown')
        ORDER BY id ASC
        LIMIT %s
    """
    cursor.execute(query, (limit,))
    addresses = cursor.fetchall()

    if not addresses:
        print("No addresses found needing funder lookup.")
        return stats

    print(f"Found {len(addresses)} addresses to process...")

    session = requests.Session()

    for i, (addr_id, address) in enumerate(addresses):
        stats['scanned'] += 1

        if verbose and (i + 1) % 50 == 0:
            print(f"  Progress: {i + 1}/{len(addresses)} "
                  f"(found: {stats['found']}, not_found: {stats['not_found']}, errors: {stats['errors']})")

        try:
            # Mark as in-progress (init_tx_fetched = 2) to prevent duplicate work
            if not dry_run:
                cursor.execute(
                    "UPDATE tx_address SET init_tx_fetched = 2 WHERE id = %s AND init_tx_fetched = 0",
                    (addr_id,)
                )
                conn.commit()
                if cursor.rowcount == 0:
                    stats['skipped'] += 1
                    continue

            # Try SOL transfers only (most common funding method)
            funder_info = solscan_get_sol_transfers(session, address)

            if funder_info:
                funder_address = funder_info['funder_address']

                if dry_run:
                    print(f"  [DRY] {address[:16]}... <- {funder_address[:16]}...")
                    stats['found'] += 1
                else:
                    # Ensure funder address exists in tx_address
                    cursor.execute(
                        "SELECT id FROM tx_address WHERE address = %s",
                        (funder_address,)
                    )
                    row = cursor.fetchone()

                    if row:
                        funder_id = row[0]
                    else:
                        cursor.execute(
                            "INSERT INTO tx_address (address, address_type) VALUES (%s, 'unknown')",
                            (funder_address,)
                        )
                        conn.commit()
                        funder_id = cursor.lastrowid

                    # Update the target address with funder info
                    cursor.execute("""
                        UPDATE tx_address
                        SET funded_by_address_id = %s,
                            init_tx_fetched = 1,
                            init_tx_hash = %s
                        WHERE id = %s
                    """, (funder_id, funder_info.get('tx_hash'), addr_id))
                    conn.commit()

                    stats['found'] += 1

                    if verbose:
                        print(f"  [{i+1}] {address[:20]}... <- {funder_address[:20]}...")

            else:
                # No funder found - mark as attempted
                if not dry_run:
                    cursor.execute(
                        "UPDATE tx_address SET init_tx_fetched = 1 WHERE id = %s",
                        (addr_id,)
                    )
                    conn.commit()

                stats['not_found'] += 1

            time.sleep(SOLSCAN_DELAY)

        except Exception as e:
            stats['errors'] += 1
            if verbose:
                print(f"  ERROR for {address[:20]}...: {e}")

            # Reset to allow retry later
            if not dry_run:
                cursor.execute(
                    "UPDATE tx_address SET init_tx_fetched = 0 WHERE id = %s",
                    (addr_id,)
                )
                conn.commit()

            time.sleep(SOLSCAN_DELAY * 2)  # Extra delay on error

    # Final summary
    print(f"\n{'='*70}")
    print(f"SYNC COMPLETE")
    print(f"{'='*70}")
    print(f"  Scanned:   {stats['scanned']:,}")
    print(f"  Found:     {stats['found']:,}")
    print(f"  Not found: {stats['not_found']:,}")
    print(f"  Errors:    {stats['errors']:,}")
    print(f"  Skipped:   {stats['skipped']:,}")

    return stats


def run_sync(cursor, conn, table: str = 'both', batch_size: int = 10000, verbose: bool = True) -> dict:
    """
    Run sync for specified table(s).
    Returns dict with sync statistics.
    """
    stats = {
        'max_guide_id': 0,
        'funding_edge': {'last_id': 0, 'new_id': 0, 'rows': 0},
        'token_participant': {'last_id': 0, 'new_id': 0, 'rows': 0}
    }

    max_id = get_max_guide_id(cursor)
    stats['max_guide_id'] = max_id

    if table in ('funding', 'both'):
        last_id = get_last_processed_id(cursor, FUNDING_EDGE_KEY)
        stats['funding_edge']['last_id'] = last_id

        if last_id < max_id:
            if verbose:
                print(f"  funding_edge: syncing {last_id:,} -> {max_id:,} ({max_id - last_id:,} new records, batch={batch_size:,})")

            rows = sync_funding_edges(cursor, conn, last_id, max_id, batch_size)
            set_last_processed_id(cursor, conn, FUNDING_EDGE_KEY, max_id)

            stats['funding_edge']['new_id'] = max_id
            stats['funding_edge']['rows'] = rows

            if verbose:
                print(f"  funding_edge: {rows:,} rows affected")
        else:
            if verbose:
                print(f"  funding_edge: already up to date (id={last_id:,})")

    if table in ('participant', 'both'):
        last_id = get_last_processed_id(cursor, TOKEN_PARTICIPANT_KEY)
        stats['token_participant']['last_id'] = last_id

        if last_id < max_id:
            if verbose:
                print(f"  token_participant: syncing {last_id:,} -> {max_id:,} ({max_id - last_id:,} new records, batch={batch_size:,})")

            rows = sync_token_participants(cursor, conn, last_id, max_id, batch_size)
            set_last_processed_id(cursor, conn, TOKEN_PARTICIPANT_KEY, max_id)

            stats['token_participant']['new_id'] = max_id
            stats['token_participant']['rows'] = rows

            if verbose:
                print(f"  token_participant: {rows:,} rows affected")
        else:
            if verbose:
                print(f"  token_participant: already up to date (id={last_id:,})")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Sync tx_funding_edge and tx_token_participant tables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python guide-sync-funding.py                         # Sync both tables once
  python guide-sync-funding.py --table funding         # Only tx_funding_edge
  python guide-sync-funding.py --daemon --interval 60  # Run every 60 seconds
  python guide-sync-funding.py --status                # Show current sync status
  python guide-sync-funding.py --db-sync-unknown       # Fetch funders via Solscan
  python guide-sync-funding.py --db-sync-unknown --limit 5000  # Process 5000 addresses
  python guide-sync-funding.py --db-sync-unknown --dry-run     # Preview without DB changes
        """
    )

    parser.add_argument('--table', '-t', choices=['funding', 'participant', 'both'],
                        default='both', help='Which table(s) to sync')
    parser.add_argument('--batch-size', '-b', type=int, default=10000,
                        help='Batch size for inserts to prevent deadlocks (default: 10000)')
    parser.add_argument('--daemon', '-d', action='store_true',
                        help='Run continuously in daemon mode')
    parser.add_argument('--interval', '-i', type=int, default=60,
                        help='Sync interval in seconds for daemon mode (default: 60)')
    parser.add_argument('--status', '-s', action='store_true',
                        help='Show current sync status and exit')
    parser.add_argument('--quiet', '-q', action='store_true',
                        help='Minimal output')
    parser.add_argument('--backfill', action='store_true',
                        help='Backfill edges with missing total_sol or total_tokens from tx_guide')
    parser.add_argument('--db-sync-unknown', action='store_true',
                        help='Fetch funders for unknown addresses via Solscan API')
    parser.add_argument('--limit', '-l', type=int, default=1000,
                        help='Limit addresses to process for --db-sync-unknown (default: 1000)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without updating database (for --db-sync-unknown)')

    args = parser.parse_args()

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if args.status:
            # Show status
            max_id = get_max_guide_id(cursor)
            funding_last = get_last_processed_id(cursor, FUNDING_EDGE_KEY)
            participant_last = get_last_processed_id(cursor, TOKEN_PARTICIPANT_KEY)

            print(f"Sync Status:")
            print(f"  tx_guide max id: {max_id:,}")
            print(f"  funding_edge last synced: {funding_last:,} (behind: {max_id - funding_last:,})")
            print(f"  token_participant last synced: {participant_last:,} (behind: {max_id - participant_last:,})")
            return

        if args.backfill:
            # Backfill edges with missing data from tx_guide
            print("Backfilling funding edges with missing data...")
            count = backfill_funding_edges(cursor, conn)
            print(f"  Updated {count:,} funding edge records")
            return

        if args.db_sync_unknown:
            # Sync unknown funders via Solscan API
            stats = sync_unknown_funders(
                cursor, conn,
                limit=args.limit,
                dry_run=args.dry_run,
                verbose=not args.quiet
            )
            return

        if args.daemon:
            print(f"Starting sync daemon (interval: {args.interval}s)")
            print("Press Ctrl+C to stop\n")

            while True:
                try:
                    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                    if not args.quiet:
                        print(f"[{timestamp}] Running sync...")

                    stats = run_sync(cursor, conn, args.table, args.batch_size, verbose=not args.quiet)

                    if not args.quiet:
                        total_rows = stats['funding_edge']['rows'] + stats['token_participant']['rows']
                        if total_rows > 0:
                            print(f"[{timestamp}] Sync complete: {total_rows:,} total rows affected\n")
                        else:
                            print(f"[{timestamp}] No new records\n")

                    # Close connection before sleeping (connections go stale)
                    try:
                        cursor.close()
                        conn.close()
                    except:
                        pass

                    time.sleep(args.interval)

                    # Reconnect after sleep
                    conn = get_db_connection()
                    cursor = conn.cursor()

                except KeyboardInterrupt:
                    print("\nStopping sync daemon...")
                    break
                except Exception as e:
                    print(f"Error during sync: {e}")
                    # Reconnect on error
                    try:
                        cursor.close()
                        conn.close()
                    except:
                        pass
                    time.sleep(args.interval)
                    conn = get_db_connection()
                    cursor = conn.cursor()
        else:
            # Single run
            if not args.quiet:
                print(f"Syncing funding tables...")

            stats = run_sync(cursor, conn, args.table, args.batch_size, verbose=not args.quiet)

            if not args.quiet:
                total_rows = stats['funding_edge']['rows'] + stats['token_participant']['rows']
                print(f"\nSync complete: {total_rows:,} total rows affected")

    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
