#!/usr/bin/env python3
"""
Guide Enricher - Consolidated enrichment worker for token, pool, and program metadata

Merges functionality from:
- guide-backfill-tokens.py: Token metadata from /v2.0/token/meta
- guide-pool-enricher.py: Pool info from /v2.0/market/info and /v2.0/account/metadata

Enriches:
- tx_token: name, symbol, icon, decimals
- tx_pool: program, tokens, token accounts, LP token, label
- tx_program: name, program_type (via /v2.0/account/metadata)

Usage:
    python guide-enricher.py                           # All enrichment, single run
    python guide-enricher.py --daemon --interval 60   # Continuous mode
    python guide-enricher.py --enrich tokens          # Only token metadata
    python guide-enricher.py --enrich pools           # Only pool data
    python guide-enricher.py --enrich programs        # Only program metadata
    python guide-enricher.py --enrich tokens,pools,programs  # All (default)
    python guide-enricher.py --pool <address>         # Enrich single pool
    python guide-enricher.py --status                 # Show enrichment status
"""

import argparse
import sys
import time
import json
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    import mysql.connector
    from mysql.connector import Error
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

try:
    import pika
    HAS_PIKA = True
except ImportError:
    HAS_PIKA = False


# =============================================================================
# Configuration
# =============================================================================

def load_config() -> dict:
    """Load configuration from JSON file with environment variable fallback."""
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

    env_mappings = {
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

DB_CONFIG = {
    'host': _CONFIG.get('DB_HOST', '127.0.0.1'),
    'port': _CONFIG.get('DB_PORT', 3396),
    'user': _CONFIG.get('DB_USER', 'root'),
    'password': _CONFIG.get('DB_PASSWORD', 'rootpassword'),
    'database': _CONFIG.get('DB_NAME', 't16o_db'),
    'autocommit': True,  # Prevent table locks when idle
}

# RabbitMQ (gateway - t16o_mq vhost)
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5692
RABBITMQ_USER = 'admin'
RABBITMQ_PASS = 'admin123'
RABBITMQ_VHOST = 't16o_mq'
RABBITMQ_REQUEST_QUEUE = 'mq.guide.enricher.request'
RABBITMQ_RESPONSE_QUEUE = 'mq.guide.enricher.response'

SOLSCAN_API_BASE = "https://pro-api.solscan.io/v2.0"
SOLSCAN_API_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjcmVhdGVkQXQiOjE3NTg0MzQzODI5MTEsImVtYWlsIjoid2dhZGJlcnJ5QGdtYWlsLmNvbSIsImFjdGlvbiI6InRva2VuLWFwaSIsImFwaVZlcnNpb24iOiJ2MiIsImlhdCI6MTc1ODQzNDM4Mn0.d_ZYyfZGaFdLfYXsuB_M2rT_Q6WkwpxVp0ZqNE_zUAk"


# =============================================================================
# Database Utilities
# =============================================================================

def get_db_connection():
    """Create database connection."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None


# =============================================================================
# Solscan API Functions
# =============================================================================

def create_api_session() -> requests.Session:
    """Create requests session with Solscan auth header"""
    session = requests.Session()
    session.headers.update({"token": SOLSCAN_API_TOKEN})
    return session


def fetch_token_meta(session: requests.Session, mint_address: str) -> Optional[Dict]:
    """Fetch token metadata from Solscan /v2.0/token/meta (single)"""
    url = f"{SOLSCAN_API_BASE}/token/meta"
    params = {"address": mint_address}

    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if data.get('success') and data.get('data'):
            return data['data']
        return None
    except requests.RequestException as e:
        print(f"    [!] API Error: {e}")
        return None


def fetch_token_meta_multi(session: requests.Session, mint_addresses: List[str]) -> Dict[str, Dict]:
    """Fetch token metadata for multiple addresses from Solscan /v2.0/token/meta/multi.
    Returns dict keyed by address."""
    url = f"{SOLSCAN_API_BASE}/token/meta/multi"
    params = [('address[]', addr) for addr in mint_addresses]

    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if data.get('success') and data.get('data'):
            return {item['address']: item for item in data['data'] if 'address' in item}
        return {}
    except requests.RequestException as e:
        print(f"    [!] Multi-meta API Error: {e}")
        return {}


def fetch_pool_info(session: requests.Session, pool_address: str) -> Optional[Dict]:
    """Fetch pool info from Solscan /v2.0/market/info"""
    url = f"{SOLSCAN_API_BASE}/market/info"
    params = {"address": pool_address}

    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if data.get('success') and data.get('data'):
            return data['data']
        return None
    except requests.RequestException as e:
        print(f"    [!] API Error: {e}")
        return None


def fetch_account_metadata(session: requests.Session, address: str) -> Optional[Dict]:
    """Fetch account metadata from Solscan /v2.0/account/metadata (for labels)"""
    url = f"{SOLSCAN_API_BASE}/account/metadata"
    params = {"address": address}

    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if data.get('success') and data.get('data'):
            return data['data']
        return None
    except requests.RequestException as e:
        print(f"    [!] Metadata API Error: {e}")
        return None


def fetch_account_metadata_multi(session: requests.Session, addresses: List[str]) -> Dict[str, Dict]:
    """Fetch account metadata for multiple addresses from Solscan /v2.0/account/metadata/multi.
    Returns dict keyed by account_address."""
    url = f"{SOLSCAN_API_BASE}/account/metadata/multi"
    params = [('address[]', addr) for addr in addresses]

    try:
        response = session.get(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        if data.get('success') and data.get('data'):
            return {item['account_address']: item for item in data['data'] if 'account_address' in item}
        return {}
    except requests.RequestException as e:
        print(f"    [!] account/metadata/multi API Error: {e}")
        return {}


# =============================================================================
# Token Enrichment (from guide-backfill-tokens.py)
# =============================================================================

def get_tokens_needing_metadata(cursor, limit: int, max_attempts: int = 3) -> List[Dict]:
    """Get tokens missing symbol, name, or decimals"""
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
    return [{'id': row[0], 'mint': row[1]} for row in cursor.fetchall()]


def update_token_metadata(cursor, conn, token_id: int, name: str, symbol: str,
                          icon: str, decimals: int, token_json: dict = None) -> bool:
    """Update token with fetched metadata.

    Only resets attempt_cnt if we got meaningful data (non-empty name or symbol).
    Otherwise increments attempt_cnt to prevent infinite retry loops.

    Args:
        token_json: Complete API response to store for reference
    """
    # Check if we got meaningful metadata (not just decimals)
    has_meaningful_data = bool(name and name.strip()) or bool(symbol and symbol.strip())

    # Serialize JSON if provided
    token_json_str = json.dumps(token_json) if token_json else None

    if has_meaningful_data:
        # Got real data - update and reset attempt counter
        cursor.execute("""
            UPDATE tx_token
            SET token_name = COALESCE(%s, token_name),
                token_symbol = COALESCE(%s, token_symbol),
                token_icon = COALESCE(%s, token_icon),
                decimals = COALESCE(%s, decimals),
                token_json = COALESCE(%s, token_json),
                attempt_cnt = 0
            WHERE id = %s
        """, (name, symbol, icon, decimals, token_json_str, token_id))
    else:
        # No meaningful data - update what we have but increment attempt counter
        cursor.execute("""
            UPDATE tx_token
            SET token_name = COALESCE(%s, token_name),
                token_symbol = COALESCE(%s, token_symbol),
                token_icon = COALESCE(%s, token_icon),
                decimals = COALESCE(%s, decimals),
                token_json = COALESCE(%s, token_json),
                attempt_cnt = COALESCE(attempt_cnt, 0) + 1
            WHERE id = %s
        """, (name, symbol, icon, decimals, token_json_str, token_id))

    conn.commit()
    return has_meaningful_data


def increment_token_attempt(cursor, conn, token_id: int):
    """Increment attempt count for a token"""
    cursor.execute("""
        UPDATE tx_token SET attempt_cnt = COALESCE(attempt_cnt, 0) + 1
        WHERE id = %s
    """, (token_id,))
    conn.commit()


MULTI_META_BATCH_SIZE = 50


def enrich_tokens(session, cursor, conn, limit: int, max_attempts: int,
                  delay: float, verbose: bool = True) -> Dict[str, int]:
    """Enrich tokens with metadata from Solscan using /v2.0/token/meta/multi.
    Phase 0: Local label-based backfill (sp_tx_token_backfill)
    Phase 1: Solscan API for remaining tokens.
    Fetches up to 50 addresses per API call. Returns stats."""
    stats = {'processed': 0, 'updated': 0, 'failed': 0, 'backfill_updated': 0}

    # Phase 0: Backfill from tx_address labels (LP tokens, etc.)
    try:
        cursor.execute("CALL sp_tx_token_backfill(@updated)")
        try:
            while cursor.nextset():
                pass
        except:
            pass
        cursor.execute("SELECT @updated")
        row = cursor.fetchone()
        bf_updated = row[0] or 0
        stats['backfill_updated'] = bf_updated
        if verbose and bf_updated > 0:
            print(f"  [tokens] Phase 0: backfill updated={bf_updated}")
    except Exception as e:
        if verbose:
            print(f"  [tokens] Phase 0: backfill error: {e}")

    # Phase 1: Solscan API enrichment
    tokens = get_tokens_needing_metadata(cursor, limit, max_attempts)
    if not tokens:
        if verbose:
            print("  [tokens] No tokens need enrichment")
        return stats

    if verbose:
        print(f"  [tokens] Found {len(tokens)} tokens needing metadata")

    # Process in chunks of MULTI_META_BATCH_SIZE
    for chunk_start in range(0, len(tokens), MULTI_META_BATCH_SIZE):
        chunk = tokens[chunk_start:chunk_start + MULTI_META_BATCH_SIZE]
        mint_to_token = {t['mint']: t for t in chunk}
        chunk_num = chunk_start // MULTI_META_BATCH_SIZE + 1
        total_chunks = (len(tokens) + MULTI_META_BATCH_SIZE - 1) // MULTI_META_BATCH_SIZE

        if verbose:
            print(f"    Batch {chunk_num}/{total_chunks}: fetching {len(chunk)} tokens...", end=" ")

        try:
            results = fetch_token_meta_multi(session, list(mint_to_token.keys()))

            if verbose:
                print(f"got {len(results)} results")

            for mint, token_info in mint_to_token.items():
                token_id = token_info['id']
                response = results.get(mint)

                if response:
                    name = response.get('name')
                    symbol = response.get('symbol')
                    icon = response.get('icon')
                    decimals = response.get('decimals')

                    if name or symbol or decimals is not None:
                        if update_token_metadata(cursor, conn, token_id, name, symbol, icon, decimals, response):
                            if verbose:
                                print(f"      {mint[:16]}... {symbol or 'unnamed'} ({decimals} dec)")
                            stats['updated'] += 1
                        else:
                            increment_token_attempt(cursor, conn, token_id)
                            stats['failed'] += 1
                    else:
                        increment_token_attempt(cursor, conn, token_id)
                        stats['failed'] += 1
                else:
                    if verbose:
                        print(f"      {mint[:16]}... not found")
                    increment_token_attempt(cursor, conn, token_id)
                    stats['failed'] += 1

                stats['processed'] += 1

        except Exception as e:
            if verbose:
                print(f"error: {e}")
            for token_info in chunk:
                increment_token_attempt(cursor, conn, token_info['id'])
                stats['failed'] += 1
                stats['processed'] += 1

        if delay > 0 and chunk_start + MULTI_META_BATCH_SIZE < len(tokens):
            time.sleep(delay)

    return stats


# =============================================================================
# Pool Enrichment (from guide-pool-enricher.py)
# =============================================================================

def get_pools_needing_labels(cursor, limit: int, max_attempts: int = 3) -> List[Dict]:
    """Get pools missing pool_label"""
    cursor.execute("""
        SELECT a.id as address_id, a.address, p.id as pool_id
        FROM tx_pool p
        JOIN tx_address a ON a.id = p.pool_address_id
        WHERE p.pool_label IS NULL
          AND COALESCE(p.attempt_cnt, 0) < %s
        LIMIT %s
    """, (max_attempts, limit))
    return [{'address_id': row[0], 'address': row[1], 'pool_id': row[2]}
            for row in cursor.fetchall()]


def get_pools_needing_structure(cursor, limit: int, max_attempts: int = 3) -> List[Dict]:
    """Get pools missing structural data (token accounts, program, etc.)"""
    cursor.execute("""
        SELECT a.id as address_id, a.address, p.id as pool_id
        FROM tx_pool p
        JOIN tx_address a ON a.id = p.pool_address_id
        WHERE p.token_account1_id IS NULL
          AND COALESCE(p.attempt_cnt, 0) < %s
        LIMIT %s
    """, (max_attempts, limit))
    return [{'address_id': row[0], 'address': row[1], 'pool_id': row[2]}
            for row in cursor.fetchall()]


def increment_pool_attempt(cursor, conn, pool_id: int):
    """Increment attempt count for a pool"""
    cursor.execute("""
        UPDATE tx_pool SET attempt_cnt = COALESCE(attempt_cnt, 0) + 1
        WHERE id = %s
    """, (pool_id,))
    conn.commit()


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


def ensure_program(cursor, conn, program_address: str) -> tuple:
    """Ensure program exists, return (program_id, program_address_id)"""
    if not program_address:
        return None, None

    addr_id = ensure_address(cursor, conn, program_address, 'program')

    cursor.execute("SELECT id FROM tx_program WHERE program_address_id = %s", (addr_id,))
    row = cursor.fetchone()
    if row:
        return row[0], addr_id

    cursor.execute("""
        INSERT INTO tx_program (program_address_id, program_type)
        VALUES (%s, 'dex')
    """, (addr_id,))
    conn.commit()
    return cursor.lastrowid, addr_id


def ensure_token(cursor, conn, mint_address: str) -> Optional[int]:
    """Ensure token exists, return id. Skips non-token addresses."""
    if not mint_address:
        return None

    cursor.execute("SELECT id, address_type FROM tx_address WHERE address = %s", (mint_address,))
    row = cursor.fetchone()

    if row:
        addr_id, addr_type = row
        if addr_type in ('program', 'pool', 'ata'):
            return None
        if addr_type in ('unknown', 'wallet'):
            cursor.execute("UPDATE tx_address SET address_type = 'mint' WHERE id = %s", (addr_id,))
            conn.commit()
    else:
        cursor.execute(
            "INSERT INTO tx_address (address, address_type) VALUES (%s, 'mint')",
            (mint_address,)
        )
        conn.commit()
        addr_id = cursor.lastrowid

    cursor.execute("SELECT id FROM tx_token WHERE mint_address_id = %s", (addr_id,))
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute("INSERT INTO tx_token (mint_address_id) VALUES (%s)", (addr_id,))
    conn.commit()
    return cursor.lastrowid


def update_pool_from_api(cursor, conn, pool_address_id: int, pool_id: int,
                         api_data: Dict, label: str = None) -> bool:
    """Update pool with data from API response"""
    try:
        program_address = api_data.get('program_id')
        if not program_address:
            return False

        program_id, program_address_id_fk = ensure_program(cursor, conn, program_address)

        tokens_info = api_data.get('tokens_info', [])
        token1_id = None
        token2_id = None
        token_account1_id = None
        token_account2_id = None

        if len(tokens_info) >= 1:
            token1_mint = tokens_info[0].get('token')
            token1_account = tokens_info[0].get('token_account')
            if token1_mint:
                token1_id = ensure_token(cursor, conn, token1_mint)
            if token1_account:
                token_account1_id = ensure_address(cursor, conn, token1_account, 'vault')

        if len(tokens_info) >= 2:
            token2_mint = tokens_info[1].get('token')
            token2_account = tokens_info[1].get('token_account')
            if token2_mint:
                token2_id = ensure_token(cursor, conn, token2_mint)
            if token2_account:
                token_account2_id = ensure_address(cursor, conn, token2_account, 'vault')

        lp_token_id = None
        lp_token_mint = api_data.get('lp_token')
        if lp_token_mint:
            lp_token_id = ensure_token(cursor, conn, lp_token_mint)

        # Update tx_address.program_id
        cursor.execute("""
            UPDATE tx_address SET program_id = %s WHERE id = %s
        """, (program_address_id_fk, pool_address_id))

        # Update tx_pool
        # Only reset attempt_cnt if we have all required data (token_account1_id and label)
        has_all_data = token_account1_id is not None and label is not None
        cursor.execute("""
            UPDATE tx_pool SET
                program_id = COALESCE(%s, program_id),
                token1_id = COALESCE(%s, token1_id),
                token2_id = COALESCE(%s, token2_id),
                token_account1_id = COALESCE(%s, token_account1_id),
                token_account2_id = COALESCE(%s, token_account2_id),
                lp_token_id = COALESCE(%s, lp_token_id),
                pool_label = COALESCE(%s, pool_label),
                attempt_cnt = CASE WHEN %s THEN 0 ELSE attempt_cnt END
            WHERE id = %s
        """, (program_id, token1_id, token2_id,
              token_account1_id, token_account2_id, lp_token_id,
              label, has_all_data, pool_id))
        conn.commit()
        return True

    except Exception as e:
        print(f"    [!] DB Error: {e}")
        conn.rollback()
        return False


def enrich_pools(session, cursor, conn, limit: int, max_attempts: int,
                 delay: float, verbose: bool = True) -> Dict[str, int]:
    """Enrich pools with data from Solscan. Returns stats.
    Phase 1: Batch-fetch labels via /account/metadata/multi (50 at a time)
    Phase 2: Individual /market/info for pools missing structural data
    """
    stats = {'processed': 0, 'updated': 0, 'labels': 0, 'not_found': 0, 'failed': 0, 'backfill_created': 0, 'backfill_updated': 0}

    # Phase 0: Run sp_tx_pool_backfill to enrich from local data before hitting APIs
    try:
        cursor.execute("CALL sp_tx_pool_backfill(@created, @updated, @accounts)")
        try:
            while cursor.nextset():
                pass
        except:
            pass
        cursor.execute("SELECT @created, @updated, @accounts")
        row = cursor.fetchone()
        bf_created = row[0] or 0
        bf_updated = row[1] or 0
        bf_accounts = row[2] or 0
        stats['backfill_created'] = bf_created
        stats['backfill_updated'] = bf_updated
        if verbose and (bf_created > 0 or bf_updated > 0 or bf_accounts > 0):
            print(f"  [pools] Phase 0: backfill created={bf_created}, updated={bf_updated}, accounts_copied={bf_accounts}")
    except Exception as e:
        if verbose:
            print(f"  [pools] Phase 0: backfill error: {e}")

    # Phase 1: Batch label enrichment
    label_pools = get_pools_needing_labels(cursor, limit, max_attempts)
    if label_pools:
        if verbose:
            print(f"  [pools] Phase 1: {len(label_pools)} pools needing labels")

        for chunk_start in range(0, len(label_pools), MULTI_META_BATCH_SIZE):
            chunk = label_pools[chunk_start:chunk_start + MULTI_META_BATCH_SIZE]
            addr_to_pool = {p['address']: p for p in chunk}
            chunk_num = chunk_start // MULTI_META_BATCH_SIZE + 1
            total_chunks = (len(label_pools) + MULTI_META_BATCH_SIZE - 1) // MULTI_META_BATCH_SIZE

            if verbose:
                print(f"    Batch {chunk_num}/{total_chunks}: fetching {len(chunk)} labels...", end=" ")

            try:
                metadata_map = fetch_account_metadata_multi(session, list(addr_to_pool.keys()))

                if verbose:
                    print(f"got {len(metadata_map)} results")

                for address, pool_info in addr_to_pool.items():
                    pool_id = pool_info['pool_id']
                    meta = metadata_map.get(address)
                    label = meta.get('account_label') if meta else None

                    if label:
                        cursor.execute("""
                            UPDATE tx_pool SET pool_label = %s, attempt_cnt = 0
                            WHERE id = %s AND pool_label IS NULL
                        """, (label, pool_id))
                        # Also update tx_address.label
                        cursor.execute("""
                            UPDATE tx_address SET label = %s
                            WHERE id = %s AND label IS NULL
                        """, (label, pool_info['address_id']))
                        conn.commit()
                        if verbose:
                            print(f"      {address[:16]}... {label}")
                        stats['labels'] += 1
                        stats['updated'] += 1
                    else:
                        increment_pool_attempt(cursor, conn, pool_id)
                        stats['failed'] += 1

                    stats['processed'] += 1

            except Exception as e:
                if verbose:
                    print(f"error: {e}")
                for pool_info in chunk:
                    increment_pool_attempt(cursor, conn, pool_info['pool_id'])
                    stats['failed'] += 1
                    stats['processed'] += 1

            if delay > 0 and chunk_start + MULTI_META_BATCH_SIZE < len(label_pools):
                time.sleep(delay)
    else:
        if verbose:
            print("  [pools] Phase 1: no pools need labels")

    # Phase 2: Individual structural enrichment via /market/info
    struct_pools = get_pools_needing_structure(cursor, limit, max_attempts)
    if struct_pools:
        if verbose:
            print(f"  [pools] Phase 2: {len(struct_pools)} pools needing structure")

        for i, pool in enumerate(struct_pools):
            address = pool['address']
            address_id = pool['address_id']
            pool_id = pool['pool_id']

            if verbose:
                print(f"    [{i+1}/{len(struct_pools)}] {address[:20]}...", end=" ")

            increment_pool_attempt(cursor, conn, pool_id)

            api_data = fetch_pool_info(session, address)

            if not api_data:
                if verbose:
                    print("not found")
                stats['not_found'] += 1
                stats['processed'] += 1
                if delay > 0:
                    time.sleep(delay)
                continue

            # Use existing pool_label if already set from Phase 1
            cursor.execute("SELECT pool_label FROM tx_pool WHERE id = %s", (pool_id,))
            row = cursor.fetchone()
            existing_label = row[0] if row else None

            if update_pool_from_api(cursor, conn, address_id, pool_id, api_data, existing_label):
                program = api_data.get('program_id', 'unknown')
                if verbose:
                    print(f"{program[:20]}...")
                stats['updated'] += 1
            else:
                if verbose:
                    print("failed")
                stats['failed'] += 1

            stats['processed'] += 1

            if delay > 0 and i < len(struct_pools) - 1:
                time.sleep(delay)
    else:
        if verbose:
            print("  [pools] Phase 2: no pools need structure")

    return stats


def enrich_single_pool(session, cursor, conn, address: str):
    """Enrich a single pool address (for testing/manual enrichment)"""
    print(f"Enriching pool: {address}\n")

    # Check/create address
    cursor.execute("SELECT id, address_type FROM tx_address WHERE address = %s", (address,))
    row = cursor.fetchone()

    if not row:
        cursor.execute("INSERT INTO tx_address (address, address_type) VALUES (%s, 'pool')", (address,))
        conn.commit()
        address_id = cursor.lastrowid
        print(f"  [+] Created address (id={address_id})")
    else:
        address_id = row[0]
        print(f"  [i] Address exists (id={address_id}, type={row[1]})")

    # Check/create pool
    cursor.execute("SELECT id FROM tx_pool WHERE pool_address_id = %s", (address_id,))
    pool_row = cursor.fetchone()

    if not pool_row:
        cursor.execute("INSERT INTO tx_pool (pool_address_id) VALUES (%s)", (address_id,))
        conn.commit()
        pool_id = cursor.lastrowid
        print(f"  [+] Created pool (id={pool_id})")
    else:
        pool_id = pool_row[0]
        print(f"  [i] Pool exists (id={pool_id})")

    # Fetch market info
    print(f"\n  Fetching /market/info...")
    api_data = fetch_pool_info(session, address)

    if api_data:
        print(f"  [+] Market info found:")
        print(f"      Program: {api_data.get('program_id', 'unknown')}")
        tokens = api_data.get('tokens_info', [])
        for j, t in enumerate(tokens):
            print(f"      Token {j+1}: {t.get('token', '?')}")
    else:
        print(f"  [-] No market info on Solscan")

    # Fetch metadata
    print(f"\n  Fetching /account/metadata...")
    time.sleep(0.3)
    metadata = fetch_account_metadata(session, address)
    label = metadata.get('account_label') if metadata else None

    if label:
        print(f"  [+] Label: {label}")
    else:
        print(f"  [-] No label found")

    # Update pool
    if api_data:
        if update_pool_from_api(cursor, conn, address_id, pool_id, api_data, label):
            print(f"\n  [+] Pool updated successfully")
        else:
            print(f"\n  [-] Failed to update pool")

    # Show final state
    print(f"\n  Final pool state:")
    cursor.execute("""
        SELECT p.id, p.pool_label, p.attempt_cnt,
               prog_a.address as program,
               t1_a.address as token1, t2_a.address as token2
        FROM tx_pool p
        LEFT JOIN tx_program prog ON prog.id = p.program_id
        LEFT JOIN tx_address prog_a ON prog_a.id = prog.program_address_id
        LEFT JOIN tx_token t1 ON t1.id = p.token1_id
        LEFT JOIN tx_address t1_a ON t1_a.id = t1.mint_address_id
        LEFT JOIN tx_token t2 ON t2.id = p.token2_id
        LEFT JOIN tx_address t2_a ON t2_a.id = t2.mint_address_id
        WHERE p.id = %s
    """, (pool_id,))
    final = cursor.fetchone()
    if final:
        print(f"      pool_id: {final[0]}")
        print(f"      label: {final[1]}")
        print(f"      attempts: {final[2]}")
        print(f"      program: {final[3]}")
        print(f"      token1: {final[4]}")
        print(f"      token2: {final[5]}")


# =============================================================================
# Program Enrichment (uses existing fetch_account_metadata)
# =============================================================================

# Known program type mappings based on address or label keywords
PROGRAM_TYPE_KEYWORDS = {
    'dex': ['swap', 'amm', 'dex', 'raydium', 'orca', 'jupiter', 'serum', 'openbook'],
    'lending': ['lend', 'borrow', 'solend', 'mango', 'port', 'jet', 'marginfi'],
    'nft': ['nft', 'metaplex', 'candy', 'auction'],
    'token': ['token', 'spl-token', 'mint'],
    'system': ['system', 'vote', 'stake', 'config'],
    'compute': ['compute', 'budget'],
    'router': ['router', 'aggregator', 'jupiter'],
}

KNOWN_PROGRAMS = {
    '11111111111111111111111111111111': ('System Program', 'system'),
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA': ('Token Program', 'token'),
    'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL': ('Associated Token Program', 'token'),
    'ComputeBudget111111111111111111111111111111': ('Compute Budget', 'compute'),
    'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK': ('Raydium CPMM', 'dex'),
    'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C': ('Raydium CPMM', 'dex'),
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8': ('Raydium AMM V4', 'dex'),
    'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc': ('Orca Whirlpool', 'dex'),
    'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4': ('Jupiter V6', 'router'),
    'LBUZKhRxPF3XUpBCjp4YzTKgLXWjvfY2LnPkDsHEVBo': ('Meteora DLMM', 'dex'),
    'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB': ('Meteora Pools', 'dex'),
    'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA': ('Pump.fun AMM', 'dex'),
    '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P': ('Pump.fun', 'dex'),
}


def infer_program_type(name: str, label: str) -> str:
    """Infer program type from name or label"""
    text = f"{name or ''} {label or ''}".lower()

    for prog_type, keywords in PROGRAM_TYPE_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                return prog_type

    return 'other'


def get_programs_needing_enrichment(cursor, limit: int, max_attempts: int = 3) -> List[Dict]:
    """Get programs missing name metadata"""
    cursor.execute("""
        SELECT p.id, a.address
        FROM tx_program p
        JOIN tx_address a ON a.id = p.program_address_id
        WHERE p.name IS NULL
          AND COALESCE(p.attempt_cnt, 0) < %s
        LIMIT %s
    """, (max_attempts, limit))
    return [{'id': row[0], 'address': row[1]} for row in cursor.fetchall()]


def update_program_metadata(cursor, conn, program_id: int, name: str,
                            program_type: str) -> bool:
    """Update program with fetched metadata.

    Only resets attempt_cnt if we got meaningful data (non-empty name).
    Otherwise increments attempt_cnt to prevent infinite retry loops.
    """
    has_meaningful_data = bool(name and name.strip())

    if has_meaningful_data:
        cursor.execute("""
            UPDATE tx_program
            SET name = %s,
                program_type = %s,
                attempt_cnt = 0
            WHERE id = %s
        """, (name, program_type, program_id))
    else:
        cursor.execute("""
            UPDATE tx_program
            SET attempt_cnt = COALESCE(attempt_cnt, 0) + 1
            WHERE id = %s
        """, (program_id,))

    conn.commit()
    return has_meaningful_data


def enrich_programs(session, cursor, conn, limit: int, max_attempts: int,
                    delay: float, verbose: bool = True) -> Dict[str, int]:
    """Enrich programs with metadata from Solscan. Returns stats.

    Uses /v2.0/account/metadata endpoint (same as pool label fetching).
    """
    stats = {'processed': 0, 'updated': 0, 'known': 0, 'failed': 0}

    programs = get_programs_needing_enrichment(cursor, limit, max_attempts)
    if not programs:
        if verbose:
            print("  [programs] No programs need enrichment")
        return stats

    if verbose:
        print(f"  [programs] Found {len(programs)} programs needing metadata")

    for i, program in enumerate(programs):
        program_id = program['id']
        address = program['address']

        if verbose:
            print(f"    [{i+1}/{len(programs)}] {address[:20]}...", end=" ")

        # Check if it's a known program first (no API call needed)
        if address in KNOWN_PROGRAMS:
            name, prog_type = KNOWN_PROGRAMS[address]
            if update_program_metadata(cursor, conn, program_id, name, prog_type):
                if verbose:
                    print(f"{name} (known)")
                stats['known'] += 1
                stats['updated'] += 1
            stats['processed'] += 1
            continue

        # Fetch from Solscan API (reuse existing function)
        try:
            metadata = fetch_account_metadata(session, address)

            if metadata:
                label = metadata.get('account_label') or metadata.get('label')

                if label:
                    prog_type = infer_program_type(label, label)
                    if update_program_metadata(cursor, conn, program_id, label, prog_type):
                        if verbose:
                            print(f"{label} ({prog_type})")
                        stats['updated'] += 1
                    else:
                        if verbose:
                            print("no change")
                        stats['failed'] += 1
                else:
                    if verbose:
                        print("no label")
                    # Increment attempt count for programs with no label
                    cursor.execute("""
                        UPDATE tx_program
                        SET attempt_cnt = COALESCE(attempt_cnt, 0) + 1
                        WHERE id = %s
                    """, (program_id,))
                    conn.commit()
                    stats['failed'] += 1
            else:
                if verbose:
                    print("not found")
                cursor.execute("""
                    UPDATE tx_program
                    SET attempt_cnt = COALESCE(attempt_cnt, 0) + 1
                    WHERE id = %s
                """, (program_id,))
                conn.commit()
                stats['failed'] += 1

        except Exception as e:
            if verbose:
                print(f"error: {e}")
            cursor.execute("""
                UPDATE tx_program
                SET attempt_cnt = COALESCE(attempt_cnt, 0) + 1
                WHERE id = %s
            """, (program_id,))
            conn.commit()
            stats['failed'] += 1

        stats['processed'] += 1

        if delay > 0 and i < len(programs) - 1:
            time.sleep(delay)

    return stats


# =============================================================================
# Status Functions
# =============================================================================

def get_enrichment_status(cursor) -> Dict[str, Any]:
    """Get current enrichment status"""
    # Tokens needing metadata
    cursor.execute("""
        SELECT COUNT(*) FROM tx_token
        WHERE (token_symbol IS NULL OR token_symbol = ''
           OR token_name IS NULL OR token_name = ''
           OR decimals IS NULL)
          AND COALESCE(attempt_cnt, 0) < 3
    """)
    tokens_pending = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM tx_token WHERE token_symbol IS NOT NULL AND token_symbol != ''")
    tokens_enriched = cursor.fetchone()[0]

    # Pools needing enrichment
    cursor.execute("""
        SELECT COUNT(*) FROM tx_pool
        WHERE (token_account1_id IS NULL OR pool_label IS NULL)
          AND COALESCE(attempt_cnt, 0) < 3
    """)
    pools_pending = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM tx_pool WHERE token_account1_id IS NOT NULL")
    pools_enriched = cursor.fetchone()[0]

    # Programs needing enrichment
    cursor.execute("""
        SELECT COUNT(*) FROM tx_program
        WHERE name IS NULL
          AND COALESCE(attempt_cnt, 0) < 3
    """)
    programs_pending = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM tx_program WHERE name IS NOT NULL")
    programs_enriched = cursor.fetchone()[0]

    return {
        'tokens': {'pending': tokens_pending, 'enriched': tokens_enriched},
        'pools': {'pending': pools_pending, 'enriched': pools_enriched},
        'programs': {'pending': programs_pending, 'enriched': programs_enriched},
    }


def print_status(status: Dict):
    """Print enrichment status"""
    print(f"\n{'='*60}")
    print(f"Guide Enricher Status")
    print(f"{'='*60}")
    print(f"  Tokens pending:   {status['tokens']['pending']:,}")
    print(f"  Tokens enriched:  {status['tokens']['enriched']:,}")
    print(f"  Pools pending:    {status['pools']['pending']:,}")
    print(f"  Pools enriched:   {status['pools']['enriched']:,}")
    print(f"  Programs pending: {status['programs']['pending']:,}")
    print(f"  Programs enriched:{status['programs']['enriched']:,}")
    print(f"{'='*60}\n")


# =============================================================================
# RabbitMQ Gateway Integration
# =============================================================================

def setup_gateway_rabbitmq():
    """Setup connection to gateway RabbitMQ (t16o_mq vhost)"""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            virtual_host=RABBITMQ_VHOST,
            credentials=credentials,
            heartbeat=600
        )
    )
    channel = connection.channel()

    for queue in [RABBITMQ_REQUEST_QUEUE, RABBITMQ_RESPONSE_QUEUE]:
        channel.queue_declare(queue=queue, durable=True, arguments={'x-max-priority': 10})

    return connection, channel


def publish_response(channel, request_id: str, status: str, result: dict, error: str = None):
    """Publish response to gateway response queue"""
    response = {
        'request_id': request_id,
        'worker': 'enricher',
        'status': status,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'result': result
    }
    if error:
        response['error'] = error

    channel.basic_publish(
        exchange='',
        routing_key=RABBITMQ_RESPONSE_QUEUE,
        body=json.dumps(response).encode('utf-8'),
        properties=pika.BasicProperties(delivery_mode=2, priority=5)
    )


def run_queue_consumer(prefetch: int = 1):
    """Run as a queue consumer, listening for gateway requests"""
    print(f"""
+-----------------------------------------------------------+
|         Guide Enricher - Queue Consumer Mode              |
|                                                           |
|  vhost:     {RABBITMQ_VHOST}                                     |
|  queue:     {RABBITMQ_REQUEST_QUEUE}            |
|  prefetch:  {prefetch}                                            |
+-----------------------------------------------------------+
""")

    if not HAS_PIKA:
        print("Error: pika not installed")
        return 1

    # Setup DB connection with reconnect capability
    db_state = {'conn': None, 'cursor': None}

    def ensure_db_connection():
        """Ensure DB connection is alive, reconnect if needed"""
        try:
            needs_reconnect = db_state['conn'] is None
            if not needs_reconnect:
                try:
                    db_state['conn'].ping(reconnect=False, attempts=1, delay=0)
                except:
                    needs_reconnect = True

            if needs_reconnect:
                if db_state['conn']:
                    try:
                        db_state['conn'].close()
                    except:
                        pass
                db_state['conn'] = get_db_connection()
                db_state['cursor'] = db_state['conn'].cursor()
                print("[OK] Database (re)connected")
            return db_state['cursor'], db_state['conn']
        except Exception as e:
            print(f"[WARN] Database connection failed: {e}")
            db_state['conn'] = None
            db_state['cursor'] = None
            return None, None

    # Initial connection
    ensure_db_connection()

    # Setup Solscan session
    session = create_api_session()
    print("[OK] Solscan session created")

    while True:
        try:
            gateway_conn, gateway_channel = setup_gateway_rabbitmq()
            gateway_channel.basic_qos(prefetch_count=prefetch)

            print(f"[OK] Connected to {RABBITMQ_REQUEST_QUEUE}")
            print("[INFO] Waiting for requests...")

            def callback(ch, method, properties, body):
                try:
                    message = json.loads(body.decode('utf-8'))
                    request_id = message.get('request_id', 'unknown')
                    batch = message.get('batch', {})

                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Received request {request_id[:8]}")

                    # Ensure DB connection is alive
                    cursor, conn = ensure_db_connection()
                    if not cursor:
                        raise Exception("Database connection unavailable")

                    # Get operations from batch or default to tokens,pools,programs
                    operations = batch.get('operations', ['tokens', 'pools', 'programs'])
                    if isinstance(operations, str):
                        operations = [op.strip() for op in operations.split(',')]

                    limit = batch.get('limit', 100)
                    max_attempts = batch.get('max_attempts', 3)
                    delay = batch.get('delay', 0.3)

                    print(f"  Running enrichment: {', '.join(operations)}")

                    try:
                        total_enriched = 0

                        if 'tokens' in operations:
                            stats = enrich_tokens(session, cursor, conn, limit, max_attempts, delay, verbose=True)
                            total_enriched += stats.get('updated', 0)

                        if 'pools' in operations:
                            stats = enrich_pools(session, cursor, conn, limit, max_attempts, delay, verbose=True)
                            total_enriched += stats.get('updated', 0)

                        if 'programs' in operations:
                            stats = enrich_programs(session, cursor, conn, limit, max_attempts, delay, verbose=True)
                            total_enriched += stats.get('updated', 0)

                        result = {'processed': total_enriched}
                        status = 'completed'
                        print(f"  Completed: {total_enriched} items enriched")

                    except Exception as e:
                        print(f"  [ERROR] {e}")
                        result = {'processed': 0, 'error': str(e)}
                        status = 'failed'

                    # Publish response
                    publish_response(gateway_channel, request_id, status, result)
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Error as e:
                    # MySQL errors are transient - requeue for retry
                    print(f"[DB ERROR] {e} - will retry")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                except Exception as e:
                    # Other errors - send to DLQ
                    print(f"[ERROR] Failed to process message: {e}")
                    import traceback
                    traceback.print_exc()
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

            gateway_channel.basic_consume(
                queue=RABBITMQ_REQUEST_QUEUE,
                on_message_callback=callback,
                auto_ack=False
            )

            gateway_channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            print(f"[WARN] RabbitMQ connection lost: {e}")
            print("[INFO] Reconnecting in 5 seconds...")
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n[INFO] Shutting down...")
            break
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)

    cursor.close()
    conn.close()
    return 0


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Guide Enricher - Consolidated enrichment worker',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Operations (--enrich):
  tokens  - Fetch token metadata (name, symbol, decimals)
  pools   - Fetch pool info (program, tokens, labels)
  all     - Both tokens and pools (default)

Examples:
  python guide-enricher.py                          # All enrichment, single run
  python guide-enricher.py --daemon --interval 60  # Continuous mode
  python guide-enricher.py --enrich tokens         # Only token metadata
  python guide-enricher.py --enrich pools          # Only pool data
  python guide-enricher.py --pool <address>        # Enrich single pool
  python guide-enricher.py --status                # Show enrichment status
        """
    )

    parser.add_argument('--enrich', '-e', type=str, default='all',
                        help='Operations: tokens,pools,all (default: all)')
    parser.add_argument('--daemon', '-d', action='store_true',
                        help='Run continuously in daemon mode')
    parser.add_argument('--interval', '-i', type=int, default=60,
                        help='Interval in seconds for daemon mode (default: 60)')
    parser.add_argument('--limit', '-l', type=int, default=100,
                        help='Max items per batch (default: 100)')
    parser.add_argument('--max-attempts', type=int, default=3,
                        help='Skip items with more than N failed attempts (default: 3)')
    parser.add_argument('--delay', type=float, default=0.3,
                        help='Delay between API calls (default: 0.3)')
    parser.add_argument('--pool', type=str,
                        help='Enrich single pool address')
    parser.add_argument('--status', action='store_true',
                        help='Show current enrichment status')
    parser.add_argument('--quiet', '-q', action='store_true',
                        help='Minimal output')
    parser.add_argument('--json', action='store_true',
                        help='Output in JSON format')
    parser.add_argument('--queue-consumer', action='store_true',
                        help='Run as queue consumer, listening for gateway requests')
    parser.add_argument('--prefetch', type=int, default=1,
                        help='Prefetch count for queue consumer mode (default: 1)')

    args = parser.parse_args()

    if args.queue_consumer:
        return run_queue_consumer(prefetch=args.prefetch)

    if not HAS_REQUESTS:
        print("Error: requests not installed")
        return 1

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        return 1

    # Parse operations
    if args.enrich.lower() == 'all':
        operations = ['tokens', 'pools', 'programs']
    else:
        operations = [op.strip().lower() for op in args.enrich.split(',')]
        valid_ops = {'tokens', 'pools', 'programs'}
        invalid = set(operations) - valid_ops
        if invalid:
            print(f"Error: Invalid operations: {invalid}. Valid: tokens, pools, programs, all")
            return 1

    conn = get_db_connection()
    if not conn:
        return 1
    cursor = conn.cursor()

    session = create_api_session()

    try:
        # Status mode
        if args.status:
            status = get_enrichment_status(cursor)
            if args.json:
                print(json.dumps(status, indent=2))
            else:
                print_status(status)
            return 0

        # Single pool mode
        if args.pool:
            enrich_single_pool(session, cursor, conn, args.pool)
            return 0

        # Banner
        if not args.quiet and not args.json:
            print(f"\n{'='*60}")
            print(f"Guide Enricher")
            print(f"{'='*60}")
            print(f"  Database:   {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
            print(f"  Operations: {', '.join(operations)}")
            print(f"  Mode:       {'DAEMON' if args.daemon else 'SINGLE RUN'}")
            print(f"  Limit:      {args.limit}")
            if args.daemon:
                print(f"  Interval:   {args.interval}s")
            print(f"{'='*60}\n")

        if args.daemon:
            print("Press Ctrl+C to stop\n")
            batch_num = 0

            while True:
                try:
                    batch_num += 1
                    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

                    if not args.quiet:
                        print(f"[{timestamp}] Batch {batch_num}...")

                    total_updated = 0

                    if 'tokens' in operations:
                        stats = enrich_tokens(session, cursor, conn, args.limit,
                                             args.max_attempts, args.delay, not args.quiet)
                        total_updated += stats['updated']

                    if 'pools' in operations:
                        stats = enrich_pools(session, cursor, conn, args.limit,
                                            args.max_attempts, args.delay, not args.quiet)
                        total_updated += stats['updated']

                    if 'programs' in operations:
                        stats = enrich_programs(session, cursor, conn, args.limit,
                                               args.max_attempts, args.delay, not args.quiet)
                        total_updated += stats['updated']

                    if not args.quiet:
                        if total_updated > 0:
                            print(f"[{timestamp}] Complete: {total_updated} items enriched\n")
                        else:
                            print(f"[{timestamp}] No changes\n")

                    # Close connection before sleeping
                    try:
                        cursor.close()
                        conn.close()
                    except:
                        pass

                    time.sleep(args.interval)

                    # Reconnect
                    conn = get_db_connection()
                    if not conn:
                        time.sleep(args.interval)
                        continue
                    cursor = conn.cursor()

                except KeyboardInterrupt:
                    print("\nStopping daemon...")
                    break
                except Exception as e:
                    print(f"Error: {e}")
                    try:
                        cursor.close()
                        conn.close()
                    except:
                        pass
                    time.sleep(args.interval)
                    conn = get_db_connection()
                    if conn:
                        cursor = conn.cursor()

        else:
            # Single run
            total_stats = {'tokens': {}, 'pools': {}, 'programs': {}}

            if 'tokens' in operations:
                total_stats['tokens'] = enrich_tokens(session, cursor, conn, args.limit,
                                                       args.max_attempts, args.delay, not args.quiet)

            if 'pools' in operations:
                total_stats['pools'] = enrich_pools(session, cursor, conn, args.limit,
                                                     args.max_attempts, args.delay, not args.quiet)

            if 'programs' in operations:
                total_stats['programs'] = enrich_programs(session, cursor, conn, args.limit,
                                                          args.max_attempts, args.delay, not args.quiet)

            if args.json:
                print(json.dumps(total_stats, indent=2))
            elif not args.quiet:
                print(f"\n{'='*60}")
                print(f"Enrichment Complete")
                print(f"{'='*60}")
                if 'tokens' in operations:
                    ts = total_stats['tokens']
                    print(f"  Tokens:   {ts.get('updated', 0)} updated, {ts.get('failed', 0)} failed")
                if 'pools' in operations:
                    ps = total_stats['pools']
                    print(f"  Pools:    {ps.get('updated', 0)} updated, {ps.get('not_found', 0)} not found")
                if 'programs' in operations:
                    prs = total_stats['programs']
                    print(f"  Programs: {prs.get('updated', 0)} updated ({prs.get('known', 0)} known), {prs.get('failed', 0)} failed")
                print(f"{'='*60}\n")

    finally:
        session.close()
        try:
            cursor.close()
            conn.close()
        except:
            pass

    return 0


if __name__ == '__main__':
    sys.exit(main())
