#!/usr/bin/env python3
"""
Guide Address Classifier - Verify and reclassify tx_address entries via RPC/Solscan

Uses hybrid approach:
1. First pass: RPC getMultipleAccounts for bulk classification
2. Second pass: Solscan API for ambiguous cases
3. Updates tx_address.address_type in MySQL

Configuration:
    Copy guide-config.json.example to guide-config.json and fill in your API keys.
    Alternatively, set environment variables:
    - SOLANA_RPC_URL: Solana RPC endpoint (e.g., Chainstack, Helius)
    - SOLSCAN_API_TOKEN: Solscan Pro API JWT token
    - MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

Usage:
    python guide-address-classifier.py                           # Process suspect_wallets_for_rpc_check.txt
    python guide-address-classifier.py --file addresses.txt     # Custom file
    python guide-address-classifier.py --dry-run                # Preview without DB updates
    python guide-address-classifier.py --limit 100              # Process first N addresses
"""

import argparse
import json
import time
import base64
import struct
import os
import functools
import random
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Callable, Any
from collections import defaultdict
from datetime import datetime

import requests
import mysql.connector


# =============================================================================
# Config Loading
# =============================================================================

def load_config() -> dict:
    """
    Load configuration from JSON file with environment variable fallback.

    Config file search order:
    1. ./guide-config.json (current directory)
    2. Same directory as this script
    3. Environment variables
    """
    config = {}
    config_paths = [
        Path('./guide-config.json'),
        Path(__file__).parent / 'guide-config.json',
    ]

    # Try to load from JSON file
    for config_path in config_paths:
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                print(f"Loaded config from: {config_path}")
                break
            except Exception as e:
                print(f"Warning: Failed to load {config_path}: {e}")

    # Environment variable fallback/override
    env_mappings = {
        'RPC_URL': 'SOLANA_RPC_URL',
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


# Load config at module level
_CONFIG = load_config()


# =============================================================================
# Retry Logic
# =============================================================================

def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: tuple = (requests.RequestException, ConnectionError, TimeoutError)
) -> Callable:
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay cap in seconds
        exponential_base: Base for exponential calculation
        jitter: Add random jitter to prevent thundering herd
        retryable_exceptions: Tuple of exceptions to retry on
    """
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

                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)

                    # Add jitter (0-50% of delay)
                    if jitter:
                        delay = delay * (1 + random.random() * 0.5)

                    print(f"    Retry {attempt + 1}/{max_retries} for {func.__name__} after {delay:.1f}s: {e}")
                    time.sleep(delay)

            # All retries exhausted
            raise last_exception

        return wrapper
    return decorator

# =============================================================================
# Configuration (loaded from guide-config.json or environment variables)
# =============================================================================

# Chainstack RPC
RPC_URL = _CONFIG.get('RPC_URL', '')

# Solscan API
SOLSCAN_API = _CONFIG.get('SOLSCAN_API', 'https://pro-api.solscan.io/v2.0')
SOLSCAN_TOKEN = _CONFIG.get('SOLSCAN_TOKEN', '')

# MySQL
DB_CONFIG = {
    'host': _CONFIG.get('DB_HOST', 'localhost'),
    'port': _CONFIG.get('DB_PORT', 3396),
    'user': _CONFIG.get('DB_USER', 'root'),
    'password': _CONFIG.get('DB_PASSWORD', ''),
    'database': _CONFIG.get('DB_NAME', 't16o_db')
}

# Rate limiting
RPC_BATCH_SIZE = _CONFIG.get('RPC_BATCH_SIZE', 100)    # Max accounts per getMultipleAccounts call
RPC_DELAY = _CONFIG.get('RPC_DELAY', 0.2)              # Seconds between RPC calls
SOLSCAN_DELAY = _CONFIG.get('SOLSCAN_DELAY', 0.25)     # Seconds between Solscan calls (4 req/sec)

# Retry settings
RETRY_MAX_ATTEMPTS = _CONFIG.get('RETRY_MAX_ATTEMPTS', 3)
RETRY_BASE_DELAY = _CONFIG.get('RETRY_BASE_DELAY', 1.0)
RETRY_MAX_DELAY = _CONFIG.get('RETRY_MAX_DELAY', 30.0)

# =============================================================================
# Known Program IDs for Classification
# =============================================================================

PROGRAM_OWNERS = {
    # System
    '11111111111111111111111111111111': 'system',

    # Token Programs
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA': 'token_program',
    'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb': 'token_2022',
    'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL': 'ata_program',

    # DEX Programs (pools)
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8': 'raydium_amm',
    'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK': 'raydium_clmm',
    '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h': 'raydium_amm_v4',
    'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc': 'orca_whirlpool',
    '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP': 'orca_swap',
    'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo': 'meteora_dlmm',
    'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB': 'meteora_pools',
    '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P': 'pump_fun',
    'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4': 'jupiter_v6',
    'jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu': 'jupiter_limit',

    # Lending/Vault Programs
    'So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo': 'solend',
    'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA': 'marginfi',
    'KLend2g3cP87ber41GXWsSZQz8QbQzqcfaTibbeQ17E': 'kamino_lending',

    # NFT Programs
    'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s': 'metaplex',
    'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K': 'magic_eden',

    # Staking
    'Stake11111111111111111111111111111111111111': 'stake_program',
}

# Programs that indicate the account is a pool
POOL_PROGRAMS = {
    'raydium_amm', 'raydium_clmm', 'raydium_amm_v4',
    'orca_whirlpool', 'orca_swap',
    'meteora_dlmm', 'meteora_pools',
    'pump_fun'
}

# Programs that indicate vault/lending accounts
VAULT_PROGRAMS = {
    'solend', 'marginfi', 'kamino_lending'
}


# =============================================================================
# RPC Functions
# =============================================================================

@retry_with_backoff(
    max_retries=RETRY_MAX_ATTEMPTS,
    base_delay=RETRY_BASE_DELAY,
    max_delay=RETRY_MAX_DELAY
)
def rpc_get_multiple_accounts(session: requests.Session, addresses: List[str]) -> dict:
    """Fetch multiple accounts in one RPC call"""
    if not RPC_URL:
        raise ValueError("RPC_URL not configured. Set in guide-config.json or SOLANA_RPC_URL env var.")

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getMultipleAccounts",
        "params": [
            addresses,
            {"encoding": "base64", "commitment": "confirmed"}
        ]
    }

    response = session.post(RPC_URL, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def classify_from_rpc(account_info: dict, address: str) -> Tuple[str, str]:
    """
    Classify address type from RPC account info.
    Returns (address_type, confidence) where confidence is 'high', 'medium', or 'low'
    """
    if account_info is None:
        return ('unknown', 'high')  # Account doesn't exist

    owner = account_info.get('owner', '')
    executable = account_info.get('executable', False)
    lamports = account_info.get('lamports', 0)
    data = account_info.get('data', [])

    # Executable = definitely a program
    if executable:
        return ('program', 'high')

    # Get owner program type
    owner_type = PROGRAM_OWNERS.get(owner, 'unknown_program')

    # System Program owned with no data = wallet
    if owner_type == 'system':
        if isinstance(data, list) and len(data) > 0:
            data_bytes = base64.b64decode(data[0]) if data[0] else b''
            if len(data_bytes) == 0:
                return ('wallet', 'high')
            else:
                # System program account with data - could be nonce account
                return ('wallet', 'medium')
        return ('wallet', 'high')

    # Token Program owned
    if owner_type in ('token_program', 'token_2022'):
        if isinstance(data, list) and len(data) > 0:
            data_bytes = base64.b64decode(data[0]) if data[0] else b''

            # Mint accounts are 82 bytes
            if len(data_bytes) == 82:
                return ('mint', 'high')

            # Token accounts are 165 bytes
            if len(data_bytes) == 165:
                return ('ata', 'high')

            # Multisig accounts are 355 bytes
            if len(data_bytes) == 355:
                return ('wallet', 'medium')  # Multisig wallet

        return ('ata', 'medium')

    # ATA Program owned
    if owner_type == 'ata_program':
        return ('ata', 'high')

    # DEX/Pool programs
    if owner_type in POOL_PROGRAMS:
        return ('pool', 'high')

    # Vault/Lending programs
    if owner_type in VAULT_PROGRAMS:
        return ('vault', 'high')

    # Stake program
    if owner_type == 'stake_program':
        return ('wallet', 'medium')  # Stake accounts are user-owned

    # NFT programs
    if owner_type == 'metaplex':
        return ('mint', 'medium')  # Usually NFT metadata

    # Unknown program owner - needs Solscan verification
    return ('unknown', 'low')


# =============================================================================
# Solscan Functions
# =============================================================================

def _solscan_request(session: requests.Session, url: str, params: dict = None) -> requests.Response:
    """Internal helper for Solscan requests with retry logic"""
    if not SOLSCAN_TOKEN:
        raise ValueError("SOLSCAN_TOKEN not configured. Set in guide-config.json or SOLSCAN_API_TOKEN env var.")

    headers = {"token": SOLSCAN_TOKEN}
    response = session.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response


@retry_with_backoff(
    max_retries=RETRY_MAX_ATTEMPTS,
    base_delay=RETRY_BASE_DELAY,
    max_delay=RETRY_MAX_DELAY,
    retryable_exceptions=(requests.RequestException, ConnectionError, TimeoutError)
)
def solscan_get_account(session: requests.Session, address: str) -> Optional[dict]:
    """Get account info from Solscan API"""
    url = f"{SOLSCAN_API}/account/{address}"

    try:
        response = _solscan_request(session, url)
        # Don't retry on 4xx client errors (400, 404, etc.)
        if response.status_code >= 400 and response.status_code < 500:
            return None
        result = response.json()
        return result.get('data') if result.get('success') else None
    except requests.exceptions.HTTPError as e:
        # Don't retry 4xx errors
        if e.response is not None and e.response.status_code < 500:
            return None
        raise
    except ValueError as e:
        # Config error - don't retry
        print(f"    Config error: {e}")
        return None


@retry_with_backoff(
    max_retries=RETRY_MAX_ATTEMPTS,
    base_delay=RETRY_BASE_DELAY,
    max_delay=RETRY_MAX_DELAY,
    retryable_exceptions=(requests.RequestException, ConnectionError, TimeoutError)
)
def solscan_get_data_decoded(session: requests.Session, address: str) -> Optional[dict]:
    """Get decoded account data from Solscan API - reveals Pool structure etc."""
    url = f"{SOLSCAN_API}/account/data-decoded"
    params = {"address": address}

    try:
        response = _solscan_request(session, url, params)
        # Don't retry on 4xx client errors (400, 404, etc.)
        if response.status_code >= 400 and response.status_code < 500:
            return None
        result = response.json()
        # Return the full response as it contains data_decoded at top level
        return result if result.get('data_decoded') else None
    except requests.exceptions.HTTPError as e:
        # Don't retry 4xx errors
        if e.response is not None and e.response.status_code < 500:
            return None
        raise
    except ValueError as e:
        # Config error - don't retry
        print(f"    Config error: {e}")
        return None


@retry_with_backoff(
    max_retries=RETRY_MAX_ATTEMPTS,
    base_delay=RETRY_BASE_DELAY,
    max_delay=RETRY_MAX_DELAY,
    retryable_exceptions=(requests.RequestException, ConnectionError, TimeoutError)
)
def solscan_get_metadata(session: requests.Session, address: str) -> Optional[dict]:
    """Get account metadata from Solscan API - labels, tags, funding info"""
    url = f"{SOLSCAN_API}/account/metadata"
    params = {"address": address}

    try:
        response = _solscan_request(session, url, params)
        # Don't retry on 4xx client errors (400, 404, etc.)
        if response.status_code >= 400 and response.status_code < 500:
            return None
        result = response.json()
        return result.get('data') if result.get('success') else None
    except requests.exceptions.HTTPError as e:
        # Don't retry 4xx errors
        if e.response is not None and e.response.status_code < 500:
            return None
        raise
    except ValueError as e:
        # Config error - don't retry
        print(f"    Config error: {e}")
        return None


def extract_pool_data(decoded: dict) -> Optional[dict]:
    """Extract pool structure from decoded account data"""
    if not decoded or not decoded.get('data_decoded'):
        return None

    dd = decoded['data_decoded']
    if dd.get('name') != 'Pool':
        return None

    data = dd.get('data', {})

    pool_info = {
        'base_mint': data.get('base_mint', {}).get('data'),
        'quote_mint': data.get('quote_mint', {}).get('data'),
        'lp_mint': data.get('lp_mint', {}).get('data'),
        'pool_base_token_account': data.get('pool_base_token_account', {}).get('data'),
        'pool_quote_token_account': data.get('pool_quote_token_account', {}).get('data'),
        'creator': data.get('creator', {}).get('data'),
        'coin_creator': data.get('coin_creator', {}).get('data'),
        'lp_supply': data.get('lp_supply', {}).get('data'),
    }

    # Only return if we got meaningful data
    if pool_info['base_mint'] or pool_info['quote_mint']:
        return pool_info

    return None


def classify_from_solscan(account_data: dict) -> str:
    """Classify address type from Solscan account data"""
    if not account_data:
        return 'unknown'

    account_type = account_data.get('account_type', '').lower()

    # Direct mappings
    type_map = {
        'system_account': 'wallet',
        'token_account': 'ata',
        'mint': 'mint',
        'program': 'program',
        'amm': 'pool',
        'liquidity_pool': 'pool',
        'pool': 'pool',
        'vault': 'vault',
        'stake': 'wallet',
        'nft': 'mint',
    }

    for key, value in type_map.items():
        if key in account_type:
            return value

    # Check if it's a known program
    if account_data.get('executable'):
        return 'program'

    # Check lamports and owner
    owner = account_data.get('owner', '')
    if owner == '11111111111111111111111111111111':
        return 'wallet'

    return 'unknown'


# =============================================================================
# Main Classification Logic
# =============================================================================

def process_batch_rpc(session: requests.Session, addresses: List[str]) -> Dict[str, Tuple[str, str]]:
    """Process a batch of addresses via RPC, return classifications"""
    results = {}

    try:
        response = rpc_get_multiple_accounts(session, addresses)

        if 'error' in response:
            print(f"    RPC error: {response['error']}")
            return {addr: ('unknown', 'low') for addr in addresses}

        accounts = response.get('result', {}).get('value', [])

        for i, account_info in enumerate(accounts):
            if i < len(addresses):
                addr = addresses[i]
                addr_type, confidence = classify_from_rpc(account_info, addr)
                results[addr] = (addr_type, confidence)

    except Exception as e:
        print(f"    Batch RPC error: {e}")
        return {addr: ('unknown', 'low') for addr in addresses}

    return results


def run_classification(addresses: List[str], dry_run: bool = False, enrich_pools: bool = True) -> Tuple[Dict[str, str], Dict[str, dict]]:
    """
    Run full classification pipeline:
    1. RPC batch classification
    2. Solscan /account for low-confidence results
    3. Solscan /account/data-decoded for unknowns (detect Pools)
    4. Solscan /account/metadata for detected pools (get labels)

    Returns (classifications, pool_data) where pool_data has decoded pool info
    """

    print(f"\n{'='*70}")
    print(f"ADDRESS CLASSIFIER - Processing {len(addresses)} addresses")
    print(f"{'='*70}")
    print(f"Dry run: {dry_run}")
    print(f"Enrich pools: {enrich_pools}")
    print(f"RPC batch size: {RPC_BATCH_SIZE}")
    print()

    rpc_session = requests.Session()
    solscan_session = requests.Session()

    all_results = {}
    low_confidence = []
    pool_data = {}  # address -> {pool_info, metadata}

    # ==========================================================================
    # Phase 1: RPC Batch Classification
    # ==========================================================================
    print(f"PHASE 1: RPC Batch Classification")
    print(f"-" * 50)

    total_batches = (len(addresses) + RPC_BATCH_SIZE - 1) // RPC_BATCH_SIZE

    for batch_num in range(total_batches):
        start_idx = batch_num * RPC_BATCH_SIZE
        end_idx = min(start_idx + RPC_BATCH_SIZE, len(addresses))
        batch = addresses[start_idx:end_idx]

        batch_results = process_batch_rpc(rpc_session, batch)

        for addr, (addr_type, confidence) in batch_results.items():
            all_results[addr] = addr_type
            if confidence == 'low':
                low_confidence.append(addr)

        if (batch_num + 1) % 10 == 0 or batch_num == total_batches - 1:
            print(f"  Processed {end_idx}/{len(addresses)} addresses...")

        time.sleep(RPC_DELAY)

    # Summary of Phase 1
    phase1_counts = defaultdict(int)
    for addr_type in all_results.values():
        phase1_counts[addr_type] += 1

    print(f"\nPhase 1 Results:")
    for t, c in sorted(phase1_counts.items(), key=lambda x: -x[1]):
        print(f"  {t}: {c}")
    print(f"  Low confidence (needs Solscan): {len(low_confidence)}")

    # ==========================================================================
    # Phase 2: Solscan /account for Low-Confidence Results
    # ==========================================================================
    if low_confidence:
        print(f"\nPHASE 2: Solscan /account Verification ({len(low_confidence)} addresses)")
        print(f"-" * 50)

        for i, addr in enumerate(low_confidence):
            account_data = solscan_get_account(solscan_session, addr)
            new_type = classify_from_solscan(account_data)

            if new_type != 'unknown':
                all_results[addr] = new_type

            if (i + 1) % 50 == 0:
                print(f"  Verified {i + 1}/{len(low_confidence)} addresses...")

            time.sleep(SOLSCAN_DELAY)

        print(f"  Completed Solscan verification")

    # ==========================================================================
    # Phase 3: Solscan /account/data-decoded for remaining unknowns
    # ==========================================================================
    still_unknown = [addr for addr, t in all_results.items() if t == 'unknown']

    if still_unknown and enrich_pools:
        print(f"\nPHASE 3: Solscan /account/data-decoded ({len(still_unknown)} unknowns)")
        print(f"-" * 50)

        pools_found = 0
        for i, addr in enumerate(still_unknown):
            decoded = solscan_get_data_decoded(solscan_session, addr)

            if decoded:
                pool_info = extract_pool_data(decoded)
                if pool_info:
                    all_results[addr] = 'pool'
                    pool_data[addr] = {'pool_info': pool_info, 'metadata': None}
                    pools_found += 1
                    print(f"    [{i+1}] {addr[:20]}... -> Pool detected!")

            if (i + 1) % 50 == 0:
                print(f"  Checked {i + 1}/{len(still_unknown)} addresses...")

            time.sleep(SOLSCAN_DELAY)

        print(f"  Found {pools_found} pools via data-decoded")

    # ==========================================================================
    # Phase 4: Solscan /account/metadata for detected pools (get labels)
    # ==========================================================================
    # Also include pools classified in Phase 1 that need enrichment
    all_pools = [addr for addr, t in all_results.items() if t == 'pool']

    if all_pools and enrich_pools:
        print(f"\nPHASE 4: Solscan /account/metadata for {len(all_pools)} pools")
        print(f"-" * 50)

        for i, addr in enumerate(all_pools):
            # Get metadata
            metadata = solscan_get_metadata(solscan_session, addr)

            if addr in pool_data:
                pool_data[addr]['metadata'] = metadata
            else:
                # Pool from Phase 1 - need to get decoded data too
                decoded = solscan_get_data_decoded(solscan_session, addr)
                pool_info = extract_pool_data(decoded) if decoded else None
                pool_data[addr] = {'pool_info': pool_info, 'metadata': metadata}
                time.sleep(SOLSCAN_DELAY)

            if metadata and metadata.get('account_label'):
                print(f"    [{i+1}] {addr[:16]}... -> {metadata['account_label'][:40]}")

            if (i + 1) % 20 == 0:
                print(f"  Enriched {i + 1}/{len(all_pools)} pools...")

            time.sleep(SOLSCAN_DELAY)

        print(f"  Completed pool enrichment")

    # ==========================================================================
    # Final Summary
    # ==========================================================================
    print(f"\n{'='*70}")
    print(f"FINAL CLASSIFICATION RESULTS")
    print(f"{'='*70}")

    final_counts = defaultdict(int)
    for addr_type in all_results.values():
        final_counts[addr_type] += 1

    for t, c in sorted(final_counts.items(), key=lambda x: -x[1]):
        pct = (c / len(addresses)) * 100
        print(f"  {t:15} : {c:6} ({pct:5.1f}%)")

    print(f"\n  Pools with decoded data: {len([p for p in pool_data.values() if p.get('pool_info')])}")
    print(f"  Pools with labels: {len([p for p in pool_data.values() if p.get('metadata', {}).get('account_label')])}")

    return all_results, pool_data


def update_pool_in_database(cursor, conn, pool_address: str, pool_data: dict, metadata: dict = None):
    """Update tx_pool with decoded pool structure data"""

    # Ensure pool address exists
    cursor.execute("SELECT id FROM tx_address WHERE address = %s", (pool_address,))
    row = cursor.fetchone()
    if not row:
        cursor.execute(
            "INSERT INTO tx_address (address, address_type) VALUES (%s, 'pool')",
            (pool_address,)
        )
        conn.commit()
        cursor.execute("SELECT id FROM tx_address WHERE address = %s", (pool_address,))
        row = cursor.fetchone()

    pool_address_id = row[0]

    # Helper to ensure address and get ID
    def ensure_addr(address: str, addr_type: str = 'unknown') -> Optional[int]:
        if not address:
            return None
        cursor.execute("SELECT id FROM tx_address WHERE address = %s", (address,))
        r = cursor.fetchone()
        if r:
            return r[0]
        cursor.execute(
            "INSERT INTO tx_address (address, address_type) VALUES (%s, %s)",
            (address, addr_type)
        )
        conn.commit()
        return cursor.lastrowid

    # Helper to ensure token exists
    def ensure_token(mint_address: str) -> Optional[int]:
        if not mint_address:
            return None
        mint_id = ensure_addr(mint_address, 'mint')
        cursor.execute("SELECT id FROM tx_token WHERE mint_address_id = %s", (mint_id,))
        r = cursor.fetchone()
        if r:
            return r[0]
        cursor.execute("INSERT INTO tx_token (mint_address_id) VALUES (%s)", (mint_id,))
        conn.commit()
        return cursor.lastrowid

    # Get IDs for pool components
    token1_id = ensure_token(pool_data.get('base_mint'))
    token2_id = ensure_token(pool_data.get('quote_mint'))
    lp_token_id = ensure_token(pool_data.get('lp_mint'))
    token_account1_id = ensure_addr(pool_data.get('pool_base_token_account'), 'vault')
    token_account2_id = ensure_addr(pool_data.get('pool_quote_token_account'), 'vault')

    # Get pool label from metadata if available
    pool_label = None
    if metadata:
        pool_label = metadata.get('account_label')

    # Check if pool exists
    cursor.execute("SELECT id FROM tx_pool WHERE pool_address_id = %s", (pool_address_id,))
    existing = cursor.fetchone()

    if existing:
        # Overwrite with fresh API data (more accurate than existing)
        cursor.execute("""
            UPDATE tx_pool SET
                token1_id = COALESCE(%s, token1_id),
                token2_id = COALESCE(%s, token2_id),
                lp_token_id = COALESCE(%s, lp_token_id),
                token_account1_id = COALESCE(%s, token_account1_id),
                token_account2_id = COALESCE(%s, token_account2_id),
                pool_label = COALESCE(%s, pool_label),
                attempt_cnt = 0
            WHERE id = %s
        """, (token1_id, token2_id, lp_token_id, token_account1_id, token_account2_id,
              pool_label, existing[0]))
    else:
        cursor.execute("""
            INSERT INTO tx_pool (pool_address_id, token1_id, token2_id, lp_token_id,
                                 token_account1_id, token_account2_id, pool_label)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (pool_address_id, token1_id, token2_id, lp_token_id,
              token_account1_id, token_account2_id, pool_label))

    conn.commit()


def update_database(classifications: Dict[str, str], pool_enrichments: Dict[str, dict], dry_run: bool = False):
    """Update tx_address.address_type and tx_pool in MySQL"""

    print(f"\n{'='*70}")
    print(f"DATABASE UPDATE")
    print(f"{'='*70}")

    if dry_run:
        print("DRY RUN - No database changes will be made")

        # Show what would change
        changes = defaultdict(lambda: defaultdict(int))
        for addr, new_type in classifications.items():
            changes[new_type]['count'] += 1

        print("\nProposed address type changes:")
        for new_type, stats in sorted(changes.items(), key=lambda x: -x[1]['count']):
            print(f"  -> {new_type}: {stats['count']} addresses")

        pools_with_data = len([p for p in pool_enrichments.values() if p.get('pool_info')])
        pools_with_labels = len([p for p in pool_enrichments.values() if p.get('metadata', {}).get('account_label')])
        print(f"\nProposed pool enrichments:")
        print(f"  -> {pools_with_data} pools would get decoded data")
        print(f"  -> {pools_with_labels} pools would get labels")
        return

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Group by new type for batch updates
    by_type = defaultdict(list)
    for addr, new_type in classifications.items():
        by_type[new_type].append(addr)

    total_updated = 0

    for new_type, addrs in by_type.items():
        if new_type == 'unknown':
            continue  # Don't update unknowns

        # Batch update in chunks of 1000
        for i in range(0, len(addrs), 1000):
            chunk = addrs[i:i+1000]
            placeholders = ','.join(['%s'] * len(chunk))

            cursor.execute(f"""
                UPDATE tx_address
                SET address_type = %s
                WHERE address IN ({placeholders})
                  AND address_type != %s
            """, [new_type] + chunk + [new_type])

            total_updated += cursor.rowcount

    conn.commit()
    print(f"\nUpdated {total_updated} addresses in tx_address")

    # Update pool enrichments
    if pool_enrichments:
        print(f"\nUpdating {len(pool_enrichments)} pools with enriched data...")
        pools_updated = 0

        for pool_addr, data in pool_enrichments.items():
            pool_info = data.get('pool_info')
            metadata = data.get('metadata')

            if pool_info or metadata:
                try:
                    update_pool_in_database(cursor, conn, pool_addr, pool_info or {}, metadata)
                    pools_updated += 1
                except Exception as e:
                    print(f"    Error updating pool {pool_addr[:16]}...: {e}")

        print(f"  Updated {pools_updated} pools in tx_pool")

    cursor.close()
    conn.close()


def generate_report(classifications: Dict[str, str], output_file: str):
    """Generate classification report"""

    report = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'total_addresses': len(classifications),
        'summary': defaultdict(int),
        'by_type': defaultdict(list)
    }

    for addr, addr_type in classifications.items():
        report['summary'][addr_type] += 1
        report['by_type'][addr_type].append(addr)

    report['summary'] = dict(report['summary'])
    report['by_type'] = {k: v for k, v in report['by_type'].items()}

    with open(output_file, 'w') as f:
        json.dump(report, f, indent=2)

    print(f"\nReport saved to: {output_file}")


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Classify and update tx_address types')
    parser.add_argument('--file', default='suspect_wallets_for_rpc_check.txt',
                        help='Input file with addresses (one per line)')
    parser.add_argument('--from-db', action='store_true',
                        help='Load unknown addresses from tx_address table instead of file')
    parser.add_argument('--scan-suspects', action='store_true',
                        help='Find and fix misclassified wallets based on table signals')
    parser.add_argument('--limit', type=int, default=0,
                        help='Limit number of addresses to process (0 = all)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview classifications without updating database')
    parser.add_argument('--no-enrich-pools', action='store_true',
                        help='Skip pool enrichment phases (data-decoded, metadata)')
    parser.add_argument('--report', default='classification_report.json',
                        help='Output report file')
    parser.add_argument('--db-host', default=DB_CONFIG['host'])
    parser.add_argument('--db-port', type=int, default=DB_CONFIG['port'])
    parser.add_argument('--db-user', default=DB_CONFIG['user'])
    parser.add_argument('--db-pass', default=DB_CONFIG['password'])
    parser.add_argument('--db-name', default=DB_CONFIG['database'])

    args = parser.parse_args()

    # Update DB config
    DB_CONFIG.update({
        'host': args.db_host,
        'port': args.db_port,
        'user': args.db_user,
        'password': args.db_pass,
        'database': args.db_name
    })

    # Scan suspects mode - fix obvious misclassifications via SQL
    if args.scan_suspects:
        print(f"SCANNING FOR MISCLASSIFIED WALLETS")
        print(f"{'='*70}")

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        fixes = [
            ("wallet in tx_pool -> pool", """
                UPDATE tx_address a
                JOIN tx_pool p ON p.pool_address_id = a.id
                SET a.address_type = 'pool'
                WHERE a.address_type = 'wallet'
            """),
            ("wallet in tx_token -> mint", """
                UPDATE tx_address a
                JOIN tx_token t ON t.mint_address_id = a.id
                SET a.address_type = 'mint'
                WHERE a.address_type = 'wallet'
            """),
            ("wallet in tx_program -> program", """
                UPDATE tx_address a
                JOIN tx_program p ON p.program_address_id = a.id
                SET a.address_type = 'program'
                WHERE a.address_type = 'wallet'
            """),
            ("wallet as pool vault -> vault", """
                UPDATE tx_address a
                SET a.address_type = 'vault'
                WHERE a.address_type = 'wallet'
                  AND (a.id IN (SELECT token_account1_id FROM tx_pool WHERE token_account1_id IS NOT NULL)
                    OR a.id IN (SELECT token_account2_id FROM tx_pool WHERE token_account2_id IS NOT NULL))
            """),
            ("wallet ending in 'pump' -> mint", """
                UPDATE tx_address
                SET address_type = 'mint'
                WHERE address_type = 'wallet'
                  AND address LIKE '%pump'
            """),
        ]

        # Count queries for dry-run mode
        count_queries = [
            "SELECT COUNT(*) FROM tx_address a JOIN tx_pool p ON p.pool_address_id = a.id WHERE a.address_type = 'wallet'",
            "SELECT COUNT(*) FROM tx_address a JOIN tx_token t ON t.mint_address_id = a.id WHERE a.address_type = 'wallet'",
            "SELECT COUNT(*) FROM tx_address a JOIN tx_program p ON p.program_address_id = a.id WHERE a.address_type = 'wallet'",
            """SELECT COUNT(*) FROM tx_address a WHERE a.address_type = 'wallet'
               AND (a.id IN (SELECT token_account1_id FROM tx_pool WHERE token_account1_id IS NOT NULL)
                 OR a.id IN (SELECT token_account2_id FROM tx_pool WHERE token_account2_id IS NOT NULL))""",
            "SELECT COUNT(*) FROM tx_address WHERE address_type = 'wallet' AND address LIKE '%pump'",
        ]

        total_fixed = 0
        for i, (desc, sql) in enumerate(fixes):
            if args.dry_run:
                cursor.execute(count_queries[i])
                count = cursor.fetchone()[0]
                print(f"  [DRY] Would fix {count}: {desc}")
            else:
                cursor.execute(sql)
                conn.commit()
                print(f"  Fixed {cursor.rowcount}: {desc}")
                total_fixed += cursor.rowcount

        if not args.dry_run:
            print(f"\nTotal fixed: {total_fixed}")

        # Find remaining suspects for full classification
        cursor.execute("""
            SELECT a.address
            FROM tx_address a
            WHERE a.address_type = 'wallet'
              AND (SELECT COUNT(*) FROM tx_address fa WHERE fa.funded_by_address_id = a.id) >= 10
        """)
        high_funders = [row[0] for row in cursor.fetchall()]

        cursor.close()
        conn.close()

        if high_funders:
            print(f"\nFound {len(high_funders)} high-funder wallets (funded 10+ addresses)")
            print("These may be exchanges/services - run full classification to verify")

            # If not dry-run and we want to continue with classification
            if not args.dry_run and args.limit > 0:
                addresses = high_funders[:args.limit]
                print(f"Continuing with classification of {len(addresses)} addresses...")
            else:
                return
        else:
            print("\nNo remaining suspects found!")
            return

    # Load addresses
    elif args.from_db:
        print(f"Loading unknown addresses from tx_address table...")
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        query = "SELECT address FROM tx_address WHERE address_type = 'unknown'"
        if args.limit > 0:
            query += f" LIMIT {args.limit}"
        cursor.execute(query)
        addresses = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        print(f"Loaded {len(addresses)} unknown addresses from database")
    else:
        print(f"Loading addresses from: {args.file}")
        with open(args.file, 'r') as f:
            addresses = [line.strip() for line in f if line.strip()]

        print(f"Loaded {len(addresses)} addresses")

        if args.limit > 0:
            addresses = addresses[:args.limit]
            print(f"Limited to first {args.limit} addresses")

    if not addresses:
        print("No addresses to process!")
        return

    # Run classification
    enrich_pools = not args.no_enrich_pools
    classifications, pool_data = run_classification(addresses, args.dry_run, enrich_pools)

    # Update database
    update_database(classifications, pool_data, args.dry_run)

    # Generate report
    generate_report(classifications, args.report)

    print(f"\n{'='*70}")
    print("CLASSIFICATION COMPLETE")
    print(f"{'='*70}")


if __name__ == '__main__':
    main()
