#!/usr/bin/env python3
"""
Guide Funder - Funding wallet discovery daemon

Consumes address batches from tx.guide.addresses queue, fetches the first
transactions for each address, identifies the funding wallet, and writes
directly to the database.

Pipeline position:
    guide-shredder.py → tx.guide.addresses queue
                             ↓
                    guide-funder.py (this script)
                             ↓
                    tx_address.funded_by_address_id

Flow:
1. Prefetch configurable messages (default 50) from tx.guide.addresses
2. Dedupe addresses across all messages
3. Filter out already-processed addresses (init_tx_fetched = 1)
4. For each address, fetch first transactions via Solscan /account/transfer
5. Find the first SOL inflow to identify the funding wallet
6. Write funding info directly to tx_address table
7. Mark addresses as processed, ACK messages

Usage:
    python guide-funder.py
    python guide-funder.py --prefetch 100
    python guide-funder.py --batch-delay 2  # 2 second delay between batches
    python guide-funder.py --dry-run
    python guide-funder.py --address-file wallets.txt  # Read from local file instead of queue
    python guide-funder.py --sync-db-missing  # Query DB for missing funders
    python guide-funder.py --sync-db-missing --limit 500  # Process max 500 addresses
    python guide-funder.py --queue-consumer  # Gateway mode (t16o_mq vhost)
"""

import argparse
import json
import random
import re
import time
import sys
from typing import Optional, Dict, List, Set, Tuple
from collections import defaultdict

# HTTP client
import requests

# MySQL connector
try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False
    MySQLError = Exception  # Fallback

# RabbitMQ
try:
    import pika
    HAS_PIKA = True
except ImportError:
    HAS_PIKA = False


# =============================================================================
# Configuration (from guide-config.json)
# =============================================================================

import os

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'guide-config.json')
    try:
        with open(config_path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

_cfg = load_config()

SOLSCAN_API_BASE = _cfg.get('SOLSCAN_API', "https://pro-api.solscan.io/v2.0")
SOLSCAN_API_TOKEN = _cfg.get('SOLSCAN_TOKEN', "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjcmVhdGVkQXQiOjE3NTg0MzQzODI5MTEsImVtYWlsIjoid2dhZGJlcnJ5QGdtYWlsLmNvbSIsImFjdGlvbiI6InRva2VuLWFwaSIsImFwaVZlcnNpb24iOiJ2MiIsImlhdCI6MTc1ODQzNDM4Mn0.d_ZYyfZGaFdLfYXsuB_M2rT_Q6WkwpxVp0ZqNE_zUAk")

# RabbitMQ (legacy queue)
RABBITMQ_HOST = _cfg.get('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = _cfg.get('RABBITMQ_PORT', 5692)
RABBITMQ_USER = _cfg.get('RABBITMQ_USER', 'admin')
RABBITMQ_PASS = _cfg.get('RABBITMQ_PASSWORD', 'admin123')
RABBITMQ_QUEUE_IN = 'tx.guide.addresses'

# Gateway RabbitMQ (t16o_mq vhost)
RABBITMQ_VHOST = _cfg.get('RABBITMQ_VHOST', 't16o_mq')
RABBITMQ_REQUEST_QUEUE = 'mq.guide.funder.request'
RABBITMQ_RESPONSE_QUEUE = 'mq.guide.funder.response'

# Database configuration (standardized across all workers)
DB_CONFIG = {
    'host': _cfg.get('DB_HOST', '127.0.0.1'),
    'port': _cfg.get('DB_PORT', 3396),
    'user': _cfg.get('DB_USER', 'root'),
    'password': _cfg.get('DB_PASSWORD', 'rootpassword'),
    'database': _cfg.get('DB_NAME', 't16o_db'),
    'ssl_disabled': True,
    'use_pure': True,
    'ssl_verify_cert': False,
    'ssl_verify_identity': False,
    'autocommit': True,  # Prevent table locks when idle
}

# Deadlock retry settings
MAX_DEADLOCK_RETRIES = 5
DEADLOCK_BASE_DELAY = 0.1


def execute_with_deadlock_retry(cursor, conn, query, params=None,
                                 max_retries=MAX_DEADLOCK_RETRIES):
    """Execute a query with deadlock retry logic. Returns rowcount."""
    for attempt in range(max_retries):
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor.rowcount
        except mysql.connector.Error as e:
            if e.errno in (1213, 1205) and attempt < max_retries - 1:
                conn.rollback()
                delay = DEADLOCK_BASE_DELAY * (2 ** attempt) + random.uniform(0, 0.1)
                print(f"    [!] Deadlock (attempt {attempt + 1}/{max_retries}), retrying in {delay:.2f}s...")
                time.sleep(delay)
            else:
                raise
    return 0


# SOL token addresses for funding detection
SOL_TOKEN = 'So11111111111111111111111111111111111111111'
SOL_TOKEN_2 = 'So11111111111111111111111111111111111111112'

# Known program addresses to skip
KNOWN_PROGRAMS = {
    '11111111111111111111111111111111',  # System Program
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',  # Token Program
    'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',  # Associated Token Program
    'ComputeBudget111111111111111111111111111111',  # Compute Budget
    'So11111111111111111111111111111111111111111',  # Wrapped SOL (not a program but skip)
    'So11111111111111111111111111111111111111112',  # SOL Mint variant
}

# Address prefixes to skip (system/MEV addresses that won't have funders)
SKIP_PREFIXES = (
    'jitodontfront',  # Jito tip accounts
    'Jito',           # Jito-related
)

# Rate limiting
API_DELAY = 0.30  # seconds between API calls


def get_db_batch_size(cursor) -> int:
    """
    Get batch size from database config table using sp_config_get.
    This is runtime_editable, so should be checked each iteration.

    Args:
        cursor: Database cursor (dictionary=True)

    Returns:
        int: The configured batch size (or default_value from proc)
    """
    try:
        cursor.execute("CALL sp_config_get(%s, %s)", ('batch', 'funder_batch_size'))
        result = cursor.fetchone()
        # Consume any additional result sets from the stored procedure
        try:
            while cursor.nextset():
                pass
        except:
            pass

        if result:
            # Use config_value if set, otherwise fall back to default_value from proc
            value = result.get('config_value') or result.get('default_value')
            if value:
                return int(value)
        return 10  # Fallback if proc returns nothing
    except Exception as e:
        print(f"    [!] Failed to get batch size from config: {e}")
        return 10


def should_skip_address(address: str) -> bool:
    """Check if address should be skipped (programs, system addresses, etc.)"""
    if address in KNOWN_PROGRAMS:
        return True
    for prefix in SKIP_PREFIXES:
        if address.startswith(prefix):
            return True
    return False


# =============================================================================
# Solscan API Client
# =============================================================================

class SolscanClient:
    """Client for Solscan Pro API"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"token": SOLSCAN_API_TOKEN})

    def close(self):
        self.session.close()

    def get_account_metadata(self, address: str) -> Optional[Dict]:
        """
        Get account metadata including funder info, label, tags, and type.
        /account/metadata - single call for comprehensive account info

        Returns dict with:
        - account_address: The address
        - account_label: Human-readable name (e.g., "Raydium (WSOL-USDC) Pool 1")
        - account_tags: Array of tags (e.g., ["raydium", "pool"])
        - account_type: "address", "token_account", "mint"
        - funded_by: {funded_by, tx_hash, block_time}
        - active_age: Days since first activity
        """
        url = f"{SOLSCAN_API_BASE}/account/metadata"
        params = {"address": address}

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            result = response.json()
            if result.get('success') and result.get('data'):
                return result['data']
            return None
        except Exception as e:
            print(f"    [!] account/metadata error for {address[:12]}...: {e}")
            return None

    def get_account_metadata_multi(self, addresses: List[str], limit: int = 50, debug: bool = False) -> Dict[str, Dict]:
        """
        Get account metadata for multiple addresses in a single call.
        /account/metadata/multi - batch endpoint using address[] query params

        Args:
            addresses: List of addresses
            limit: Max addresses per call (default 50, Solscan API max)
            debug: If True, print detailed API response info

        Returns dict mapping address -> metadata dict
        """
        if not addresses:
            return {}

        # Apply limit
        addresses = addresses[:limit]

        # Build URL with address[] params
        url = f"{SOLSCAN_API_BASE}/account/metadata/multi"
        params = [('address[]', addr) for addr in addresses]

        try:
            response = self.session.get(url, params=params, timeout=60)
            if debug:
                print(f"      [DEBUG] API status: {response.status_code}")
            response.raise_for_status()
            result = response.json()

            if debug:
                print(f"      [DEBUG] API success: {result.get('success')}, data count: {len(result.get('data', []))}")
                if result.get('data') and len(result['data']) > 0:
                    sample = result['data'][0]
                    print(f"      [DEBUG] Sample keys: {list(sample.keys())}")
                    print(f"      [DEBUG] Sample account_tags: {sample.get('account_tags')}")

            # Build address -> metadata map
            metadata_map = {}
            if result.get('success') and result.get('data'):
                for item in result['data']:
                    addr = item.get('account_address')
                    if addr:
                        metadata_map[addr] = item
            elif debug:
                print(f"      [DEBUG] API response: {result}")

            return metadata_map
        except Exception as e:
            print(f"    [!] account/metadata/multi error: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def get_account_transfers(self, address: str, page_size: int = 20, token_filter: str = None) -> Optional[Dict]:
        """
        Get transfer history for a wallet address.
        /account/transfer - for wallet accounts
        Note: Solscan only accepts page_size of 10, 20, 30, 40, 60, or 100

        Args:
            token_filter: Optional token address to filter by (e.g., SOL_TOKEN for SOL-only)
        """
        # Solscan only accepts specific page_size values
        valid_sizes = [10, 20, 30, 40, 60, 100]
        actual_size = min([s for s in valid_sizes if s >= page_size], default=100)

        url = f"{SOLSCAN_API_BASE}/account/transfer"
        params = {
            "address": address,
            "page": 1,
            "page_size": actual_size,
            "sort_by": "block_time",
            "sort_order": "asc"
        }
        if token_filter:
            params["token"] = token_filter

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"    [!] account/transfer error for {address[:12]}...: {e}")
            return None

    def get_token_transfers(self, address: str, page_size: int = 20) -> Optional[Dict]:
        """
        Get transfer history for a token mint address.
        /token/transfer - for token mints
        Note: Solscan only accepts page_size of 10, 20, 30, 40, 60, or 100
        """
        valid_sizes = [10, 20, 30, 40, 60, 100]
        actual_size = min([s for s in valid_sizes if s >= page_size], default=100)

        url = f"{SOLSCAN_API_BASE}/token/transfer"
        params = {
            "address": address,
            "page": 1,
            "page_size": actual_size,
            "sort_by": "block_time",
            "sort_order": "asc"
        }
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"    [!] token/transfer error for {address[:12]}...: {e}")
            return None

    def get_token_defi_activities(self, address: str, page_size: int = 20) -> Optional[Dict]:
        """
        Get DeFi activities for a token mint address.
        /token/defi/activities - for token swaps, liquidity adds, etc.
        Note: Solscan only accepts page_size of 10, 20, 30, 40, 60, or 100
        """
        valid_sizes = [10, 20, 30, 40, 60, 100]
        actual_size = min([s for s in valid_sizes if s >= page_size], default=100)

        url = f"{SOLSCAN_API_BASE}/token/defi/activities"
        params = {
            "address": address,
            "page": 1,
            "page_size": actual_size,
            "sort_by": "block_time",
            "sort_order": "asc"
        }
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"    [!] token/defi/activities error for {address[:12]}...: {e}")
            return None

    def find_funding_wallet(self, target_address: str, transfers_data: Dict) -> Optional[Dict]:
        """
        Find the funding wallet from transfer history.
        Looks for the first SOL inflow to identify who funded the address.

        Returns dict with: funder, signature, amount, block_time
        """
        if not transfers_data.get('success') or not transfers_data.get('data'):
            return None

        for record in transfers_data['data']:
            # Look for SOL transfers INTO the target address
            token_addr = record.get('token_address', '')
            flow = record.get('flow', '')
            to_addr = record.get('to_address', '')
            from_addr = record.get('from_address', '')

            # Check if this is a SOL inflow to target
            is_sol = token_addr in (SOL_TOKEN, SOL_TOKEN_2)
            is_inflow = flow == 'in' or to_addr == target_address

            if is_sol and is_inflow and from_addr and from_addr != target_address:
                return {
                    'funder': from_addr,
                    'signature': record.get('trans_id'),
                    'amount': record.get('amount', 0),
                    'block_time': record.get('block_time')
                }

        return None


# =============================================================================
# Funding Detection Helper
# =============================================================================

def extract_signatures_from_account_transfer(data: Dict) -> Set[str]:
    """Extract transaction signatures from /account/transfer response"""
    signatures = set()
    if not data.get('success') or not data.get('data'):
        return signatures

    for record in data['data']:
        if sig := record.get('trans_id'):
            signatures.add(sig)

    return signatures


def extract_signatures_from_token_transfer(data: Dict) -> Set[str]:
    """Extract transaction signatures from /token/transfer response"""
    signatures = set()
    if not data.get('success') or not data.get('data'):
        return signatures

    for record in data['data']:
        if sig := record.get('trans_id'):
            signatures.add(sig)

    return signatures


def extract_signatures_from_defi_activities(data: Dict) -> Set[str]:
    """Extract transaction signatures from /token/defi/activities response"""
    signatures = set()
    if not data.get('success') or not data.get('data'):
        return signatures

    for record in data['data']:
        if sig := record.get('trans_id'):
            signatures.add(sig)

    return signatures


# =============================================================================
# Gateway Integration
# =============================================================================

def setup_gateway_rabbitmq():
    """Setup RabbitMQ connection to t16o_mq vhost for gateway integration"""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            virtual_host=RABBITMQ_VHOST,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )
    )
    channel = connection.channel()

    # Declare request and response queues (must match gateway's queue args)
    channel.queue_declare(queue=RABBITMQ_REQUEST_QUEUE, durable=True,
                          arguments={'x-max-priority': 10})
    channel.queue_declare(queue=RABBITMQ_RESPONSE_QUEUE, durable=True,
                          arguments={'x-max-priority': 10})

    return connection, channel


def publish_response(channel, request_id: str, status: str, result: Dict,
                     correlation_id: str = None, request_log_id: int = None):
    """Publish response message to gateway response queue"""
    from datetime import datetime

    response = {
        'request_id': request_id,
        'correlation_id': correlation_id or request_id,
        'request_log_id': request_log_id,  # For billing linkage
        'worker': 'funder',
        'status': status,
        'timestamp': datetime.now().isoformat() + 'Z',
        'result': result
    }

    channel.basic_publish(
        exchange='',
        routing_key=RABBITMQ_RESPONSE_QUEUE,
        body=json.dumps(response),
        properties=pika.BasicProperties(
            delivery_mode=2,  # persistent
            content_type='application/json'
        )
    )


# =============================================================================
# Daemon Request Logging (for billing)
# =============================================================================

def log_worker_request(cursor, conn, request_id: str, correlation_id: str,
                       worker: str, action: str, batch_num: int = 0,
                       batch_size: int = 0, priority: int = 5,
                       api_key_id: int = None) -> int:
    """
    Create a request_log entry for worker activity.
    Returns the request_log.id for tracking (existing or newly created).
    """
    # Check if record already exists (for retry scenarios)
    if api_key_id is not None:
        cursor.execute("""
            SELECT id FROM tx_request_log
            WHERE request_id = %s AND target_worker = %s AND api_key_id = %s
        """, (request_id, worker, api_key_id))
    else:
        cursor.execute("""
            SELECT id FROM tx_request_log
            WHERE request_id = %s AND target_worker = %s AND api_key_id IS NULL
        """, (request_id, worker))

    existing = cursor.fetchone()
    if existing:
        return existing[0] if isinstance(existing, tuple) else existing['id']

    # Insert new record
    payload_summary = json.dumps({
        'batch_num': batch_num,
        'batch_size': batch_size,
        'source': 'queue'
    })

    cursor.execute("""
        INSERT INTO tx_request_log
        (request_id, correlation_id, api_key_id, source, target_worker, action, priority, status, payload_summary)
        VALUES (%s, %s, %s, 'queue', %s, %s, %s, 'processing', %s)
    """, (request_id, correlation_id, api_key_id, worker, action, priority, payload_summary))
    conn.commit()
    return cursor.lastrowid


def log_daemon_request(cursor, db_conn, worker: str, action: str, batch_size: int = 0) -> int:
    """
    Create a request_log entry for daemon work cycle.
    Returns the request_log_id for linking discovered addresses.
    """
    import uuid
    from datetime import datetime

    request_id = f"daemon-{worker}-{uuid.uuid4().hex[:12]}"
    correlation_id = request_id

    cursor.execute("""
        INSERT INTO tx_request_log
        (request_id, correlation_id, api_key_id, source, target_worker, action, priority, status, payload_summary)
        VALUES (%s, %s, NULL, 'daemon', %s, %s, 5, 'processing', %s)
    """, (
        request_id,
        correlation_id,
        worker,
        action,
        json.dumps({'batch_size': batch_size})
    ))
    request_log_id = cursor.lastrowid
    db_conn.commit()
    print(f"[DAEMON] Created request_log entry id={request_log_id} for {worker}/{action}")
    return request_log_id


def update_daemon_request(cursor, db_conn, request_log_id: int, status: str, result: Dict):
    """Update a daemon request_log entry with completion status and results."""
    cursor.execute("""
        UPDATE tx_request_log
        SET status = %s, completed_at = CURRENT_TIMESTAMP(3), result = %s
        WHERE id = %s
    """, (status, json.dumps(result), request_log_id))
    db_conn.commit()


def run_queue_consumer(args):
    """
    Run in gateway queue consumer mode.
    Consumes from mq.guide.funder.request, processes addresses,
    and publishes responses to mq.guide.funder.response.

    Uses AddressHistoryWorker.process_addresses() for core processing logic.
    Gateway-specific publishing (response) happens after processing.
    """
    import uuid
    from datetime import datetime

    print(f"\n{'='*60}")
    print(f"  FUNDER - Gateway Queue Consumer Mode")
    print(f"{'='*60}")
    print(f"  Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print(f"  Request queue: {RABBITMQ_REQUEST_QUEUE}")
    print(f"  Response queue: {RABBITMQ_RESPONSE_QUEUE}")
    print(f"  VHost: {RABBITMQ_VHOST}")
    print(f"{'='*60}\n")

    # Create Solscan client
    solscan = SolscanClient()

    # Setup DB connection with reconnect capability
    db_state = {'conn': None, 'cursor': None, 'worker': None}

    def ensure_db_connection(channel):
        """Ensure DB connection is alive, reconnect and recreate worker if needed"""
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
                db_state['conn'] = mysql.connector.connect(**DB_CONFIG)
                db_state['cursor'] = db_state['conn'].cursor(dictionary=True)
                db_state['worker'] = AddressHistoryWorker(
                    db_conn=db_state['conn'],
                    channel=channel,
                    solscan=solscan,
                    tx_limit=args.tx_limit,
                    api_delay=args.api_delay,
                    dry_run=False
                )
                print("[OK] Database (re)connected")
            return db_state['worker'], db_state['cursor']
        except Exception as e:
            print(f"[WARN] Database connection failed: {e}")
            db_state['conn'] = None
            db_state['cursor'] = None
            db_state['worker'] = None
            return None, None

    # Connect to gateway RabbitMQ
    print(f"Connecting to RabbitMQ (vhost: {RABBITMQ_VHOST})...")
    connection, channel = setup_gateway_rabbitmq()
    channel.basic_qos(prefetch_count=1)

    # Initial connection
    ensure_db_connection(channel)

    # Verify init_tx_fetched column exists
    try:
        db_state['cursor'].execute("SELECT init_tx_fetched FROM tx_address LIMIT 1")
        db_state['cursor'].fetchall()
    except mysql.connector.Error:
        print("Adding init_tx_fetched column to tx_address...")
        db_state['cursor'].execute("""
            ALTER TABLE tx_address
            ADD COLUMN init_tx_fetched TINYINT(1) DEFAULT NULL
        """)
        db_state['conn'].commit()
        print("Column added successfully")

    print(f"Waiting for requests on '{RABBITMQ_REQUEST_QUEUE}'...")
    print("Press Ctrl+C to exit\n")

    def process_request(ch, method, properties, body):
        """Process a single request from gateway"""
        nonlocal channel
        start_time = time.time()

        try:
            request = json.loads(body)
            request_id = request.get('request_id', str(uuid.uuid4()))
            correlation_id = request.get('correlation_id', request_id)
            api_key_id = request.get('api_key_id')  # For billing tracking
            action = request.get('action', 'process')
            priority = request.get('priority', 5)
            batch = request.get('batch', {})

            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Request {request_id[:8]}...")
            print(f"  Action: {action}, api_key_id: {api_key_id}")

            # Ensure DB connection is alive
            worker, cursor = ensure_db_connection(channel)
            if not worker:
                raise Exception("Database connection unavailable")

            # Log this worker's request for billing tracking
            # This creates or retrieves the funder's own request_log entry (not gateway's)
            funder_log_id = log_worker_request(
                cursor, db_state['conn'],
                request_id, correlation_id, 'funder', action,
                batch_size=len(batch.get('addresses', []) or batch.get('funder_addresses', [])),
                priority=priority, api_key_id=api_key_id
            )
            print(f"  Funder request_log_id: {funder_log_id}")

            # Extract addresses from batch
            # Check multiple keys: 'addresses', 'funder_addresses' (from shredder cascade), or 'address_ids'
            addresses = batch.get('addresses', []) or batch.get('funder_addresses', [])
            address_ids = batch.get('address_ids', [])

            # If we got address_ids, look them up
            if address_ids and not addresses:
                placeholders = ','.join(['%s'] * len(address_ids))
                cursor.execute(f"SELECT address FROM tx_address WHERE id IN ({placeholders})", address_ids)
                addresses = [row['address'] for row in cursor.fetchall()]

            if not addresses:
                print(f"  No addresses to process")
                publish_response(ch, request_id, 'completed', {
                    'processed': 0,
                    'funders_found': 0,
                    'funders_not_found': 0
                }, correlation_id, funder_log_id)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            print(f"  Addresses: {len(addresses)}")

            # === USE PROVEN CLASS METHOD FOR CORE PROCESSING ===
            # For explicit API calls (action='process'), force re-lookup even if previously attempted
            # For cascades from other workers, respect init_tx_fetched to avoid duplicate work
            force_lookup = (action == 'process')
            result = worker.process_addresses(addresses, force=force_lookup, request_log_id=funder_log_id)

            elapsed = time.time() - start_time

            # Build response from result
            if result.get('error'):
                status = 'failed'
                response_data = {'processed': 0, 'error': result['error']}
            else:
                status = 'completed'
                response_data = {
                    'processed': result['processed'],
                    'funders_found': result['funders_found'],
                    'funders_not_found': result['funders_not_found'],
                    'elapsed_seconds': round(elapsed, 2)
                }

            publish_response(ch, request_id, status, response_data, correlation_id, funder_log_id)

            print(f"  Completed in {elapsed:.1f}s: {result['funders_found']} found, {result['funders_not_found']} not found")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except MySQLError as e:
            # MySQL errors are transient - requeue for retry
            print(f"  [DB ERROR] {e} - will retry")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        except Exception as e:
            # Other errors - send to DLQ
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()

            req_id = request.get('request_id', 'unknown') if 'request' in dir() else 'unknown'
            corr_id = request.get('correlation_id', req_id) if 'request' in dir() else req_id
            funder_id = funder_log_id if 'funder_log_id' in dir() else None
            publish_response(ch, req_id, 'failed', {'error': str(e)}, corr_id, funder_id)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(
        queue=RABBITMQ_REQUEST_QUEUE,
        on_message_callback=process_request
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        channel.stop_consuming()
    finally:
        solscan.close()
        connection.close()
        db_conn.close()

    return 0


# =============================================================================
# Address History Worker
# =============================================================================

class AddressHistoryWorker:
    """Worker that fetches initial transactions and finds funding wallets"""

    def __init__(self, db_conn, channel, solscan: SolscanClient,
                 prefetch: int = 10, tx_limit: int = 10,
                 dry_run: bool = False, api_delay: float = API_DELAY,
                 batch_delay: float = 0.5):
        self.db_conn = db_conn
        self.cursor = db_conn.cursor(dictionary=True)
        self.channel = channel
        self.solscan = solscan
        self.prefetch = prefetch
        self.tx_limit = tx_limit
        self.dry_run = dry_run
        self.api_delay = api_delay
        self.batch_delay = batch_delay

        # Stats
        self.messages_processed = 0
        self.addresses_processed = 0
        self.addresses_skipped = 0
        self.funders_found = 0
        self.funders_not_found = 0

        # Pending messages and addresses
        self.pending_messages = []  # List of (body, delivery_tag)
        self.should_stop = False

    def get_address_info(self, addresses: List[str]) -> Dict[str, Dict]:
        """
        Get address info from database.
        Returns dict of address -> {id, address_type, init_tx_fetched}
        """
        if not addresses:
            return {}

        placeholders = ','.join(['%s'] * len(addresses))
        self.cursor.execute(f"""
            SELECT id, address, address_type, init_tx_fetched
            FROM tx_address
            WHERE address IN ({placeholders})
        """, addresses)

        result = {}
        for row in self.cursor.fetchall():
            result[row['address']] = {
                'id': row['id'],
                'address_type': row['address_type'],
                'init_tx_fetched': row['init_tx_fetched']
            }
        return result

    def mark_addresses_initialized(self, addresses: List[str]):
        """Mark addresses as completed (init_tx_fetched = 1)"""
        if not addresses or self.dry_run:
            return

        placeholders = ','.join(['%s'] * len(addresses))
        execute_with_deadlock_retry(self.cursor, self.db_conn, f"""
            UPDATE tx_address
            SET init_tx_fetched = 1
            WHERE address IN ({placeholders})
        """, addresses)

    def claim_addresses(self, addresses: List[str], force: bool = False) -> List[str]:
        """
        Atomically claim addresses for processing (init_tx_fetched = 0 -> 2).
        Returns list of addresses successfully claimed.
        This prevents duplicate processing when multiple workers run.

        Args:
            addresses: List of addresses to claim
            force: If True, claim even if already processed (init_tx_fetched=1)
        """
        if not addresses or self.dry_run:
            return addresses  # In dry-run, pretend we claimed all

        claimed = []
        for addr in addresses:
            if force:
                # Force mode: claim any address not currently being processed (init_tx_fetched != 2)
                execute_with_deadlock_retry(self.cursor, self.db_conn, """
                    UPDATE tx_address
                    SET init_tx_fetched = 2
                    WHERE address = %s AND (init_tx_fetched != 2 OR init_tx_fetched IS NULL)
                """, (addr,))
            else:
                # Normal mode: only claim if init_tx_fetched is still 0 (or NULL)
                execute_with_deadlock_retry(self.cursor, self.db_conn, """
                    UPDATE tx_address
                    SET init_tx_fetched = 2
                    WHERE address = %s AND (init_tx_fetched = 0 OR init_tx_fetched IS NULL)
                """, (addr,))
            if self.cursor.rowcount > 0:
                claimed.append(addr)
        return claimed

    def ensure_addresses_exist(self, addresses: List[str]):
        """Ensure all addresses exist in tx_address table. Uses deadlock retry for INSERT."""
        if not addresses:
            return

        # Check which addresses already exist to avoid consuming AUTO_INCREMENT IDs
        placeholders = ','.join(['%s'] * len(addresses))
        self.cursor.execute(f"SELECT address FROM tx_address WHERE address IN ({placeholders})", addresses)
        existing = {row['address'] for row in self.cursor.fetchall()}

        # Only insert addresses that don't exist
        new_addresses = [addr for addr in addresses if addr not in existing]
        if new_addresses:
            values = [(addr, 'unknown') for addr in new_addresses]
            for attempt in range(MAX_DEADLOCK_RETRIES):
                try:
                    self.cursor.executemany("""
                        INSERT INTO tx_address (address, address_type)
                        VALUES (%s, %s)
                    """, values)
                    self.db_conn.commit()
                    break
                except mysql.connector.Error as e:
                    if e.errno in (1213, 1205) and attempt < MAX_DEADLOCK_RETRIES - 1:
                        self.db_conn.rollback()
                        delay = DEADLOCK_BASE_DELAY * (2 ** attempt) + random.uniform(0, 0.1)
                        print(f"    [!] Deadlock in ensure_addresses_exist (attempt {attempt + 1}/{MAX_DEADLOCK_RETRIES}), retrying in {delay:.2f}s...")
                        time.sleep(delay)
                    else:
                        raise

    def save_funding_info(self, target_address: str, funding_info: Dict, request_log_id: int = None):
        """
        Save funding info to tx_address table.
        Updates funded_by_address_id and related fields.
        funding_info has: funder, signature, amount, block_time, label, tags, account_type, active_age
        request_log_id: Links discovered funder address to the request for billing
        """
        if self.dry_run:
            return

        funder = funding_info['funder']
        amount = funding_info.get('amount')
        block_time = funding_info.get('block_time')
        label = funding_info.get('label')
        tags = funding_info.get('tags')
        account_type = funding_info.get('account_type')
        active_age = funding_info.get('active_age')

        for attempt in range(MAX_DEADLOCK_RETRIES):
            try:
                # Get funder address ID, insert only if doesn't exist (avoids AUTO_INCREMENT consumption)
                # Mark as init_tx_fetched=1 to prevent recursive funder lookups (we don't need to find the funder's funder)
                # Set request_log_id for billing linkage (only on new addresses)
                self.cursor.execute("SELECT id FROM tx_address WHERE address = %s", (funder,))
                funder_row = self.cursor.fetchone()
                if funder_row:
                    funder_id = funder_row['id']
                else:
                    self.cursor.execute("""
                        INSERT INTO tx_address (address, address_type, init_tx_fetched, request_log_id)
                        VALUES (%s, 'wallet', 1, %s)
                    """, (funder, request_log_id))
                    funder_id = self.cursor.lastrowid

                # Get target address ID
                self.cursor.execute("SELECT id FROM tx_address WHERE address = %s", (target_address,))
                target_row = self.cursor.fetchone()
                target_id = target_row['id'] if target_row else None

                # Build dynamic UPDATE with available metadata fields
                update_fields = ["funded_by_address_id = %s"]
                update_values = [funder_id]

                if amount is not None:
                    update_fields.append("funding_amount = %s")
                    update_values.append(amount)

                if block_time is not None:
                    update_fields.append("first_seen_block_time = %s")
                    update_values.append(block_time)

                if label is not None:
                    update_fields.append("label = COALESCE(label, %s)")  # Don't overwrite existing labels
                    update_values.append(label)

                mapped_type = None

                if tags is not None:
                    update_fields.append("account_tags = %s")
                    update_values.append(json.dumps(tags) if tags else None)

                    # Derive address_type from account_tags (more specific than account_type)
                    tags_lower = [t.lower() for t in tags] if tags else []
                    label_lower = (label or '').lower()

                    # Priority order: label-based vault check first (vault authorities/LP vaults
                    # often carry a 'pool' tag but are NOT pools — they own token accounts).
                    # Must exclude labels where "vault" is a token name inside parens,
                    # e.g. "Pump.fun AMM (VAULT-WSOL) Market" is a legit pool.
                    if ('vault' in label_lower
                            and not re.search(r'\([^)]*vault[^)]*\)', label_lower)):
                        mapped_type = 'vault'
                    elif 'pool' in tags_lower:
                        mapped_type = 'pool'
                    elif 'market' in tags_lower:
                        mapped_type = 'market'
                    elif any('vault' in tag for tag in tags_lower):
                        mapped_type = 'vault'
                    elif 'lptoken' in tags_lower:
                        mapped_type = 'lp_token'
                    elif 'dex_wallet' in tags_lower:
                        mapped_type = 'dex_wallet'
                    elif 'fee_vault' in tags_lower:
                        mapped_type = 'fee_vault'
                    elif 'arbitrage_bot' in tags_lower:
                        mapped_type = 'bot'
                    elif 'token_creator' in tags_lower:
                        mapped_type = 'wallet'
                        # Set label for token creators (type stays wallet for tax detection)
                        update_fields.append("label = COALESCE(label, %s)")
                        update_values.append('Token Creator')
                    elif 'memecoin' in tags_lower:
                        mapped_type = 'mint'
                    elif account_type == 'token_account':
                        mapped_type = 'ata'
                    elif account_type == 'mint':
                        mapped_type = 'mint'
                    elif account_type == 'address':
                        mapped_type = 'wallet'

                    if mapped_type:
                        # Override unknown, wallet, and ata (ata is often incorrectly assigned)
                        update_fields.append("address_type = CASE WHEN address_type IN ('unknown', 'wallet', 'ata', 'program', 'mint') THEN %s ELSE address_type END")
                        update_values.append(mapped_type)

                elif account_type is not None:
                    # Fallback: use account_type if no tags
                    mapped_type = account_type
                    if account_type == 'token_account':
                        mapped_type = 'ata'
                    elif account_type == 'address':
                        mapped_type = 'wallet'
                    update_fields.append("address_type = CASE WHEN address_type = 'unknown' THEN %s ELSE address_type END")
                    update_values.append(mapped_type)

                if active_age is not None:
                    update_fields.append("active_age_days = %s")
                    update_values.append(active_age)

                update_values.append(target_address)

                # Execute update with all available fields
                self.cursor.execute(f"""
                    UPDATE tx_address
                    SET {', '.join(update_fields)}
                    WHERE address = %s AND funded_by_address_id IS NULL
                """, update_values)

                # Ensure tx_pool record exists for pool/lp_token addresses
                # Label format: "Meteora (PAPER-WSOL) Pool 1" or "Raydium (KOINZ) LP Token"
                if mapped_type in ('pool', 'lp_token') and target_id:
                    self.cursor.execute(
                        "SELECT 1 FROM tx_pool WHERE pool_address_id = %s", (target_id,))
                    if not self.cursor.fetchone():
                        program_id = None
                        token1_id = None
                        token2_id = None

                        if label:
                            # DEX name is everything before the opening paren
                            paren_match = re.match(r'^(.+?)\s*\((.+?)\)', label)
                            if paren_match:
                                dex_name = paren_match.group(1).strip()
                                token_part = paren_match.group(2).strip()

                                # Look up DEX program
                                if dex_name:
                                    self.cursor.execute(
                                        "SELECT id FROM tx_program WHERE name = %s LIMIT 1",
                                        (dex_name,))
                                    prog_row = self.cursor.fetchone()
                                    if prog_row:
                                        program_id = prog_row['id']

                                # Parse tokens: "PAPER-WSOL" → token1=PAPER, token2=WSOL
                                #                "KOINZ"     → token1=KOINZ
                                symbols = [s.strip() for s in token_part.split('-', 1)]
                                for i, sym in enumerate(symbols[:2]):
                                    if sym:
                                        self.cursor.execute(
                                            "SELECT id FROM tx_token WHERE token_symbol = %s LIMIT 1",
                                            (sym,))
                                        tok_row = self.cursor.fetchone()
                                        if tok_row:
                                            if i == 0:
                                                token1_id = tok_row['id']
                                            else:
                                                token2_id = tok_row['id']

                        self.cursor.execute("""
                            INSERT INTO tx_pool (pool_address_id, program_id, token1_id, token2_id, pool_label)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (target_id, program_id, token1_id, token2_id, label))

                self.db_conn.commit()
                break  # Success, exit retry loop
            except mysql.connector.Error as e:
                if e.errno in (1213, 1205) and attempt < MAX_DEADLOCK_RETRIES - 1:
                    self.db_conn.rollback()
                    delay = DEADLOCK_BASE_DELAY * (2 ** attempt) + random.uniform(0, 0.1)
                    print(f"    [!] Deadlock in save_funding_info (attempt {attempt + 1}/{MAX_DEADLOCK_RETRIES}), retrying in {delay:.2f}s...")
                    time.sleep(delay)
                else:
                    raise

    def find_funder_for_address(self, address: str) -> Optional[Dict]:
        """
        Get funding info for an address using /account/metadata API.
        Falls back to /account/transfer if metadata doesn't have funder info.

        Returns dict with: funder, signature, block_time, label, tags, account_type, active_age
        """
        # Skip known programs and system addresses
        if should_skip_address(address):
            return None

        time.sleep(self.api_delay)

        # Primary: Use /account/metadata API (single call for all info)
        metadata = self.solscan.get_account_metadata(address)
        if metadata:
            funded_by = metadata.get('funded_by', {})
            if funded_by and funded_by.get('funded_by'):
                return {
                    'funder': funded_by['funded_by'],
                    'signature': funded_by.get('tx_hash'),
                    'amount': None,  # Metadata API doesn't provide amount
                    'block_time': funded_by.get('block_time'),
                    # Bonus metadata fields
                    'label': metadata.get('account_label'),
                    'tags': metadata.get('account_tags'),
                    'account_type': metadata.get('account_type'),
                    'active_age': metadata.get('active_age')
                }

        # Fallback: Use /account/transfer API if metadata didn't have funder
        time.sleep(self.api_delay)
        data = self.solscan.get_account_transfers(address, self.tx_limit, token_filter=SOL_TOKEN)
        if not data:
            return None

        result = self.solscan.find_funding_wallet(address, data)
        if result and metadata:
            # Enrich with metadata fields if we have them
            result['label'] = metadata.get('account_label')
            result['tags'] = metadata.get('account_tags')
            result['account_type'] = metadata.get('account_type')
            result['active_age'] = metadata.get('active_age')
        return result

    def process_addresses(self, addresses: List[str], force: bool = False, request_log_id: int = None) -> Dict:
        """
        Core processing logic for a list of addresses.
        Returns dict with: processed, claimed, funders_found, funders_not_found

        Used by both process_batch() and run_queue_consumer() to avoid code duplication.

        Args:
            addresses: List of addresses to process
            force: If True, re-process addresses even if init_tx_fetched=1 (for explicit API calls)
            request_log_id: Links discovered funder addresses to the request for billing
        """
        result = {
            'processed': 0,
            'claimed': 0,
            'skipped': 0,
            'funders_found': 0,
            'funders_not_found': 0,
            'error': None
        }

        if not addresses:
            return result

        # Filter out known programs and system addresses
        addresses = [a for a in addresses if not should_skip_address(a)]
        if not addresses:
            return result

        # Step 1: Ensure all addresses exist in DB
        self.ensure_addresses_exist(addresses)

        # Step 2: Get address info from DB
        address_info = self.get_address_info(addresses)

        # Step 3: Filter to addresses that need funding lookup
        candidates = []
        for addr in addresses:
            info = address_info.get(addr, {})
            # If force=True, re-process even if already attempted (for explicit API calls)
            if force or not info.get('init_tx_fetched'):
                candidates.append(addr)
            else:
                result['skipped'] += 1

        print(f"  Candidates for lookup: {len(candidates)}{' (force=True)' if force else ''}")
        print(f"  Already processed/claimed: {result['skipped']}")

        # Step 4: Atomically claim addresses
        claimed = self.claim_addresses(candidates, force=force)
        result['claimed'] = len(claimed)
        if len(claimed) < len(candidates):
            skipped_by_claim = len(candidates) - len(claimed)
            print(f"  Claimed: {len(claimed)} (skipped {skipped_by_claim} claimed by others)")
            result['skipped'] += skipped_by_claim

        if self.dry_run:
            print(f"  DRY RUN - would lookup funding for {len(claimed)} addresses")
            return result

        # Step 4b: For explicit API calls, update addresses to use this request's request_log_id
        # This ensures the client who requested funder processing is billed, not the original creator
        if force and request_log_id and claimed:
            placeholders = ','.join(['%s'] * len(claimed))
            rowcount = execute_with_deadlock_retry(self.cursor, self.db_conn, f"""
                UPDATE tx_address
                SET request_log_id = %s
                WHERE address IN ({placeholders})
            """, [request_log_id] + claimed)
            print(f"  Updated {rowcount} addresses with request_log_id={request_log_id}")

        # Step 5: Find funders using batch API
        # Get batch_size from config table (runtime_editable, checked each iteration)
        initialized_addresses = []
        batch_size = get_db_batch_size(self.cursor)
        total_claimed = len(claimed)

        for batch_start in range(0, total_claimed, batch_size):
            batch = claimed[batch_start:batch_start + batch_size]
            batch_num = batch_start // batch_size + 1
            total_batches = (total_claimed + batch_size - 1) // batch_size

            print(f"  Batch {batch_num}/{total_batches}: Fetching metadata for {len(batch)} addresses...")

            # Call batch API
            metadata_map = self.solscan.get_account_metadata_multi(batch)
            print(f"    Got metadata for {len(metadata_map)} addresses")

            # Process each address in batch
            for i, addr in enumerate(batch):
                idx = batch_start + i + 1
                print(f"  [{idx}/{total_claimed}] {addr[:20]}...", end='')

                metadata = metadata_map.get(addr)
                funding_info = None

                if metadata:
                    funded_by = metadata.get('funded_by', {})
                    if funded_by and funded_by.get('funded_by'):
                        funding_info = {
                            'funder': funded_by['funded_by'],
                            'signature': funded_by.get('tx_hash'),
                            'amount': None,
                            'block_time': funded_by.get('block_time'),
                            'label': metadata.get('account_label'),
                            'tags': metadata.get('account_tags'),
                            'account_type': metadata.get('account_type'),
                            'active_age': metadata.get('active_age')
                        }

                if funding_info:
                    print(f" -> funded by {funding_info['funder'][:16]}...")
                    self.save_funding_info(addr, funding_info, request_log_id)
                    result['funders_found'] += 1
                    self.funders_found += 1
                else:
                    print(f" -> no funder found")
                    result['funders_not_found'] += 1
                    self.funders_not_found += 1

                initialized_addresses.append(addr)
                result['processed'] += 1
                self.addresses_processed += 1

            # 1 second delay between batch API calls
            if batch_start + batch_size < total_claimed:
                time.sleep(1.0)

        # Step 6: Mark addresses as processed
        if initialized_addresses:
            self.mark_addresses_initialized(initialized_addresses)
            print(f"\n  Marked {len(initialized_addresses)} addresses as processed")

        return result

    def fetch_address_history(self, address: str, address_type: Optional[str]) -> Set[str]:
        """
        Fetch initial transaction history for an address.
        Returns set of transaction signatures.
        """
        signatures = set()

        # Skip known programs and system addresses
        if should_skip_address(address):
            return signatures

        # Determine which APIs to call based on address type
        if address_type == 'mint':
            # For tokens/mints, get both transfer and DeFi activity
            time.sleep(self.api_delay)
            data = self.solscan.get_token_transfers(address, self.tx_limit)
            if data:
                sigs = extract_signatures_from_token_transfer(data)
                signatures.update(sigs)

            time.sleep(self.api_delay)
            data = self.solscan.get_token_defi_activities(address, self.tx_limit)
            if data:
                sigs = extract_signatures_from_defi_activities(data)
                signatures.update(sigs)

        elif address_type == 'pool':
            # For pools, try DeFi activities
            time.sleep(self.api_delay)
            data = self.solscan.get_token_defi_activities(address, self.tx_limit)
            if data:
                sigs = extract_signatures_from_defi_activities(data)
                signatures.update(sigs)

        else:
            # For wallets, ATAs, unknown - get account transfers
            time.sleep(self.api_delay)
            data = self.solscan.get_account_transfers(address, self.tx_limit)
            if data:
                sigs = extract_signatures_from_account_transfer(data)
                signatures.update(sigs)

        return signatures

    def process_batch(self):
        """Process batch of messages - find funding wallets and save to DB"""
        if not self.pending_messages:
            return

        print(f"\n{'='*70}")
        print(f"Processing batch of {len(self.pending_messages)} messages...")

        # Step 1: Collect all unique addresses from all messages
        all_addresses = set()
        for body, _ in self.pending_messages:
            try:
                addresses = json.loads(body)
                if isinstance(addresses, list):
                    for addr in addresses:
                        if isinstance(addr, str) and len(addr) >= 32:
                            all_addresses.add(addr)
            except json.JSONDecodeError:
                continue

        # Filter out known programs and system addresses
        all_addresses = {a for a in all_addresses if not should_skip_address(a)}

        print(f"  Unique addresses: {len(all_addresses)}")

        if not all_addresses:
            # ACK all messages and return
            for _, tag in self.pending_messages:
                self.channel.basic_ack(delivery_tag=tag)
            self.messages_processed += len(self.pending_messages)
            self.pending_messages = []
            return

        # Step 2: Ensure all addresses exist in DB
        self.ensure_addresses_exist(list(all_addresses))

        # Step 3: Get address info from DB
        address_info = self.get_address_info(list(all_addresses))

        # Step 4: Filter to addresses that need funding lookup
        candidates = []
        for addr in all_addresses:
            info = address_info.get(addr, {})
            # 0 or NULL = not processed, 1 = done, 2 = in progress by another worker
            if not info.get('init_tx_fetched'):
                candidates.append(addr)
            else:
                self.addresses_skipped += 1

        print(f"  Candidates for lookup: {len(candidates)}")
        print(f"  Already processed/claimed: {len(all_addresses) - len(candidates)}")

        # Step 4b: Atomically claim addresses (prevents duplicate work)
        addresses_to_process = self.claim_addresses(candidates)
        if len(addresses_to_process) < len(candidates):
            skipped_by_claim = len(candidates) - len(addresses_to_process)
            print(f"  Claimed by this worker: {len(addresses_to_process)} (skipped {skipped_by_claim} claimed by others)")

        if self.dry_run:
            print(f"  DRY RUN - would lookup funding for {len(addresses_to_process)} addresses")
            for addr in addresses_to_process[:5]:
                print(f"    {addr[:20]}...")
            if len(addresses_to_process) > 5:
                print(f"    ... and {len(addresses_to_process) - 5} more")

            # ACK all messages
            for _, tag in self.pending_messages:
                self.channel.basic_ack(delivery_tag=tag)
            self.messages_processed += len(self.pending_messages)
            self.pending_messages = []
            return

        # Step 5: Find funders for each address
        initialized_addresses = []
        for i, addr in enumerate(addresses_to_process):
            print(f"  [{i+1}/{len(addresses_to_process)}] {addr[:20]}...", end='')

            funding_info = self.find_funder_for_address(addr)

            if funding_info:
                print(f" -> funded by {funding_info['funder'][:16]}...")
                self.save_funding_info(addr, funding_info)
                self.funders_found += 1
            else:
                print(f" -> no funder found")
                self.funders_not_found += 1

            initialized_addresses.append(addr)
            self.addresses_processed += 1

        # Step 6: Mark addresses as processed
        if initialized_addresses:
            self.mark_addresses_initialized(initialized_addresses)
            print(f"\n  Marked {len(initialized_addresses)} addresses as processed")

        # Step 7: ACK all messages
        for _, tag in self.pending_messages:
            self.channel.basic_ack(delivery_tag=tag)
        self.messages_processed += len(self.pending_messages)

        # Clear pending
        self.pending_messages = []

        # Batch delay to reduce deadlocks
        if self.batch_delay > 0:
            print(f"  Waiting {self.batch_delay}s before next batch...")
            time.sleep(self.batch_delay)

        print(f"  Batch complete. Totals: {self.addresses_processed} processed, "
              f"{self.funders_found} found, {self.funders_not_found} not found")

    def on_message(self, channel, method, properties, body):
        """Handle incoming message - collect until batch is ready"""
        self.pending_messages.append((body, method.delivery_tag))

        if len(self.pending_messages) >= self.prefetch:
            self.process_batch()

    def start(self):
        """Start consuming messages with idle timeout"""
        # Set prefetch
        self.channel.basic_qos(prefetch_count=self.prefetch)

        self.channel.basic_consume(
            queue=RABBITMQ_QUEUE_IN,
            on_message_callback=self.on_message
        )

        print(f"Waiting for addresses on queue '{RABBITMQ_QUEUE_IN}'...")
        print(f"Prefetch: {self.prefetch}, TX limit: {self.tx_limit}")
        print("Press Ctrl+C to exit\n")

        try:
            # Use timeout-based consuming to process partial batches
            while not self.should_stop:
                # Process events with 5 second timeout
                self.channel.connection.process_data_events(time_limit=5)

                # If we have pending messages and haven't received new ones, process them
                if self.pending_messages:
                    print(f"\n  [idle] Processing {len(self.pending_messages)} pending messages...")
                    self.process_batch()

        except KeyboardInterrupt:
            print("\n\nShutting down...")
            # Process any remaining messages
            if self.pending_messages:
                print(f"Processing {len(self.pending_messages)} remaining messages...")
                self.process_batch()
            self.channel.stop_consuming()


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Address Funding Worker - Find funding wallets for addresses'
    )
    parser.add_argument('--prefetch', type=int, default=10,
                        help='Number of messages to prefetch before processing (default: 10)')
    parser.add_argument('--tx-limit', type=int, default=20,
                        help='Number of transactions to fetch per address (default: 20)')
    parser.add_argument('--api-delay', type=float, default=API_DELAY,
                        help=f'Delay between API calls in seconds (default: {API_DELAY})')
    parser.add_argument('--batch-delay', type=float, default=1.0,
                        help='Delay between batches in seconds to reduce deadlocks (default: 1.0)')
    # DB config now loaded from guide-config.json (standardized across all workers)
    parser.add_argument('--rabbitmq-host', default=RABBITMQ_HOST, help='RabbitMQ host')
    parser.add_argument('--rabbitmq-port', type=int, default=RABBITMQ_PORT, help='RabbitMQ port')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be processed without making changes')
    parser.add_argument('--address-file', type=str, default=None,
                        help='Path to .txt file with addresses (one per line). Skips RabbitMQ when provided.')
    parser.add_argument('--sync-db-missing', action='store_true',
                        help='Query DB for addresses with funded_by_address_id=NULL and init_tx_fetched=0, then process them.')
    parser.add_argument('--sync-db-missing-metadata', action='store_true',
                        help='Query DB for addresses with funded_by_address_id set but account_tags=NULL, fetch metadata only.')
    parser.add_argument('--limit', type=int, default=0,
                        help='Max addresses to process in --sync-db-missing or --sync-db-missing-metadata mode (0 = unlimited)')
    parser.add_argument('--interval', type=int, default=0,
                        help='Polling interval in seconds for --sync-db-missing mode. If set, daemon will sleep and retry when no work found (0 = exit when done)')
    parser.add_argument('--queue-consumer', action='store_true',
                        help='Run as gateway queue consumer (uses t16o_mq vhost)')

    args = parser.parse_args()

    # Gateway queue consumer mode
    if args.queue_consumer:
        if not HAS_PIKA:
            print("Error: pika not installed (required for queue consumer mode)")
            return 1
        if not HAS_MYSQL:
            print("Error: mysql-connector-python not installed")
            return 1
        return run_queue_consumer(args)

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        return 1

    # File/sync modes don't need RabbitMQ
    if not args.address_file and not args.sync_db_missing and not args.sync_db_missing_metadata and not HAS_PIKA:
        print("Error: pika not installed (required for queue mode)")
        return 1

    print(f"Address Funding Worker")
    print(f"{'='*60}")
    if args.address_file:
        print(f"MODE: File input ({args.address_file})")
    elif args.sync_db_missing:
        print(f"MODE: Sync DB Missing (funder discovery)")
    elif args.sync_db_missing_metadata:
        print(f"MODE: Sync DB Missing Metadata (metadata backfill)")
    else:
        print(f"MODE: Queue consumer")
        print(f"Queue: {RABBITMQ_QUEUE_IN}")
    print(f"Batch size: {args.prefetch}")
    print(f"TX limit: {args.tx_limit} per address")
    print(f"API delay: {args.api_delay}s")
    print(f"Batch delay: {args.batch_delay}s")
    if args.dry_run:
        print(f"DRY RUN: Yes")
    print()

    # Connect to MySQL
    print(f"Connecting to MySQL {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}...")
    db_conn = mysql.connector.connect(**DB_CONFIG)

    # Verify init_tx_fetched column exists
    cursor = db_conn.cursor()
    try:
        cursor.execute("SELECT init_tx_fetched FROM tx_address LIMIT 1")
        cursor.fetchall()
    except mysql.connector.Error:
        print("Adding init_tx_fetched column to tx_address...")
        cursor.execute("""
            ALTER TABLE tx_address
            ADD COLUMN init_tx_fetched TINYINT(1) DEFAULT NULL
        """)
        db_conn.commit()
        print("Column added successfully")
    cursor.close()

    # Create Solscan client
    solscan = SolscanClient()

    # FILE MODE or SYNC-DB-MISSING MODE: Process addresses from file or DB query
    if args.address_file or args.sync_db_missing or args.sync_db_missing_metadata:
        import os

        # Determine source of addresses
        if args.address_file:
            if not os.path.exists(args.address_file):
                print(f"Error: Address file not found: {args.address_file}")
                return 1

            # Read addresses from file
            print(f"Reading addresses from {args.address_file}...")
            with open(args.address_file, 'r') as f:
                all_addresses = []
                for line in f:
                    addr = line.strip()
                    # Skip empty lines and comments
                    if addr and not addr.startswith('#') and len(addr) >= 32:
                        all_addresses.append(addr)

            # Filter out known programs and skip prefixes
            all_addresses = [a for a in all_addresses if not should_skip_address(a)]
            print(f"Loaded {len(all_addresses)} addresses from file")
        else:
            # sync-db-missing mode: will query DB after FileWorker is created
            all_addresses = None  # Placeholder, will be populated below

        # For file mode, check we have addresses
        if args.address_file and not all_addresses:
            print("No valid addresses found in file")
            return 0

        # Create a mock worker for file processing (no channel needed)
        class FileWorker:
            def __init__(self, db_conn, solscan, tx_limit, dry_run, api_delay, batch_delay):
                self.db_conn = db_conn
                self.cursor = db_conn.cursor(dictionary=True)
                self.solscan = solscan
                self.tx_limit = tx_limit
                self.dry_run = dry_run
                self.api_delay = api_delay
                self.batch_delay = batch_delay
                self.addresses_processed = 0
                self.addresses_skipped = 0
                self.funders_found = 0
                self.funders_not_found = 0

            def get_address_info(self, addresses):
                if not addresses:
                    return {}
                placeholders = ','.join(['%s'] * len(addresses))
                self.cursor.execute(f"""
                    SELECT id, address, address_type, init_tx_fetched
                    FROM tx_address WHERE address IN ({placeholders})
                """, addresses)
                return {row['address']: row for row in self.cursor.fetchall()}

            def ensure_addresses_exist(self, addresses):
                if not addresses:
                    return
                # Check which addresses already exist to avoid consuming AUTO_INCREMENT IDs
                placeholders = ','.join(['%s'] * len(addresses))
                self.cursor.execute(f"SELECT address FROM tx_address WHERE address IN ({placeholders})", addresses)
                existing = {row['address'] for row in self.cursor.fetchall()}
                # Only insert addresses that don't exist
                new_addresses = [addr for addr in addresses if addr not in existing]
                if new_addresses:
                    values = [(addr, 'unknown') for addr in new_addresses]
                    for attempt in range(MAX_DEADLOCK_RETRIES):
                        try:
                            self.cursor.executemany("""
                                INSERT INTO tx_address (address, address_type) VALUES (%s, %s)
                            """, values)
                            self.db_conn.commit()
                            break
                        except mysql.connector.Error as e:
                            if e.errno in (1213, 1205) and attempt < MAX_DEADLOCK_RETRIES - 1:
                                self.db_conn.rollback()
                                delay = DEADLOCK_BASE_DELAY * (2 ** attempt) + random.uniform(0, 0.1)
                                print(f"    [!] Deadlock in ensure_addresses_exist (attempt {attempt + 1}/{MAX_DEADLOCK_RETRIES}), retrying in {delay:.2f}s...")
                                time.sleep(delay)
                            else:
                                raise

            def mark_addresses_initialized(self, addresses):
                if not addresses or self.dry_run:
                    return
                placeholders = ','.join(['%s'] * len(addresses))
                execute_with_deadlock_retry(self.cursor, self.db_conn, f"""
                    UPDATE tx_address SET init_tx_fetched = 1
                    WHERE address IN ({placeholders})
                """, addresses)

            def claim_addresses(self, addresses):
                """Atomically claim addresses (0/NULL -> 2). Returns claimed list."""
                if not addresses or self.dry_run:
                    return addresses
                claimed = []
                for addr in addresses:
                    execute_with_deadlock_retry(self.cursor, self.db_conn, """
                        UPDATE tx_address SET init_tx_fetched = 2
                        WHERE address = %s AND (init_tx_fetched = 0 OR init_tx_fetched IS NULL)
                    """, (addr,))
                    if self.cursor.rowcount > 0:
                        claimed.append(addr)
                return claimed

            def find_funder_for_address(self, address):
                """Use /account/metadata API first, fall back to /account/transfer"""
                if should_skip_address(address):
                    return None
                time.sleep(self.api_delay)

                # Primary: Use /account/metadata API
                metadata = self.solscan.get_account_metadata(address)
                if metadata:
                    funded_by = metadata.get('funded_by', {})
                    if funded_by and funded_by.get('funded_by'):
                        return {
                            'funder': funded_by['funded_by'],
                            'signature': funded_by.get('tx_hash'),
                            'amount': None,
                            'block_time': funded_by.get('block_time'),
                            'label': metadata.get('account_label'),
                            'tags': metadata.get('account_tags'),
                            'account_type': metadata.get('account_type'),
                            'active_age': metadata.get('active_age')
                        }

                # Fallback: Use /account/transfer API
                time.sleep(self.api_delay)
                data = self.solscan.get_account_transfers(address, self.tx_limit, token_filter=SOL_TOKEN)
                if not data:
                    return None
                result = self.solscan.find_funding_wallet(address, data)
                if result and metadata:
                    result['label'] = metadata.get('account_label')
                    result['tags'] = metadata.get('account_tags')
                    result['account_type'] = metadata.get('account_type')
                    result['active_age'] = metadata.get('active_age')
                return result

            def save_funding_info(self, target_address, funding_info, request_log_id=None):
                if self.dry_run:
                    return
                funder = funding_info['funder']
                amount = funding_info.get('amount')
                block_time = funding_info.get('block_time')
                label = funding_info.get('label')
                tags = funding_info.get('tags')
                account_type = funding_info.get('account_type')
                active_age = funding_info.get('active_age')

                for attempt in range(MAX_DEADLOCK_RETRIES):
                    try:
                        # Get funder address ID, insert only if doesn't exist (avoids AUTO_INCREMENT consumption)
                        # Mark as init_tx_fetched=1 to prevent recursive funder lookups
                        # Set request_log_id for billing linkage (only on new addresses)
                        self.cursor.execute("SELECT id FROM tx_address WHERE address = %s", (funder,))
                        funder_row = self.cursor.fetchone()
                        if funder_row:
                            funder_id = funder_row['id']
                        else:
                            self.cursor.execute("""
                                INSERT INTO tx_address (address, address_type, init_tx_fetched, request_log_id)
                                VALUES (%s, 'wallet', 1, %s)
                            """, (funder, request_log_id))
                            funder_id = self.cursor.lastrowid

                        self.cursor.execute("SELECT id FROM tx_address WHERE address = %s", (target_address,))
                        target_row = self.cursor.fetchone()
                        target_id = target_row['id'] if target_row else None

                        # Build dynamic UPDATE with available metadata fields
                        update_fields = ["funded_by_address_id = %s"]
                        update_values = [funder_id]

                        if amount is not None:
                            update_fields.append("funding_amount = %s")
                            update_values.append(amount)

                        if block_time is not None:
                            update_fields.append("first_seen_block_time = %s")
                            update_values.append(block_time)

                        if label is not None:
                            update_fields.append("label = COALESCE(label, %s)")
                            update_values.append(label)

                        if tags is not None:
                            update_fields.append("account_tags = %s")
                            update_values.append(json.dumps(tags) if tags else None)

                            # Derive address_type from account_tags
                            tags_lower = [t.lower() for t in tags] if tags else []
                            mapped_type = None

                            if 'pool' in tags_lower:
                                mapped_type = 'pool'
                            elif 'market' in tags_lower:
                                mapped_type = 'market'
                            elif any('vault' in tag for tag in tags_lower):
                                mapped_type = 'vault'
                            elif 'lptoken' in tags_lower:
                                mapped_type = 'lp_token'
                            elif 'dex_wallet' in tags_lower:
                                mapped_type = 'dex_wallet'
                            elif 'fee_vault' in tags_lower:
                                mapped_type = 'fee_vault'
                            elif 'arbitrage_bot' in tags_lower:
                                mapped_type = 'bot'
                            elif 'token_creator' in tags_lower:
                                mapped_type = 'wallet'
                                # Set label for token creators (type stays wallet for tax detection)
                                update_fields.append("label = COALESCE(label, %s)")
                                update_values.append('Token Creator')
                            elif 'memecoin' in tags_lower:
                                mapped_type = 'mint'
                            elif account_type == 'token_account':
                                mapped_type = 'ata'
                            elif account_type == 'mint':
                                mapped_type = 'mint'
                            elif account_type == 'address':
                                mapped_type = 'wallet'

                            if mapped_type:
                                update_fields.append("address_type = CASE WHEN address_type IN ('unknown', 'wallet', 'ata', 'program', 'mint') THEN %s ELSE address_type END")
                                update_values.append(mapped_type)

                        elif account_type is not None:
                            mapped_type = account_type
                            if account_type == 'token_account':
                                mapped_type = 'ata'
                            elif account_type == 'address':
                                mapped_type = 'wallet'
                            update_fields.append("address_type = CASE WHEN address_type = 'unknown' THEN %s ELSE address_type END")
                            update_values.append(mapped_type)

                        if active_age is not None:
                            update_fields.append("active_age_days = %s")
                            update_values.append(active_age)

                        update_values.append(target_address)

                        self.cursor.execute(f"""
                            UPDATE tx_address
                            SET {', '.join(update_fields)}
                            WHERE address = %s AND funded_by_address_id IS NULL
                        """, update_values)

                        self.db_conn.commit()
                        break  # Success
                    except mysql.connector.Error as e:
                        if e.errno in (1213, 1205) and attempt < MAX_DEADLOCK_RETRIES - 1:
                            self.db_conn.rollback()
                            delay = DEADLOCK_BASE_DELAY * (2 ** attempt) + random.uniform(0, 0.1)
                            print(f"    [!] Deadlock in save_funding_info (attempt {attempt + 1}/{MAX_DEADLOCK_RETRIES}), retrying in {delay:.2f}s...")
                            time.sleep(delay)
                        else:
                            raise

            def get_missing_addresses(self, limit: int) -> list:
                """
                Fetch addresses from DB where funded_by_address_id IS NULL
                and init_tx_fetched = 0 (or NULL).
                Orders by id ASC to process oldest addresses first (backfill mode).
                Returns list of dicts with 'address' and 'request_log_id' for billing.
                """
                query = """
                    SELECT address, request_log_id FROM tx_address
                    WHERE funded_by_address_id IS NULL
                      AND (init_tx_fetched = 0 OR init_tx_fetched IS NULL)
                    ORDER BY id ASC
                    LIMIT %s
                """
                self.cursor.execute(query, (limit,))
                return [{'address': row['address'], 'request_log_id': row['request_log_id']}
                        for row in self.cursor.fetchall()]

            def get_missing_metadata_addresses(self, limit: int) -> list:
                """
                Fetch addresses from DB where funded_by_address_id IS NOT NULL
                but account_tags IS NULL (have funder but missing metadata).
                Orders by id ASC to process oldest addresses first (backfill mode).
                Returns list of dicts with 'address' and 'request_log_id' for billing.
                """
                query = """
                    SELECT address, request_log_id FROM tx_address
                    WHERE funded_by_address_id IS NOT NULL
                      AND account_tags IS NULL
                    ORDER BY id ASC
                    LIMIT %s
                """
                self.cursor.execute(query, (limit,))
                return [{'address': row['address'], 'request_log_id': row['request_log_id']}
                        for row in self.cursor.fetchall()]

            def save_metadata_only(self, target_address: str, metadata: Dict):
                """
                Save only metadata fields to tx_address table (no funder changes).
                Used for backfilling metadata on addresses that already have funders.
                metadata has: account_label, account_tags, account_type, active_age
                """
                if self.dry_run:
                    return

                label = metadata.get('account_label')
                tags = metadata.get('account_tags')
                account_type = metadata.get('account_type')

                update_fields = []
                update_values = []

                if label is not None:
                    update_fields.append("label = COALESCE(label, %s)")
                    update_values.append(label)

                if tags is not None:
                    update_fields.append("account_tags = %s")
                    update_values.append(json.dumps(tags) if tags else None)

                    # Derive address_type from account_tags
                    tags_lower = [t.lower() for t in tags] if tags else []
                    mapped_type = None

                    if 'pool' in tags_lower:
                        mapped_type = 'pool'
                    elif 'market' in tags_lower:
                        mapped_type = 'market'
                    elif any('vault' in tag for tag in tags_lower):
                        mapped_type = 'vault'
                    elif 'lptoken' in tags_lower:
                        mapped_type = 'lp_token'
                    elif 'dex_wallet' in tags_lower:
                        mapped_type = 'dex_wallet'
                    elif 'fee_vault' in tags_lower:
                        mapped_type = 'fee_vault'
                    elif 'arbitrage_bot' in tags_lower:
                        mapped_type = 'bot'
                    elif 'token_creator' in tags_lower:
                        mapped_type = 'wallet'
                        update_fields.append("label = COALESCE(label, %s)")
                        update_values.append('Token Creator')
                    elif 'memecoin' in tags_lower:
                        mapped_type = 'mint'
                    elif account_type == 'token_account':
                        mapped_type = 'ata'
                    elif account_type == 'mint':
                        mapped_type = 'mint'
                    elif account_type == 'address':
                        mapped_type = 'wallet'

                    if mapped_type:
                        update_fields.append("address_type = CASE WHEN address_type IN ('unknown', 'wallet', 'ata', 'program', 'mint') THEN %s ELSE address_type END")
                        update_values.append(mapped_type)

                elif account_type is not None:
                    mapped_type = account_type
                    if account_type == 'token_account':
                        mapped_type = 'ata'
                    elif account_type == 'mint':
                        mapped_type = 'mint'
                    elif account_type == 'address':
                        mapped_type = 'wallet'
                    update_fields.append("address_type = CASE WHEN address_type IN ('unknown', 'wallet', 'ata') THEN %s ELSE address_type END")
                    update_values.append(mapped_type)

                if not update_fields:
                    return

                update_values.append(target_address)
                execute_with_deadlock_retry(self.cursor, self.db_conn, f"""
                    UPDATE tx_address
                    SET {', '.join(update_fields)}
                    WHERE address = %s
                """, update_values)

        worker = FileWorker(db_conn, solscan, args.tx_limit, args.dry_run, args.api_delay, args.batch_delay)

        # SYNC-DB-MISSING MODE: Query DB in batches until exhausted
        if args.sync_db_missing:
            total_limit = args.limit if args.limit > 0 else float('inf')
            batch_num = 0
            daemon_request_log_id = None

            print(f"Sync DB Missing Mode")
            print(f"  Limit: {args.limit if args.limit > 0 else 'unlimited'}")
            print()

            while worker.addresses_processed < total_limit:
                # Get batch_size from config each iteration (runtime_editable)
                batch_size = get_db_batch_size(worker.cursor)
                remaining = int(min(batch_size, total_limit - worker.addresses_processed))
                candidates = worker.get_missing_addresses(remaining)

                if not candidates:
                    print("\nNo more addresses to process.")
                    break

                # Filter out known programs and skip prefixes
                original_count = len(candidates)
                skipped_addrs = [c['address'] for c in candidates if should_skip_address(c['address'])]
                candidates = [c for c in candidates if not should_skip_address(c['address'])]

                # Mark skipped addresses so they don't appear again
                if skipped_addrs and not args.dry_run:
                    worker.mark_addresses_initialized(skipped_addrs)
                    print(f"  Marked {len(skipped_addrs)} system/program addresses as processed (skipped)")

                if not candidates:
                    print("  No valid candidates in this batch, continuing...")
                    continue

                batch_num += 1
                print(f"\n{'='*60}")
                print(f"Batch {batch_num}: Found {len(candidates)} candidates")

                # Build address-to-request_log_id mapping for billing attribution
                addr_to_request_log = {c['address']: c['request_log_id'] for c in candidates}
                candidate_addresses = list(addr_to_request_log.keys())

                # Atomically claim addresses
                addresses_to_process = worker.claim_addresses(candidate_addresses)
                if len(addresses_to_process) < len(candidate_addresses):
                    skipped = len(candidate_addresses) - len(addresses_to_process)
                    print(f"  Claimed: {len(addresses_to_process)} (skipped {skipped} claimed by others)")

                if not addresses_to_process:
                    print("  No addresses claimed, retrying...")
                    time.sleep(1)
                    continue

                if args.dry_run:
                    print(f"  DRY RUN - would lookup funding for {len(addresses_to_process)} addresses")
                    for addr in addresses_to_process[:5]:
                        print(f"    {addr[:20]}...")
                    if len(addresses_to_process) > 5:
                        print(f"    ... and {len(addresses_to_process) - 5} more")
                    # In dry-run, simulate progress
                    worker.addresses_processed += len(addresses_to_process)
                else:
                    initialized = []
                    batch_funders_found = 0
                    batch_funders_not_found = 0

                    # Use batch API - get batch_size from config (runtime_editable)
                    api_batch_size = get_db_batch_size(worker.cursor)
                    total_to_process = len(addresses_to_process)

                    for api_batch_start in range(0, total_to_process, api_batch_size):
                        api_batch = addresses_to_process[api_batch_start:api_batch_start + api_batch_size]
                        api_batch_num = api_batch_start // api_batch_size + 1
                        total_api_batches = (total_to_process + api_batch_size - 1) // api_batch_size

                        print(f"    API batch {api_batch_num}/{total_api_batches}: Fetching metadata for {len(api_batch)} addresses...")

                        # Call batch API
                        metadata_map = solscan.get_account_metadata_multi(api_batch)
                        print(f"      Got metadata for {len(metadata_map)} addresses")

                        # Process each address in API batch
                        for j, addr in enumerate(api_batch):
                            if worker.addresses_processed >= total_limit:
                                break

                            idx = api_batch_start + j + 1
                            request_log_id = addr_to_request_log.get(addr) or daemon_request_log_id
                            print(f"  [{idx}/{total_to_process}] {addr[:20]}...", end='')

                            metadata = metadata_map.get(addr)
                            funding_info = None

                            if metadata:
                                funded_by = metadata.get('funded_by', {})
                                if funded_by and funded_by.get('funded_by'):
                                    funding_info = {
                                        'funder': funded_by['funded_by'],
                                        'signature': funded_by.get('tx_hash'),
                                        'amount': None,
                                        'block_time': funded_by.get('block_time'),
                                        'label': metadata.get('account_label'),
                                        'tags': metadata.get('account_tags'),
                                        'account_type': metadata.get('account_type'),
                                        'active_age': metadata.get('active_age')
                                    }

                            if funding_info:
                                print(f" -> funded by {funding_info['funder'][:16]}...")
                                worker.save_funding_info(addr, funding_info, request_log_id)
                                worker.funders_found += 1
                                batch_funders_found += 1
                            else:
                                print(f" -> no funder found")
                                worker.funders_not_found += 1
                                batch_funders_not_found += 1

                            initialized.append(addr)
                            worker.addresses_processed += 1

                        # Check limit after each API batch
                        if worker.addresses_processed >= total_limit:
                            print(f"\n  Reached limit of {args.limit} addresses")
                            break

                        # 1 second delay between API batch calls
                        if api_batch_start + api_batch_size < total_to_process:
                            time.sleep(1.0)

                    # Only log to tx_request_log if we actually updated rows
                    if batch_funders_found > 0:
                        daemon_request_log_id = log_daemon_request(
                            worker.cursor, worker.db_conn,
                            worker='funder', action='discover',
                            batch_size=len(addresses_to_process)
                        )
                        update_daemon_request(
                            worker.cursor, worker.db_conn, daemon_request_log_id,
                            status='completed',
                            result={
                                'processed': len(initialized),
                                'funders_found': batch_funders_found,
                                'funders_not_found': batch_funders_not_found
                            }
                        )

                    worker.mark_addresses_initialized(initialized)

                print(f"  Batch complete. Total processed: {worker.addresses_processed}")

                if args.batch_delay > 0:
                    print(f"  Waiting {args.batch_delay}s before next batch...")
                    time.sleep(args.batch_delay)

        # SYNC-DB-MISSING-METADATA MODE: Query DB for addresses with funders but missing metadata
        elif args.sync_db_missing_metadata:
            total_limit = args.limit if args.limit > 0 else float('inf')
            batch_num = 0
            metadata_updated = 0
            metadata_not_found = 0

            print(f"Sync DB Missing Metadata Mode")
            print(f"  Target: addresses with funded_by_address_id but no account_tags")
            print(f"  Limit: {args.limit if args.limit > 0 else 'unlimited'}")
            print()

            while worker.addresses_processed < total_limit:
                # Get batch_size from config each iteration (runtime_editable)
                batch_size = get_db_batch_size(worker.cursor)
                remaining = int(min(batch_size, total_limit - worker.addresses_processed))
                candidates = worker.get_missing_metadata_addresses(remaining)

                if not candidates:
                    print("\nNo more addresses to process.")
                    break

                # Filter out known programs and skip prefixes
                skipped_addrs = [c['address'] for c in candidates if should_skip_address(c['address'])]
                candidates = [c for c in candidates if not should_skip_address(c['address'])]

                # For skipped addresses, set account_tags to empty array so they don't appear again
                if skipped_addrs and not args.dry_run:
                    placeholders = ','.join(['%s'] * len(skipped_addrs))
                    worker.cursor.execute(f"""
                        UPDATE tx_address SET account_tags = '[]'
                        WHERE address IN ({placeholders})
                    """, skipped_addrs)
                    worker.db_conn.commit()
                    print(f"  Marked {len(skipped_addrs)} system/program addresses with empty tags (skipped)")

                if not candidates:
                    print("  No valid candidates in this batch, continuing...")
                    continue

                batch_num += 1
                print(f"\n{'='*60}")
                print(f"Batch {batch_num}: Found {len(candidates)} candidates needing metadata")

                candidate_addresses = [c['address'] for c in candidates]

                if args.dry_run:
                    print(f"  DRY RUN - would fetch metadata for {len(candidate_addresses)} addresses")
                    for addr in candidate_addresses[:5]:
                        print(f"    {addr[:20]}...")
                    if len(candidate_addresses) > 5:
                        print(f"    ... and {len(candidate_addresses) - 5} more")
                    worker.addresses_processed += len(candidate_addresses)
                else:
                    batch_metadata_found = 0
                    batch_metadata_not_found = 0

                    # Use batch API - get batch_size from config
                    api_batch_size = get_db_batch_size(worker.cursor)
                    total_to_process = len(candidate_addresses)

                    for api_batch_start in range(0, total_to_process, api_batch_size):
                        api_batch = candidate_addresses[api_batch_start:api_batch_start + api_batch_size]
                        api_batch_num = api_batch_start // api_batch_size + 1
                        total_api_batches = (total_to_process + api_batch_size - 1) // api_batch_size

                        print(f"    API batch {api_batch_num}/{total_api_batches}: Fetching metadata for {len(api_batch)} addresses...")

                        # Call batch API (debug=True for first batch to diagnose issues)
                        is_first_batch = (batch_num == 1 and api_batch_num == 1)
                        metadata_map = solscan.get_account_metadata_multi(api_batch, debug=is_first_batch)
                        print(f"      Got metadata for {len(metadata_map)} addresses")

                        # Process each address in API batch
                        for j, addr in enumerate(api_batch):
                            if worker.addresses_processed >= total_limit:
                                break

                            idx = api_batch_start + j + 1
                            print(f"  [{idx}/{total_to_process}] {addr[:20]}...", end='')

                            metadata = metadata_map.get(addr)

                            if metadata and metadata.get('account_tags'):
                                tags = metadata.get('account_tags')
                                print(f" -> tags: {tags}")
                                worker.save_metadata_only(addr, metadata)
                                metadata_updated += 1
                                batch_metadata_found += 1
                            else:
                                print(f" -> no tags found")
                                # Set empty tags so this address isn't queried again
                                if not args.dry_run:
                                    worker.cursor.execute("""
                                        UPDATE tx_address SET account_tags = '[]'
                                        WHERE address = %s
                                    """, (addr,))
                                    worker.db_conn.commit()
                                metadata_not_found += 1
                                batch_metadata_not_found += 1

                            worker.addresses_processed += 1

                        # Check limit after each API batch
                        if worker.addresses_processed >= total_limit:
                            print(f"\n  Reached limit of {args.limit} addresses")
                            break

                        # 1 second delay between API batch calls
                        if api_batch_start + api_batch_size < total_to_process:
                            time.sleep(1.0)

                    # Only log to tx_request_log if we actually updated rows
                    if batch_metadata_found > 0:
                        daemon_request_log_id = log_daemon_request(
                            worker.cursor, worker.db_conn,
                            worker='funder', action='metadata_backfill',
                            batch_size=len(candidate_addresses)
                        )
                        update_daemon_request(
                            worker.cursor, worker.db_conn, daemon_request_log_id,
                            status='completed',
                            result={
                                'processed': worker.addresses_processed,
                                'metadata_found': batch_metadata_found,
                                'metadata_not_found': batch_metadata_not_found
                            }
                        )

                    # Mark all processed addresses as initialized
                    worker.mark_addresses_initialized(candidate_addresses)

                print(f"  Batch complete. Total processed: {worker.addresses_processed}")

                if args.batch_delay > 0:
                    print(f"  Waiting {args.batch_delay}s before next batch...")
                    time.sleep(args.batch_delay)

            # Print final summary for metadata mode
            print(f"\n{'='*60}")
            print(f"Metadata Backfill Summary:")
            print(f"  Addresses processed: {worker.addresses_processed}")
            print(f"  Metadata found: {metadata_updated}")
            print(f"  Metadata not found: {metadata_not_found}")

        # FILE MODE: Process addresses from file
        else:
            # Ensure all addresses exist
            worker.ensure_addresses_exist(all_addresses)

            # Get address info and filter to unprocessed
            address_info = worker.get_address_info(all_addresses)
            candidates = []
            for addr in all_addresses:
                info = address_info.get(addr, {})
                if not info.get('init_tx_fetched'):
                    candidates.append(addr)
                else:
                    worker.addresses_skipped += 1

            print(f"Candidates for lookup: {len(candidates)}")
            print(f"Already processed/claimed: {worker.addresses_skipped}")

            # Atomically claim addresses (prevents duplicate work with other workers)
            addresses_to_process = worker.claim_addresses(candidates)
            if len(addresses_to_process) < len(candidates):
                skipped = len(candidates) - len(addresses_to_process)
                print(f"Claimed by this worker: {len(addresses_to_process)} (skipped {skipped} claimed by others)")

            if args.dry_run:
                print(f"\nDRY RUN - would lookup funding for {len(addresses_to_process)} addresses")
            else:
                # Process in batches
                batch_size = args.prefetch
                for i in range(0, len(addresses_to_process), batch_size):
                    batch = addresses_to_process[i:i + batch_size]
                    print(f"\nBatch {i // batch_size + 1}: Processing {len(batch)} addresses...")

                    initialized = []
                    for j, addr in enumerate(batch):
                        print(f"  [{i + j + 1}/{len(addresses_to_process)}] {addr[:20]}...", end='')
                        funding_info = worker.find_funder_for_address(addr)
                        if funding_info:
                            print(f" -> funded by {funding_info['funder'][:16]}...")
                            worker.save_funding_info(addr, funding_info)
                            worker.funders_found += 1
                        else:
                            print(f" -> no funder found")
                            worker.funders_not_found += 1
                        initialized.append(addr)
                        worker.addresses_processed += 1

                    worker.mark_addresses_initialized(initialized)

                    if args.batch_delay > 0 and i + batch_size < len(addresses_to_process):
                        print(f"  Waiting {args.batch_delay}s before next batch...")
                        time.sleep(args.batch_delay)

        solscan.close()
        db_conn.close()

        print(f"\n{'='*60}")
        print(f"Done!")
        print(f"  Addresses processed: {worker.addresses_processed}")
        print(f"  Addresses skipped: {worker.addresses_skipped}")
        print(f"  Funders found: {worker.funders_found}")
        print(f"  Funders not found: {worker.funders_not_found}")
        print(f"{'='*60}")
        return 0

    # QUEUE MODE: Connect to RabbitMQ and consume messages
    print(f"Connecting to RabbitMQ {args.rabbitmq_host}:{args.rabbitmq_port}...")
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=args.rabbitmq_host,
            port=args.rabbitmq_port,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )
    )
    channel = connection.channel()
    channel.queue_declare(
        queue=RABBITMQ_QUEUE_IN,
        durable=True,
        arguments={'x-max-priority': 10}
    )

    # Create and start worker
    worker = AddressHistoryWorker(
        db_conn, channel, solscan,
        prefetch=args.prefetch,
        tx_limit=args.tx_limit,
        dry_run=args.dry_run,
        api_delay=args.api_delay,
        batch_delay=args.batch_delay
    )

    try:
        worker.start()
    finally:
        solscan.close()
        connection.close()
        db_conn.close()

    print(f"\n{'='*60}")
    print(f"Done!")
    print(f"  Messages processed: {worker.messages_processed}")
    print(f"  Addresses processed: {worker.addresses_processed}")
    print(f"  Addresses skipped: {worker.addresses_skipped}")
    print(f"  Funders found: {worker.funders_found}")
    print(f"  Funders not found: {worker.funders_not_found}")
    print(f"{'='*60}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
