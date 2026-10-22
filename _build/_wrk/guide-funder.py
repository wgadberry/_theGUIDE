#!/usr/bin/env python3
"""
Guide Funder - Queue Consumer (multi-threaded, config-driven)

Supervisor thread polls config for desired thread/prefetch counts.
Worker threads each own their own DB + RabbitMQ + API connections.

Consumes address batches from mq.guide.funder.request, fetches account
metadata via Solscan API, identifies funding wallets, classifies address
types, and writes results to tx_address.

Config keys (config_type='queue'):
    funder_wrk_cnt_threads           - desired worker thread count (0 = idle)
    funder_wrk_cnt_prefetch          - RabbitMQ prefetch per worker channel
    funder_wrk_supervisor_poll_sec   - supervisor config poll interval
    funder_wrk_poll_idle_sec         - worker sleep when no message available
    funder_wrk_reconnect_sec         - delay before reconnecting after errors
    funder_wrk_shutdown_timeout_sec  - max wait for worker thread on shutdown
    funder_wrk_api_timeout_sec       - Solscan API request timeout
    funder_wrk_api_delay_sec         - delay between individual API calls
    funder_wrk_batch_delay_sec       - delay between processing batches
    funder_wrk_deadlock_max_retries  - max deadlock retry attempts
    funder_wrk_deadlock_base_delay   - initial deadlock retry delay (seconds)

Manual modes (run directly, not as service):
    python guide-funder.py --sync-db-missing
    python guide-funder.py --sync-db-missing --limit 500
    python guide-funder.py --sync-db-missing-metadata
    python guide-funder.py --dry-run
"""

import argparse
import json
import os
import random
import re
import sys
import time
import threading
import requests
import pika
import mysql.connector
from mysql.connector import Error as MySQLError
from datetime import datetime
from typing import Optional, Dict, List, Set


# =============================================================================
# Static config (from common.config → guide-config.json)
# =============================================================================

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from t16o_exchange.guide.common.config import (
    get_db_config, get_rabbitmq_config, get_solscan_config,
    get_queue_names, get_retry_config, nack_with_retry,
    load_db_config, cfg_int, cfg_float,
)

_solscan            = get_solscan_config()
_rmq                = get_rabbitmq_config()
_queues             = get_queue_names('funder')
_retry              = get_retry_config()

SOLSCAN_API_BASE    = _solscan['api_base']
SOLSCAN_API_TOKEN   = _solscan['token']
RABBITMQ_HOST       = _rmq['host']
RABBITMQ_PORT       = _rmq['port']
RABBITMQ_USER       = _rmq['user']
RABBITMQ_PASS       = _rmq['password']
RABBITMQ_VHOST      = _rmq['vhost']
RABBITMQ_HEARTBEAT  = _rmq['heartbeat']
RABBITMQ_BLOCKED_TIMEOUT = _rmq['blocked_timeout']
DB_FALLBACK_RETRY_SEC = _retry['db_fallback_retry_sec']
REQUEST_QUEUE       = _queues['request']
RESPONSE_QUEUE      = _queues['response']
DLQ_QUEUE           = _queues['dlq']
DB_CONFIG           = get_db_config()

# SOL token addresses for funding detection
SOL_TOKEN   = 'So11111111111111111111111111111111111111111'
SOL_TOKEN_2 = 'So11111111111111111111111111111111111111112'

# Known program addresses to skip
KNOWN_PROGRAMS = {
    '11111111111111111111111111111111',
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
    'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',
    'ComputeBudget111111111111111111111111111111',
    'So11111111111111111111111111111111111111111',
    'So11111111111111111111111111111111111111112',
}

SKIP_PREFIXES = ('jitodontfront', 'Jito')


# =============================================================================
# Helpers
# =============================================================================

def log(tag, msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}][{tag}] {msg}", flush=True)


# get_config_int / get_config_float removed — use load_db_config + cfg_int/cfg_float from common.config


def db_connect():
    return mysql.connector.connect(**DB_CONFIG)


def rmq_connect():
    creds = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, virtual_host=RABBITMQ_VHOST,
        credentials=creds,
        heartbeat=RABBITMQ_HEARTBEAT,
        blocked_connection_timeout=RABBITMQ_BLOCKED_TIMEOUT,
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=DLQ_QUEUE, durable=True,
                     arguments={'x-max-priority': 10, 'x-message-ttl': 86400000})
    ch.queue_declare(queue=REQUEST_QUEUE, durable=True,
                     arguments={'x-max-priority': 10,
                                'x-dead-letter-exchange': '',
                                'x-dead-letter-routing-key': DLQ_QUEUE})
    ch.queue_declare(queue=RESPONSE_QUEUE, durable=True,
                     arguments={'x-max-priority': 10})
    return conn, ch


def should_skip_address(address: str) -> bool:
    if address in KNOWN_PROGRAMS:
        return True
    for prefix in SKIP_PREFIXES:
        if address.startswith(prefix):
            return True
    return False


def execute_with_deadlock_retry(cursor, conn, query, params=None,
                                 max_retries=5, base_delay=0.1):
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
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
                time.sleep(delay)
            else:
                raise
    return 0


# =============================================================================
# Request logging (billing)
# =============================================================================

def log_worker_request(cursor, conn, request_id, correlation_id,
                       batch_size, priority, api_key_id):
    if api_key_id is not None:
        cursor.execute(
            "SELECT id FROM tx_request_log "
            "WHERE request_id=%s AND target_worker='funder' AND api_key_id=%s",
            (request_id, api_key_id))
    else:
        cursor.execute(
            "SELECT id FROM tx_request_log "
            "WHERE request_id=%s AND target_worker='funder' AND api_key_id IS NULL",
            (request_id,))
    row = cursor.fetchone()
    if row:
        return row['id']

    cursor.execute(
        "INSERT INTO tx_request_log "
        "(request_id, correlation_id, api_key_id, source, target_worker, "
        " action, priority, status, payload_summary) "
        "VALUES (%s,%s,%s,'queue','funder','process',%s,'processing',%s)",
        (request_id, correlation_id, api_key_id, priority,
         json.dumps({'batch_size': batch_size, 'source': 'queue'})))
    conn.commit()
    return cursor.lastrowid


def update_worker_request(cursor, conn, log_id, status, result=None):
    cursor.execute(
        "UPDATE tx_request_log SET status=%s, result_summary=%s, completed_at=NOW() WHERE id=%s",
        (status, json.dumps(result) if result else None, log_id))
    conn.commit()


def log_daemon_request(cursor, conn, action, batch_size):
    import uuid
    request_id = f"daemon-funder-{uuid.uuid4().hex[:12]}"
    cursor.execute(
        "INSERT INTO tx_request_log "
        "(request_id, correlation_id, api_key_id, source, target_worker, "
        " action, priority, status, payload_summary) "
        "VALUES (%s,%s,NULL,'daemon','funder',%s,5,'processing',%s)",
        (request_id, request_id, action,
         json.dumps({'batch_size': batch_size})))
    conn.commit()
    log_id = cursor.lastrowid
    log('DAEMON', f"request_log id={log_id}")
    return log_id


# =============================================================================
# Solscan API
# =============================================================================

def solscan_session():
    s = requests.Session()
    s.headers['token'] = SOLSCAN_API_TOKEN
    return s


def fetch_account_metadata_multi(session, addresses, api_timeout, limit=50):
    if not addresses:
        return {}
    addresses = addresses[:limit]
    url = f"{SOLSCAN_API_BASE}/account/metadata/multi"
    params = [('address[]', addr) for addr in addresses]
    try:
        r = session.get(url, params=params, timeout=api_timeout)
        r.raise_for_status()
        result = r.json()
        metadata_map = {}
        if result.get('success') and result.get('data'):
            for item in result['data']:
                addr = item.get('account_address')
                if addr:
                    metadata_map[addr] = item
        return metadata_map
    except Exception as e:
        log('API', f"account/metadata/multi error: {e}")
        return {}


def fetch_account_metadata(session, address, api_timeout):
    url = f"{SOLSCAN_API_BASE}/account/metadata"
    try:
        r = session.get(url, params={'address': address}, timeout=api_timeout)
        r.raise_for_status()
        result = r.json()
        if result.get('success') and result.get('data'):
            return result['data']
    except Exception as e:
        log('API', f"account/metadata error for {address[:12]}...: {e}")
    return None


def fetch_account_transfers(session, address, page_size, api_timeout, token_filter=None):
    valid_sizes = [10, 20, 30, 40, 60, 100]
    actual_size = min([s for s in valid_sizes if s >= page_size], default=100)
    url = f"{SOLSCAN_API_BASE}/account/transfer"
    params = {
        'address': address, 'page': 1, 'page_size': actual_size,
        'sort_by': 'block_time', 'sort_order': 'asc'
    }
    if token_filter:
        params['token'] = token_filter
    try:
        r = session.get(url, params=params, timeout=api_timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log('API', f"account/transfer error for {address[:12]}...: {e}")
    return None


def find_funding_wallet(target_address, transfers_data):
    if not transfers_data.get('success') or not transfers_data.get('data'):
        return None
    for record in transfers_data['data']:
        token_addr = record.get('token_address', '')
        flow = record.get('flow', '')
        to_addr = record.get('to_address', '')
        from_addr = record.get('from_address', '')
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
# Address processing (shared by queue consumer and manual modes)
# =============================================================================

def classify_address_type(label, tags, account_type):
    """Derive address_type from Solscan metadata. Returns mapped_type or None."""
    tags_lower = [t.lower() for t in tags] if tags else []
    label_lower = (label or '').lower()

    # Vault check first — vault authorities/LP vaults often carry a 'pool' tag
    # but are NOT pools. Exclude labels where "vault" is a token name inside parens
    # e.g., "Pump.fun AMM (VAULT-WSOL) Market" is a legit pool.
    if ('vault' in label_lower
            and not re.search(r'\([^)]*vault[^)]*\)', label_lower)):
        return 'vault'
    if 'pool' in tags_lower:
        return 'pool'
    if 'market' in tags_lower:
        return 'market'
    if any('vault' in tag for tag in tags_lower):
        return 'vault'
    if 'lptoken' in tags_lower:
        return 'lp_token'
    if 'dex_wallet' in tags_lower:
        return 'dex_wallet'
    if 'fee_vault' in tags_lower:
        return 'fee_vault'
    if 'arbitrage_bot' in tags_lower:
        return 'bot'
    if 'token_creator' in tags_lower:
        return 'wallet'
    if 'memecoin' in tags_lower:
        return 'mint'
    if account_type == 'token_account':
        return 'ata'
    if account_type == 'mint':
        return 'mint'
    if account_type == 'address':
        return 'wallet'
    return None


def ensure_addresses_exist(cursor, conn, addresses, max_retries, base_delay):
    if not addresses:
        return
    placeholders = ','.join(['%s'] * len(addresses))
    cursor.execute(f"SELECT address FROM tx_address WHERE address IN ({placeholders})", addresses)
    existing = {row['address'] for row in cursor.fetchall()}
    new_addresses = [addr for addr in addresses if addr not in existing]
    if new_addresses:
        values = [(addr, 'unknown') for addr in new_addresses]
        for attempt in range(max_retries):
            try:
                cursor.executemany(
                    "INSERT INTO tx_address (address, address_type) VALUES (%s, %s)",
                    values)
                conn.commit()
                break
            except mysql.connector.Error as e:
                if e.errno in (1213, 1205) and attempt < max_retries - 1:
                    conn.rollback()
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
                    time.sleep(delay)
                else:
                    raise


def claim_addresses(cursor, conn, addresses, force=False, max_retries=5, base_delay=0.1):
    if not addresses:
        return []
    placeholders = ','.join(['%s'] * len(addresses))
    if force:
        condition = f"init_tx_fetched != 2 OR init_tx_fetched IS NULL"
    else:
        condition = f"init_tx_fetched = 0 OR init_tx_fetched IS NULL"

    # Batch claim: single UPDATE for all addresses
    execute_with_deadlock_retry(cursor, conn,
        f"UPDATE tx_address SET init_tx_fetched = 2 "
        f"WHERE address IN ({placeholders}) AND ({condition})",
        addresses, max_retries, base_delay)

    # Return which ones were actually claimed
    cursor.execute(
        f"SELECT address FROM tx_address WHERE address IN ({placeholders}) AND init_tx_fetched = 2",
        addresses)
    return [row['address'] if isinstance(row, dict) else row[0] for row in cursor.fetchall()]


def mark_addresses_initialized(cursor, conn, addresses, max_retries=5, base_delay=0.1):
    if not addresses:
        return
    placeholders = ','.join(['%s'] * len(addresses))
    execute_with_deadlock_retry(cursor, conn,
        f"UPDATE tx_address SET init_tx_fetched = 1 WHERE address IN ({placeholders})",
        addresses, max_retries, base_delay)


def save_funding_info(tag, cursor, conn, target_address, funding_info,
                      request_log_id=None, max_retries=5, base_delay=0.1):
    """Save funding info + metadata to tx_address. Creates funder record if needed."""
    funder = funding_info['funder']
    amount = funding_info.get('amount')
    block_time = funding_info.get('block_time')
    label = funding_info.get('label')
    tags = funding_info.get('tags')
    account_type = funding_info.get('account_type')
    active_age = funding_info.get('active_age')

    for attempt in range(max_retries):
        try:
            # Upsert funder address
            cursor.execute("SELECT id FROM tx_address WHERE address = %s", (funder,))
            funder_row = cursor.fetchone()
            if funder_row:
                funder_id = funder_row['id'] if isinstance(funder_row, dict) else funder_row[0]
            else:
                try:
                    cursor.execute(
                        "INSERT INTO tx_address (address, address_type, init_tx_fetched, request_log_id) "
                        "VALUES (%s, 'wallet', 1, %s)", (funder, request_log_id))
                    funder_id = cursor.lastrowid
                except mysql.connector.IntegrityError:
                    # Another thread inserted it between our SELECT and INSERT
                    cursor.execute("SELECT id FROM tx_address WHERE address = %s", (funder,))
                    funder_row = cursor.fetchone()
                    funder_id = funder_row['id'] if isinstance(funder_row, dict) else funder_row[0]

            # Get target address ID for pool record creation
            cursor.execute("SELECT id FROM tx_address WHERE address = %s", (target_address,))
            target_row = cursor.fetchone()
            target_id = target_row['id'] if target_row else None

            # Build dynamic UPDATE
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

            mapped_type = None

            if tags is not None:
                update_fields.append("account_tags = %s")
                update_values.append(json.dumps(tags) if tags else None)
                mapped_type = classify_address_type(label, tags, account_type)

                if mapped_type == 'wallet' and tags and 'token_creator' in [t.lower() for t in tags]:
                    update_fields.append("label = COALESCE(label, %s)")
                    update_values.append('Token Creator')

                if mapped_type:
                    update_fields.append(
                        "address_type = CASE WHEN address_type IN ('unknown', 'wallet', 'ata', 'program', 'mint') "
                        "THEN %s ELSE address_type END")
                    update_values.append(mapped_type)

            elif account_type is not None:
                mapped_type = account_type
                if account_type == 'token_account':
                    mapped_type = 'ata'
                elif account_type == 'address':
                    mapped_type = 'wallet'
                update_fields.append(
                    "address_type = CASE WHEN address_type = 'unknown' THEN %s ELSE address_type END")
                update_values.append(mapped_type)

            if active_age is not None:
                update_fields.append("active_age_days = %s")
                update_values.append(active_age)

            update_values.append(target_address)
            cursor.execute(
                f"UPDATE tx_address SET {', '.join(update_fields)} "
                "WHERE address = %s AND funded_by_address_id IS NULL",
                update_values)

            # Create tx_pool record for pool/lp_token addresses
            if mapped_type in ('pool', 'lp_token') and target_id:
                cursor.execute("SELECT 1 FROM tx_pool WHERE pool_address_id = %s", (target_id,))
                if not cursor.fetchone():
                    program_id = None
                    token1_id = None
                    token2_id = None
                    if label:
                        paren_match = re.match(r'^(.+?)\s*\((.+?)\)', label)
                        if paren_match:
                            dex_name = paren_match.group(1).strip()
                            token_part = paren_match.group(2).strip()
                            if dex_name:
                                cursor.execute(
                                    "SELECT id FROM tx_program WHERE name = %s LIMIT 1", (dex_name,))
                                prog_row = cursor.fetchone()
                                if prog_row:
                                    program_id = prog_row['id']
                            symbols = [s.strip() for s in token_part.split('-', 1)]
                            for i, sym in enumerate(symbols[:2]):
                                if sym:
                                    cursor.execute(
                                        "SELECT id FROM tx_token WHERE token_symbol = %s LIMIT 1", (sym,))
                                    tok_row = cursor.fetchone()
                                    if tok_row:
                                        if i == 0:
                                            token1_id = tok_row['id']
                                        else:
                                            token2_id = tok_row['id']
                    cursor.execute(
                        "INSERT IGNORE INTO tx_pool (pool_address_id, program_id, token1_id, token2_id, pool_label) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        (target_id, program_id, token1_id, token2_id, label))

            conn.commit()
            break
        except mysql.connector.Error as e:
            if e.errno in (1213, 1205) and attempt < max_retries - 1:
                conn.rollback()
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
                log(tag, f"Deadlock in save_funding_info (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {delay:.2f}s...")
                time.sleep(delay)
            else:
                raise


def process_addresses(tag, cursor, conn, session, addresses,
                      api_timeout, api_delay, max_retries, base_delay,
                      force=False, request_log_id=None):
    """
    Core processing: fetch metadata, find funders, save results.
    Returns dict with processed/funders_found/funders_not_found.
    """
    result = {'processed': 0, 'claimed': 0, 'skipped': 0,
              'funders_found': 0, 'funders_not_found': 0, 'error': None}

    if not addresses:
        return result

    # Filter known programs
    addresses = [a for a in addresses if not should_skip_address(a)]
    if not addresses:
        return result

    # Ensure all addresses exist in DB
    ensure_addresses_exist(cursor, conn, addresses, max_retries, base_delay)

    # Get address info from DB
    placeholders = ','.join(['%s'] * len(addresses))
    cursor.execute(
        f"SELECT id, address, address_type, init_tx_fetched "
        f"FROM tx_address WHERE address IN ({placeholders})", addresses)
    address_info = {row['address']: row for row in cursor.fetchall()}

    # Filter to candidates needing lookup
    candidates = []
    for addr in addresses:
        info = address_info.get(addr, {})
        if force or not info.get('init_tx_fetched'):
            candidates.append(addr)
        else:
            result['skipped'] += 1

    log(tag, f"Candidates: {len(candidates)}{' (force)' if force else ''}, "
        f"skipped: {result['skipped']}")

    # Claim addresses atomically
    claimed = claim_addresses(cursor, conn, candidates, force, max_retries, base_delay)
    result['claimed'] = len(claimed)
    if len(claimed) < len(candidates):
        result['skipped'] += len(candidates) - len(claimed)

    if not claimed:
        return result

    # For explicit API calls, update request_log_id for billing
    if force and request_log_id and claimed:
        ph = ','.join(['%s'] * len(claimed))
        execute_with_deadlock_retry(cursor, conn,
            f"UPDATE tx_address SET request_log_id = %s WHERE address IN ({ph})",
            [request_log_id] + claimed, max_retries, base_delay)

    # Get batch_size from config table (runtime-editable)
    _batch_cfg = load_db_config(cursor, 'batch')
    batch_size = cfg_int(_batch_cfg, 'funder_batch_size', 10)
    total = len(claimed)
    initialized = []

    for batch_start in range(0, total, batch_size):
        batch = claimed[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (total + batch_size - 1) // batch_size

        log(tag, f"Batch {batch_num}/{total_batches}: {len(batch)} addresses")

        # Batch metadata API
        metadata_map = fetch_account_metadata_multi(session, batch, api_timeout)
        log(tag, f"  Got metadata for {len(metadata_map)} addresses")

        for i, addr in enumerate(batch):
            idx = batch_start + i + 1
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
                log(tag, f"  [{idx}/{total}] {addr[:20]}... -> "
                    f"funded by {funding_info['funder'][:16]}...")
                save_funding_info(tag, cursor, conn, addr, funding_info,
                                  request_log_id, max_retries, base_delay)
                result['funders_found'] += 1
            else:
                log(tag, f"  [{idx}/{total}] {addr[:20]}... -> no funder")
                result['funders_not_found'] += 1

            initialized.append(addr)
            result['processed'] += 1

        # Delay between API batch calls
        if batch_start + batch_size < total:
            time.sleep(api_delay)

    # Mark all processed
    if initialized:
        mark_addresses_initialized(cursor, conn, initialized, max_retries, base_delay)
        log(tag, f"Marked {len(initialized)} addresses as processed")

    return result


# =============================================================================
# Worker thread
# =============================================================================

class WorkerThread(threading.Thread):
    def __init__(self, worker_id, prefetch, dry_run, stop_event,
                 poll_idle_sec, reconnect_sec, api_timeout_sec,
                 api_delay_sec, batch_delay_sec,
                 deadlock_max_retries, deadlock_base_delay):
        super().__init__(daemon=True)
        self.tag = f"W-{worker_id}"
        self.worker_id = worker_id
        self.prefetch = prefetch
        self.dry_run = dry_run
        self.stop_event = stop_event
        self.poll_idle_sec = poll_idle_sec
        self.reconnect_sec = reconnect_sec
        self.api_timeout_sec = api_timeout_sec
        self.api_delay_sec = api_delay_sec
        self.batch_delay_sec = batch_delay_sec
        self.deadlock_max_retries = deadlock_max_retries
        self.deadlock_base_delay = deadlock_base_delay

    def run(self):
        log(self.tag, f"Starting (prefetch={self.prefetch})")
        session = solscan_session()
        db_conn = None
        cursor = None

        while not self.stop_event.is_set():
            try:
                # Connect DB
                if db_conn is None:
                    db_conn = db_connect()
                    cursor = db_conn.cursor(dictionary=True)
                    log(self.tag, "DB connected")

                # Connect RabbitMQ
                rmq_conn, ch = rmq_connect()
                ch.basic_qos(prefetch_count=self.prefetch)
                log(self.tag, "RabbitMQ connected, consuming...")

                while not self.stop_event.is_set():
                    method, properties, body = ch.basic_get(queue=REQUEST_QUEUE, auto_ack=False)
                    if method is None:
                        time.sleep(self.poll_idle_sec)
                        rmq_conn.process_data_events(time_limit=0)
                        continue

                    self._handle_message(ch, method, properties, body, cursor, db_conn, session)

                    if self.batch_delay_sec > 0:
                        time.sleep(self.batch_delay_sec)

                try:
                    rmq_conn.close()
                except Exception:
                    pass

            except MySQLError as e:
                log(self.tag, f"MySQL error: {e}, reconnecting in {self.reconnect_sec}s...")
                db_conn = None
                cursor = None
                time.sleep(self.reconnect_sec)
            except pika.exceptions.AMQPConnectionError as e:
                log(self.tag, f"RabbitMQ lost: {e}, reconnecting in {self.reconnect_sec}s...")
                time.sleep(self.reconnect_sec)
            except Exception as e:
                log(self.tag, f"Unexpected error: {e}, retrying in {self.reconnect_sec}s...")
                time.sleep(self.reconnect_sec)

        if db_conn:
            try:
                db_conn.close()
            except Exception:
                pass
        session.close()
        log(self.tag, "Stopped")

    def _handle_message(self, ch, method, properties, body, cursor, db_conn, session):
        worker_log_id = None
        try:
            import uuid
            msg = json.loads(body.decode('utf-8'))
            request_id     = msg.get('request_id', str(uuid.uuid4()))
            correlation_id = msg.get('correlation_id', request_id)
            api_key_id     = msg.get('api_key_id')
            action         = msg.get('action', 'process')
            priority       = msg.get('priority', 5)
            batch          = msg.get('batch', {})

            # ── sync-db-missing: supervisor-scheduled DB poll ──
            if action == 'sync-db-missing':
                self._handle_db_poll(ch, method, cursor, db_conn, session)
                return

            # ── Normal queue message: explicit addresses ──
            addresses = batch.get('addresses', []) or batch.get('funder_addresses', [])
            address_ids = batch.get('address_ids', [])

            if address_ids and not addresses:
                ph = ','.join(['%s'] * len(address_ids))
                cursor.execute(f"SELECT address FROM tx_address WHERE id IN ({ph})", address_ids)
                addresses = [row['address'] for row in cursor.fetchall()]

            log(self.tag, f"Request {request_id[:8]} (action={action}, "
                f"addresses={len(addresses)})")

            worker_log_id = log_worker_request(
                cursor, db_conn, request_id, correlation_id,
                len(addresses), priority, api_key_id)

            if not addresses:
                resp = {'processed': 0, 'funders_found': 0, 'funders_not_found': 0}
                update_worker_request(cursor, db_conn, worker_log_id, 'completed', resp)
                self._publish_response(ch, request_id, correlation_id, 'completed',
                                       resp, worker_log_id)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            force = (action == 'process')
            result = process_addresses(
                self.tag, cursor, db_conn, session, addresses,
                self.api_timeout_sec, self.api_delay_sec,
                self.deadlock_max_retries, self.deadlock_base_delay,
                force=force, request_log_id=worker_log_id)

            if result.get('error'):
                status = 'failed'
                resp = {'processed': 0, 'error': result['error']}
            else:
                status = 'completed'
                resp = {
                    'processed': result['processed'],
                    'funders_found': result['funders_found'],
                    'funders_not_found': result['funders_not_found'],
                }

            update_worker_request(cursor, db_conn, worker_log_id, status, resp)
            self._publish_response(ch, request_id, correlation_id, status,
                                   resp, worker_log_id)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except MySQLError:
            nack_with_retry(ch, method.delivery_tag, properties,
                            log_fn=lambda msg: log(self.tag, f"[DB ERROR] {msg}"),
                            queue_name=REQUEST_QUEUE, body=body)
            raise
        except Exception as e:
            log(self.tag, f"ERROR processing message -> DLQ: {e}")
            import traceback
            traceback.print_exc()
            if worker_log_id:
                try:
                    update_worker_request(cursor, db_conn, worker_log_id, 'failed', {'error': str(e)})
                except Exception:
                    pass
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _handle_db_poll(self, ch, method, cursor, db_conn, session):
        """Handle supervisor-scheduled DB poll — loops until no more unfunded addresses."""
        total_processed = 0
        total_found = 0
        total_not_found = 0
        batch_num = 0

        while not self.stop_event.is_set():
            _batch_cfg = load_db_config(cursor, 'batch')
            batch_size = cfg_int(_batch_cfg, 'funder_batch_size', 10)

            cursor.execute(
                "SELECT address FROM tx_address "
                "WHERE funded_by_address_id IS NULL "
                "  AND (init_tx_fetched = 0 OR init_tx_fetched IS NULL) "
                "ORDER BY id ASC LIMIT %s", (batch_size,))
            candidates = [row['address'] for row in cursor.fetchall()]

            if not candidates:
                break

            # Filter system addresses and mark them done
            skip = [a for a in candidates if should_skip_address(a)]
            if skip:
                mark_addresses_initialized(cursor, db_conn, skip,
                                           self.deadlock_max_retries, self.deadlock_base_delay)
            candidates = [a for a in candidates if not should_skip_address(a)]

            if not candidates:
                continue

            batch_num += 1
            log(self.tag, f"DB poll batch {batch_num}: {len(candidates)} unfunded addresses")

            result = process_addresses(
                self.tag, cursor, db_conn, session, candidates,
                self.api_timeout_sec, self.api_delay_sec,
                self.deadlock_max_retries, self.deadlock_base_delay)

            total_processed += result['processed']
            total_found += result['funders_found']
            total_not_found += result['funders_not_found']

            log(self.tag, f"  batch {batch_num}: {result['processed']} processed, "
                f"{result['funders_found']} found, {result['funders_not_found']} not found")

            if self.batch_delay_sec > 0:
                time.sleep(self.batch_delay_sec)

        if total_processed > 0:
            log(self.tag, f"DB poll complete: {batch_num} batches, {total_processed} processed, "
                f"{total_found} found, {total_not_found} not found")
            try:
                dl_id = log_daemon_request(cursor, db_conn,
                                           'sync-db-missing', total_processed)
                update_worker_request(cursor, db_conn, dl_id, 'completed', {
                    'batches': batch_num,
                    'processed': total_processed,
                    'funders_found': total_found,
                    'funders_not_found': total_not_found
                })
            except Exception as e:
                log(self.tag, f"Failed to log request: {e}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _publish_response(self, ch, request_id, correlation_id, status, result, funder_log_id):
        body = json.dumps({
            'request_id':     request_id,
            'correlation_id': correlation_id,
            'request_log_id': funder_log_id,
            'worker':         'funder',
            'status':         status,
            'timestamp':      datetime.now().isoformat() + 'Z',
            'result':         result,
        })
        ch.basic_publish(
            exchange='', routing_key=RESPONSE_QUEUE,
            body=body.encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2, content_type='application/json'))


# =============================================================================
# Supervisor
# =============================================================================

def read_config(cursor):
    cfg = load_db_config(cursor, 'queue')
    return {
        'threads':          cfg_int(cfg, 'funder_wrk_cnt_threads', 0),
        'prefetch':         cfg_int(cfg, 'funder_wrk_cnt_prefetch', 5),
        'supervisor_poll':  cfg_float(cfg, 'funder_wrk_supervisor_poll_sec', 5.0),
        'poll_idle':        cfg_float(cfg, 'funder_wrk_poll_idle_sec', 0.25),
        'reconnect':        cfg_float(cfg, 'funder_wrk_reconnect_sec', 5.0),
        'shutdown_timeout': cfg_float(cfg, 'funder_wrk_shutdown_timeout_sec', 10.0),
        'api_timeout':      cfg_float(cfg, 'funder_wrk_api_timeout_sec', 60.0),
        'api_delay':        cfg_float(cfg, 'funder_wrk_api_delay_sec', 0.30),
        'batch_delay':      cfg_float(cfg, 'funder_wrk_batch_delay_sec', 1.0),
        'deadlock_max':     cfg_int(cfg, 'funder_wrk_deadlock_max_retries', 5),
        'deadlock_delay':   cfg_float(cfg, 'funder_wrk_deadlock_base_delay', 0.1),
        'db_poll':          cfg_float(cfg, 'funder_wrk_db_poll_sec', 0),
    }


def run_supervisor(dry_run=False):
    print(f"""
+-----------------------------------------------------------+
|  Guide Funder - Supervisor                                |
|  vhost: {RABBITMQ_VHOST:<10}  queue: {REQUEST_QUEUE:<24} |
+-----------------------------------------------------------+
""", flush=True)

    workers = {}
    next_id = 1
    svr_conn = None
    svr_cursor = None
    svr_rmq_conn = None
    svr_rmq_ch = None
    last_db_poll = 0.0

    def ensure_svr_db():
        nonlocal svr_conn, svr_cursor
        try:
            if svr_conn is not None:
                svr_conn.ping(reconnect=False, attempts=1, delay=0)
                return True
        except Exception:
            svr_conn = None
        try:
            svr_conn = db_connect()
            svr_cursor = svr_conn.cursor(dictionary=True)
            log('SVR', 'DB connected')
            return True
        except Exception as e:
            log('SVR', f'DB connect failed: {e}')
            return False

    def ensure_svr_rmq():
        nonlocal svr_rmq_conn, svr_rmq_ch
        try:
            if svr_rmq_conn is not None and svr_rmq_conn.is_open:
                return True
        except Exception:
            svr_rmq_conn = None
        try:
            svr_rmq_conn, svr_rmq_ch = rmq_connect()
            log('SVR', 'RabbitMQ connected (publisher)')
            return True
        except Exception as e:
            log('SVR', f'RabbitMQ connect failed: {e}')
            return False

    def publish_db_poll():
        """Publish a sync-db-missing message to the request queue."""
        import uuid
        # Skip if queue already has pending messages (workers loop until drained)
        result = svr_rmq_ch.queue_declare(queue=REQUEST_QUEUE, passive=True)
        if result.method.message_count > 0:
            return
        msg = json.dumps({
            'request_id': f"dbpoll-{uuid.uuid4().hex[:12]}",
            'action': 'sync-db-missing',
        })
        svr_rmq_ch.basic_publish(
            exchange='', routing_key=REQUEST_QUEUE,
            body=msg.encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2, content_type='application/json'))
        log('SVR', 'Published sync-db-missing to queue')

    try:
        while True:
            if not ensure_svr_db():
                time.sleep(DB_FALLBACK_RETRY_SEC)
                continue

            cfg = read_config(svr_cursor)

            # Prune dead workers
            dead = [wid for wid, (w, _) in workers.items() if not w.is_alive()]
            for wid in dead:
                log('SVR', f'Worker W-{wid} died, removing')
                del workers[wid]

            active = len(workers)
            if active != cfg['threads']:
                log('SVR', f"Config: threads={cfg['threads']} prefetch={cfg['prefetch']} | active={active}")

            # Scale up
            while len(workers) < cfg['threads']:
                stop_evt = threading.Event()
                w = WorkerThread(
                    next_id, cfg['prefetch'], dry_run, stop_evt,
                    cfg['poll_idle'], cfg['reconnect'], cfg['api_timeout'],
                    cfg['api_delay'], cfg['batch_delay'],
                    cfg['deadlock_max'], cfg['deadlock_delay'])
                w.start()
                workers[next_id] = (w, stop_evt)
                next_id += 1

            # Scale down (stop newest first)
            while len(workers) > cfg['threads']:
                wid = max(workers.keys())
                wthread, stop_evt = workers.pop(wid)
                log('SVR', f'Stopping W-{wid}...')
                stop_evt.set()

            # DB poll scheduler: publish sync-db-missing at configured interval
            now = time.time()
            if cfg['db_poll'] > 0 and active > 0 and (now - last_db_poll) >= cfg['db_poll']:
                if ensure_svr_rmq():
                    try:
                        publish_db_poll()
                        last_db_poll = now
                    except Exception as e:
                        log('SVR', f'Failed to publish db poll: {e}')
                        svr_rmq_conn = None

            time.sleep(cfg['supervisor_poll'])

    except KeyboardInterrupt:
        log('SVR', 'Shutting down...')

    shutdown_timeout = cfg['shutdown_timeout'] if 'cfg' in dir() else 10.0

    for wid, (w, stop_evt) in workers.items():
        stop_evt.set()
    for wid, (w, _) in workers.items():
        w.join(timeout=shutdown_timeout)
        if w.is_alive():
            log('SVR', f'W-{wid} did not stop in time')

    if svr_rmq_conn:
        try:
            svr_rmq_conn.close()
        except Exception:
            pass
    if svr_conn:
        try:
            svr_conn.close()
        except Exception:
            pass

    log('SVR', 'Shutdown complete')


# =============================================================================
# Manual modes (sync-db-missing, sync-db-missing-metadata)
# =============================================================================

def run_sync_db_missing(args):
    """Query DB for addresses missing funders and process them."""
    log('SYNC', f"Mode: sync-db-missing, limit={args.limit or 'unlimited'}, dry_run={args.dry_run}")

    conn = db_connect()
    cursor = conn.cursor(dictionary=True)
    session = solscan_session()

    total_limit = args.limit if args.limit > 0 else float('inf')
    processed = 0
    funders_found = 0
    funders_not_found = 0
    batch_num = 0

    try:
        while processed < total_limit:
            _batch_cfg = load_db_config(cursor, 'batch')
            batch_size = cfg_int(_batch_cfg, 'funder_batch_size', 10)
            remaining = int(min(batch_size, total_limit - processed))

            cursor.execute(
                "SELECT address, request_log_id FROM tx_address "
                "WHERE funded_by_address_id IS NULL "
                "  AND (init_tx_fetched = 0 OR init_tx_fetched IS NULL) "
                "ORDER BY id ASC LIMIT %s", (remaining,))
            candidates = cursor.fetchall()

            if not candidates:
                log('SYNC', "No more addresses to process")
                break

            # Filter system addresses
            skip_addrs = [c['address'] for c in candidates if should_skip_address(c['address'])]
            candidates = [c for c in candidates if not should_skip_address(c['address'])]

            if skip_addrs and not args.dry_run:
                mark_addresses_initialized(cursor, conn, skip_addrs)
                log('SYNC', f"Skipped {len(skip_addrs)} system addresses")

            if not candidates:
                continue

            batch_num += 1
            addresses = [c['address'] for c in candidates]

            log('SYNC', f"Batch {batch_num}: {len(addresses)} candidates")

            # Claim
            claimed = claim_addresses(cursor, conn, addresses)
            if not claimed:
                time.sleep(0.1)
                continue

            if args.dry_run:
                log('SYNC', f"DRY RUN - would process {len(claimed)} addresses")
                processed += len(claimed)
                continue

            # Batch API
            metadata_map = fetch_account_metadata_multi(session, claimed, 60)
            log('SYNC', f"  Got metadata for {len(metadata_map)} addresses")

            initialized = []
            for addr in claimed:
                if processed >= total_limit:
                    break

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

                rlid = next((c['request_log_id'] for c in candidates if c['address'] == addr), None)
                if funding_info:
                    log('SYNC', f"  {addr[:20]}... -> funded by {funding_info['funder'][:16]}...")
                    save_funding_info('SYNC', cursor, conn, addr, funding_info, rlid)
                    funders_found += 1
                else:
                    log('SYNC', f"  {addr[:20]}... -> no funder")
                    funders_not_found += 1

                initialized.append(addr)
                processed += 1

            mark_addresses_initialized(cursor, conn, initialized)

            if funders_found > 0:
                dl_id = log_daemon_request(cursor, conn, 'discover', len(claimed))
                update_worker_request(cursor, conn, dl_id, 'completed', {
                    'processed': len(initialized),
                    'funders_found': funders_found,
                    'funders_not_found': funders_not_found
                })

            _queue_cfg = load_db_config(cursor, 'queue')
            batch_delay = cfg_float(_queue_cfg, 'funder_wrk_batch_delay_sec', 0.1)
            time.sleep(batch_delay)

    except KeyboardInterrupt:
        log('SYNC', 'Interrupted')
    finally:
        session.close()
        conn.close()

    log('SYNC', f"Done: processed={processed}, found={funders_found}, not_found={funders_not_found}")


def run_sync_db_missing_metadata(args):
    """Backfill metadata for addresses that have funders but no tags."""
    log('META', f"Mode: sync-db-missing-metadata, limit={args.limit or 'unlimited'}, dry_run={args.dry_run}")

    conn = db_connect()
    cursor = conn.cursor(dictionary=True)
    session = solscan_session()

    total_limit = args.limit if args.limit > 0 else float('inf')
    processed = 0
    updated = 0
    not_found = 0

    try:
        while processed < total_limit:
            _batch_cfg = load_db_config(cursor, 'batch')
            batch_size = cfg_int(_batch_cfg, 'funder_batch_size', 10)
            remaining = int(min(batch_size, total_limit - processed))

            cursor.execute(
                "SELECT address, request_log_id FROM tx_address "
                "WHERE funded_by_address_id IS NOT NULL AND account_tags IS NULL "
                "  AND address_type IN ('pool', 'program', 'lp_token') "
                "ORDER BY id ASC LIMIT %s", (remaining,))
            candidates = cursor.fetchall()

            if not candidates:
                log('META', "No more addresses")
                break

            # Filter system addresses
            skip_addrs = [c['address'] for c in candidates if should_skip_address(c['address'])]
            candidates = [c for c in candidates if not should_skip_address(c['address'])]

            if skip_addrs and not args.dry_run:
                ph = ','.join(['%s'] * len(skip_addrs))
                cursor.execute(
                    f"UPDATE tx_address SET account_tags = '[]' WHERE address IN ({ph})",
                    skip_addrs)
                conn.commit()

            if not candidates:
                continue

            addresses = [c['address'] for c in candidates]

            if args.dry_run:
                log('META', f"DRY RUN - would fetch metadata for {len(addresses)} addresses")
                processed += len(addresses)
                continue

            metadata_map = fetch_account_metadata_multi(session, addresses, 60)
            log('META', f"Got metadata for {len(metadata_map)} addresses")

            for addr in addresses:
                if processed >= total_limit:
                    break

                metadata = metadata_map.get(addr)
                if metadata and metadata.get('account_tags'):
                    tags = metadata['account_tags']
                    label = metadata.get('account_label')
                    account_type = metadata.get('account_type')

                    update_fields = []
                    update_values = []

                    if label:
                        update_fields.append("label = COALESCE(label, %s)")
                        update_values.append(label)

                    update_fields.append("account_tags = %s")
                    update_values.append(json.dumps(tags))

                    mapped_type = classify_address_type(label, tags, account_type)
                    if mapped_type == 'wallet' and 'token_creator' in [t.lower() for t in tags]:
                        update_fields.append("label = COALESCE(label, %s)")
                        update_values.append('Token Creator')
                    if mapped_type:
                        update_fields.append(
                            "address_type = CASE WHEN address_type IN ('unknown', 'wallet', 'ata', 'program', 'mint') "
                            "THEN %s ELSE address_type END")
                        update_values.append(mapped_type)

                    update_values.append(addr)
                    cursor.execute(
                        f"UPDATE tx_address SET {', '.join(update_fields)} WHERE address = %s",
                        update_values)
                    conn.commit()

                    log('META', f"  {addr[:20]}... -> tags: {tags}")
                    updated += 1
                else:
                    cursor.execute(
                        "UPDATE tx_address SET account_tags = '[]' WHERE address = %s", (addr,))
                    conn.commit()
                    log('META', f"  {addr[:20]}... -> no tags")
                    not_found += 1

                processed += 1

            time.sleep(0.1)

    except KeyboardInterrupt:
        log('META', 'Interrupted')
    finally:
        session.close()
        conn.close()

    log('META', f"Done: processed={processed}, updated={updated}, not_found={not_found}")


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Guide Funder - Funding wallet discovery')
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview only, no DB changes')
    parser.add_argument('--sync-db-missing', action='store_true',
                        help='Query DB for addresses with no funder and process them')
    parser.add_argument('--sync-db-missing-metadata', action='store_true',
                        help='Backfill metadata for addresses with funders but no tags')
    parser.add_argument('--limit', type=int, default=0,
                        help='Max addresses to process in sync modes (0 = unlimited)')
    args = parser.parse_args()

    if args.sync_db_missing:
        return run_sync_db_missing(args)
    elif args.sync_db_missing_metadata:
        return run_sync_db_missing_metadata(args)
    else:
        return run_supervisor(dry_run=args.dry_run)


if __name__ == '__main__':
    sys.exit(main() or 0)
