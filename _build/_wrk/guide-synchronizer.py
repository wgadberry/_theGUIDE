#!/usr/bin/env python3
"""
Guide Synchronizer - Gap-filling worker for mint transaction history
(multi-threaded, config-driven)

Fills gaps in transaction coverage for a given mint. Different users request
different time windows, creating holes in our data. The synchronizer:
1. Fetches ALL on-chain signatures for a mint (newest → oldest known)
2. Stores them in t16o_db_staging.txs_sync
3. Diffs against our tx table to find missing signatures
4. Cascades missing sigs through the decoder → detailer pipeline

Triggered on-demand via RabbitMQ message (API call → gateway → synchronizer).

Config keys (config_type='queue'):
    synchronizer_wrk_cnt_threads          - desired worker thread count (0 = idle)
    synchronizer_wrk_supervisor_poll_sec  - supervisor config poll interval
    synchronizer_wrk_poll_idle_sec        - worker sleep when no messages
    synchronizer_wrk_reconnect_sec        - delay before reconnecting after errors
    synchronizer_wrk_shutdown_timeout_sec - max wait for worker thread on shutdown
    synchronizer_wrk_rpc_delay_sec        - delay between Chainstack RPC calls
    synchronizer_wrk_cascade_batch_size   - sigs per batch sent to decoder/detailer

Usage:
    python guide-synchronizer.py              # Supervisor + worker threads
    python guide-synchronizer.py --dry-run    # Preview only
"""

import argparse
import hashlib
import json
import os
import sys
import time
import threading
import uuid
import requests
import pika
import mysql.connector
from mysql.connector import Error as MySQLError
from datetime import datetime, timezone

# =============================================================================
# Static config (from common.config → guide-config.json)
# =============================================================================

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from t16o_exchange.guide.common.config import (
    get_db_config, get_rabbitmq_config, get_rpc_config,
    get_queue_names, get_retry_config, nack_with_retry,
    load_db_config, cfg_int, cfg_float,
)

_rmq                = get_rabbitmq_config()
_rpc                = get_rpc_config()
_retry              = get_retry_config()
_queues             = get_queue_names('synchronizer')
_decoder_queues     = get_queue_names('decoder')
_detailer_queues    = get_queue_names('detailer')

DB_CONFIG               = get_db_config()
RABBITMQ_HOST           = _rmq['host']
RABBITMQ_PORT           = _rmq['port']
RABBITMQ_USER           = _rmq['user']
RABBITMQ_PASS           = _rmq['password']
RABBITMQ_VHOST          = _rmq['vhost']
RABBITMQ_HEARTBEAT      = _rmq['heartbeat']
RABBITMQ_BLOCKED_TIMEOUT = _rmq['blocked_timeout']
DB_FALLBACK_RETRY_SEC   = _retry['db_fallback_retry_sec']

REQUEST_QUEUE           = _queues['request']
RESPONSE_QUEUE          = _queues['response']
DLQ_QUEUE               = _queues['dlq']
DECODER_REQUEST_QUEUE   = _decoder_queues['request']
DETAILER_REQUEST_QUEUE  = _detailer_queues['request']

RPC_URL                 = _rpc['url']

STAGING_SCHEMA          = 't16o_db_staging'
SYNC_TABLE              = 'txs_sync'


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
    # DLQ must be declared before request queue references it
    ch.queue_declare(queue=DLQ_QUEUE, durable=True,
                     arguments={'x-max-priority': 10, 'x-message-ttl': 86400000})
    ch.queue_declare(queue=REQUEST_QUEUE, durable=True,
                     arguments={'x-max-priority': 10,
                                'x-dead-letter-exchange': '',
                                'x-dead-letter-routing-key': DLQ_QUEUE})
    ch.queue_declare(queue=RESPONSE_QUEUE, durable=True,
                     arguments={'x-max-priority': 10})
    # Downstream queues (decoder/detailer) are declared by their own workers.
    # We only publish to them — no need to declare here.
    return conn, ch


def compute_sig_hash(signatures):
    """Compute SHA256 hash of sorted signatures for batch pairing"""
    sorted_sigs = '|'.join(sorted(signatures))
    return hashlib.sha256(sorted_sigs.encode()).hexdigest()


# =============================================================================
# Chainstack RPC
# =============================================================================

def fetch_signatures_rpc(session, address, limit=1000, before=None, until=None):
    """Fetch signatures from Solana RPC using getSignaturesForAddress"""
    params_obj = {"limit": min(limit, 1000)}
    if before:
        params_obj["before"] = before
    if until:
        params_obj["until"] = until

    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "getSignaturesForAddress",
        "params": [address, params_obj]
    }

    response = session.post(RPC_URL, json=payload)
    response.raise_for_status()
    return response.json()


def fetch_all_signatures(session, tag, address, until_sig=None, rpc_delay=0.2):
    """
    Generator that fetches all signatures for an address from newest to oldest.
    Stops when it hits until_sig (our oldest known signature) or runs out of history.
    Skips failed transactions.
    """
    before = None
    total_fetched = 0
    page = 0

    while True:
        page += 1
        try:
            response = fetch_signatures_rpc(
                session, address, limit=1000, before=before, until=until_sig
            )
        except requests.RequestException as e:
            log(tag, f"RPC error on page {page}: {e}")
            break

        if 'error' in response:
            log(tag, f"RPC returned error on page {page}: {response['error']}")
            break

        signatures = response.get('result', [])
        if not signatures:
            break

        for sig in signatures:
            if sig.get('err') is not None:
                continue  # Skip failed txs
            yield sig

        total_fetched += len(signatures)
        before = signatures[-1].get('signature') if signatures else None

        if len(signatures) < 1000:
            break  # Last page

        if rpc_delay > 0:
            time.sleep(rpc_delay)

        if page % 10 == 0:
            log(tag, f"  ... fetched {total_fetched} signatures so far (page {page})")


# =============================================================================
# Sync Processing
# =============================================================================

def resolve_mint(cursor, tag, mint_address=None, mint_symbol=None):
    """
    Resolve mint to (address_id, address_string).
    Returns (None, None) if not found.
    """
    if mint_address:
        cursor.execute(
            "SELECT id, address FROM tx_address WHERE address = %s",
            (mint_address,)
        )
        row = cursor.fetchone()
        if row:
            return row[0], row[1]
        log(tag, f"Mint address not found in tx_address: {mint_address}")
        return None, None

    if mint_symbol:
        cursor.execute("""
            SELECT a.id, a.address
            FROM tx_address a
            JOIN tx_token t ON t.mint_address_id = a.id
            WHERE t.token_symbol = %s
            LIMIT 1
        """, (mint_symbol,))
        row = cursor.fetchone()
        if row:
            return row[0], row[1]
        log(tag, f"Mint symbol not found: {mint_symbol}")
        return None, None

    return None, None


def find_oldest_signature(cursor, tag, mint_address_id):
    """
    Find the oldest transaction signature we have for this mint.
    Returns (signature, block_time) or (None, None) if we have no txs.
    """
    cursor.execute("""
        SELECT t.signature, t.block_time
        FROM tx_guide g
        JOIN tx t ON t.id = g.tx_id
        JOIN tx_token tk ON tk.id = g.token_id
        WHERE tk.mint_address_id = %s
        ORDER BY t.block_time ASC
        LIMIT 1
    """, (mint_address_id,))
    row = cursor.fetchone()
    if row:
        return row[0], row[1]
    return None, None


def store_discovered_signatures(cursor, conn, tag, sync_id, mint_address_id, sig_batch):
    """
    INSERT IGNORE a batch of discovered signatures into txs_sync.
    sig_batch is a list of RPC result dicts with 'signature', 'blockTime', 'slot'.
    Returns number of rows inserted.
    """
    if not sig_batch:
        return 0

    values = []
    params = []
    for sig in sig_batch:
        values.append("(%s, %s, %s, %s, %s)")
        params.extend([
            sync_id,
            mint_address_id,
            sig['signature'],
            sig.get('blockTime'),
            sig.get('slot'),
        ])

    sql = f"""
        INSERT IGNORE INTO {STAGING_SCHEMA}.{SYNC_TABLE}
            (sync_id, mint_address_id, signature, block_time, block_id)
        VALUES {','.join(values)}
    """
    cursor.execute(sql, params)
    inserted = cursor.rowcount
    conn.commit()
    return inserted


def mark_existing_signatures(cursor, conn, tag, sync_id):
    """
    Mark signatures that already exist in our tx table.
    Returns count of existing signatures.
    """
    cursor.execute(f"""
        UPDATE {STAGING_SCHEMA}.{SYNC_TABLE} s
        JOIN tx t ON t.signature = s.signature
        SET s.status = 'exists'
        WHERE s.sync_id = %s AND s.status = 'discovered'
    """, (sync_id,))
    existing = cursor.rowcount
    conn.commit()
    return existing


def fetch_gap_signatures(cursor, sync_id):
    """
    Fetch all discovered (gap) signatures that need processing.
    Returns list of signature strings ordered by block_time DESC.
    """
    cursor.execute(f"""
        SELECT signature
        FROM {STAGING_SCHEMA}.{SYNC_TABLE}
        WHERE sync_id = %s AND status = 'discovered'
        ORDER BY block_time DESC
    """, (sync_id,))
    return [row[0] for row in cursor.fetchall()]


def mark_queued(cursor, conn, sync_id, signatures):
    """Mark a batch of signatures as queued in txs_sync."""
    if not signatures:
        return
    placeholders = ','.join(['%s'] * len(signatures))
    cursor.execute(f"""
        UPDATE {STAGING_SCHEMA}.{SYNC_TABLE}
        SET status = 'queued'
        WHERE sync_id = %s AND signature IN ({placeholders})
    """, (sync_id, *signatures))
    conn.commit()


def cascade_to_pipeline(ch, tag, signatures, request_id, correlation_id,
                        priority, request_log_id, api_key_id, batch_num, total_batches):
    """Publish a batch of signatures to both decoder and detailer queues."""
    sig_hash = compute_sig_hash(signatures)

    cascade_msg = {
        'request_id': request_id,
        'correlation_id': correlation_id,
        'request_log_id': request_log_id,
        'api_key_id': api_key_id,
        'features': 0,
        'sig_hash': sig_hash,
        'action': 'cascade',
        'source_worker': 'synchronizer',
        'tx_origin': 2,  # sync backfill
        'priority': priority,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'batch': {
            'signatures': signatures,
            'batch_num': batch_num,
            'total_batches': total_batches
        }
    }

    body = json.dumps(cascade_msg).encode('utf-8')
    props = pika.BasicProperties(delivery_mode=2, content_type='application/json', priority=priority)

    ch.basic_publish(exchange='', routing_key=DECODER_REQUEST_QUEUE, body=body, properties=props)
    ch.basic_publish(exchange='', routing_key=DETAILER_REQUEST_QUEUE, body=body, properties=props)


def process_sync_request(tag, cursor, conn, ch, session, msg, dry_run, rpc_delay, cascade_batch_size, max_sigs=1000):
    """
    Main sync processing flow for a single mint.
    Returns result dict.
    """
    request_id = msg.get('request_id', str(uuid.uuid4()))
    correlation_id = msg.get('correlation_id', request_id)
    request_log_id = msg.get('request_log_id')
    api_key_id = msg.get('api_key_id')
    priority = msg.get('priority', 5)
    sync_data = msg.get('sync', {})

    mint_address = sync_data.get('mint_address')
    mint_symbol = sync_data.get('mint_symbol')
    sync_id = str(uuid.uuid4())

    result = {
        'mint_address': mint_address,
        'mint_symbol': mint_symbol,
        'sync_id': sync_id,
        'total_on_chain': 0,
        'already_have': 0,
        'gaps_found': 0,
        'batches_queued': 0,
        'error': None,
    }

    # Step 1: Resolve mint
    log(tag, f"Sync request: mint_address={mint_address}, mint_symbol={mint_symbol}")
    address_id, address_str = resolve_mint(cursor, tag, mint_address, mint_symbol)
    if not address_id:
        result['error'] = f'Mint not found: {mint_address or mint_symbol}'
        return result

    result['mint_address'] = address_str
    log(tag, f"Resolved mint: {address_str} (id={address_id})")

    # Step 2: Find our oldest known signature
    oldest_sig, oldest_block_time = find_oldest_signature(cursor, tag, address_id)
    if oldest_sig:
        log(tag, f"Oldest known tx: {oldest_sig[:16]}... (block_time={oldest_block_time})")
    else:
        log(tag, "No existing transactions found for this mint — fetching full history")

    if dry_run:
        log(tag, "[DRY] Would fetch signatures from Chainstack and process gaps")
        return result

    # Step 3: Fetch all signatures from Chainstack
    log(tag, f"Fetching signatures from Chainstack (until={oldest_sig[:16] + '...' if oldest_sig else 'none'}, max={max_sigs})...")
    sig_buffer = []
    total_fetched = 0
    total_stored = 0
    buffer_size = 500  # INSERT batch size
    capped = False

    for sig in fetch_all_signatures(session, tag, address_str, until_sig=oldest_sig, rpc_delay=rpc_delay):
        sig_buffer.append(sig)
        total_fetched += 1

        if len(sig_buffer) >= buffer_size:
            total_stored += store_discovered_signatures(cursor, conn, tag, sync_id, address_id, sig_buffer)
            sig_buffer = []

        if total_fetched >= max_sigs:
            log(tag, f"Reached max sig cap ({max_sigs}), stopping RPC pagination")
            capped = True
            break

    # Flush remaining
    if sig_buffer:
        total_stored += store_discovered_signatures(cursor, conn, tag, sync_id, address_id, sig_buffer)

    log(tag, f"Discovered {total_stored} signatures on-chain (fetched {total_fetched}{', CAPPED' if capped else ''})")
    result['total_on_chain'] = total_stored

    if total_stored == 0:
        log(tag, "No signatures found on-chain")
        return result

    # Step 4: Mark existing signatures
    existing_count = mark_existing_signatures(cursor, conn, tag, sync_id)
    result['already_have'] = existing_count
    log(tag, f"Already have: {existing_count}, gaps: {total_stored - existing_count}")

    # Step 5: Cascade missing signatures to pipeline
    gap_sigs = fetch_gap_signatures(cursor, sync_id)
    result['gaps_found'] = len(gap_sigs)

    if not gap_sigs:
        log(tag, "No gaps found — mint is fully synced")
        return result

    log(tag, f"Cascading {len(gap_sigs)} gap signatures in batches of {cascade_batch_size}...")
    total_batches = (len(gap_sigs) + cascade_batch_size - 1) // cascade_batch_size
    batches_queued = 0

    for i in range(0, len(gap_sigs), cascade_batch_size):
        batch = gap_sigs[i:i + cascade_batch_size]
        batch_num = (i // cascade_batch_size) + 1

        try:
            cascade_to_pipeline(
                ch, tag, batch, request_id, correlation_id,
                priority, request_log_id, api_key_id,
                batch_num, total_batches
            )
            mark_queued(cursor, conn, sync_id, batch)
            batches_queued += 1
        except Exception as e:
            log(tag, f"Failed to cascade batch {batch_num}: {e}")

    result['batches_queued'] = batches_queued
    log(tag, f"Sync complete: {batches_queued}/{total_batches} batches queued")

    return result


# =============================================================================
# Worker Thread
# =============================================================================

class WorkerThread(threading.Thread):
    def __init__(self, worker_id, dry_run, stop_event,
                 poll_idle_sec, reconnect_sec, rpc_delay_sec, cascade_batch_size):
        super().__init__(daemon=True)
        self.tag = f"W-{worker_id}"
        self.worker_id = worker_id
        self.dry_run = dry_run
        self.stop_event = stop_event
        self.poll_idle_sec = poll_idle_sec
        self.reconnect_sec = reconnect_sec
        self.rpc_delay_sec = rpc_delay_sec
        self.cascade_batch_size = cascade_batch_size

    def run(self):
        log(self.tag, "Starting")
        session = requests.Session()
        db_conn = None
        cursor = None

        while not self.stop_event.is_set():
            try:
                # Connect DB
                if db_conn is None:
                    db_conn = db_connect()
                    cursor = db_conn.cursor()
                    log(self.tag, "DB connected")

                # Connect RabbitMQ
                rmq_conn, ch = rmq_connect()
                ch.basic_qos(prefetch_count=1)
                log(self.tag, "RabbitMQ connected, consuming...")

                # Consume one message at a time
                while not self.stop_event.is_set():
                    method, properties, body = ch.basic_get(queue=REQUEST_QUEUE, auto_ack=False)
                    if method is None:
                        # No message — sleep and check stop_event
                        elapsed = 0.0
                        while elapsed < self.poll_idle_sec and not self.stop_event.is_set():
                            time.sleep(min(0.5, self.poll_idle_sec - elapsed))
                            elapsed += 0.5
                        rmq_conn.process_data_events(time_limit=0)
                        continue

                    self._handle_message(ch, method, properties, body, cursor, db_conn, session)

                # Clean exit
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
                import traceback
                traceback.print_exc()
                time.sleep(self.reconnect_sec)

        # Cleanup
        if db_conn:
            try:
                db_conn.close()
            except Exception:
                pass
        session.close()
        log(self.tag, "Stopped")

    def _handle_message(self, ch, method, properties, body, cursor, db_conn, session):
        try:
            msg = json.loads(body.decode('utf-8'))
            request_id = msg.get('request_id', 'unknown')
            correlation_id = msg.get('correlation_id', request_id)

            log(self.tag, f"Received sync request {request_id[:8]} (corr={correlation_id[:8]})")

            queue_cfg = load_db_config(cursor, 'queue')
            max_sigs = cfg_int(queue_cfg, 'synchronizer_wrk_max_sigs', 1000)
            result = process_sync_request(
                self.tag, cursor, db_conn, ch, session, msg,
                self.dry_run, self.rpc_delay_sec, self.cascade_batch_size,
                max_sigs=max_sigs
            )

            # Publish response
            status = 'failed' if result.get('error') else 'completed'
            response = {
                'request_id': request_id,
                'correlation_id': correlation_id,
                'worker': 'synchronizer',
                'status': status,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'result': result,
            }

            resp_body = json.dumps(response).encode('utf-8')
            ch.basic_publish(
                exchange='', routing_key=RESPONSE_QUEUE,
                body=resp_body,
                properties=pika.BasicProperties(delivery_mode=2, content_type='application/json')
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)

            if result.get('error'):
                log(self.tag, f"Sync failed: {result['error']}")
            else:
                log(self.tag, f"Sync done: on_chain={result['total_on_chain']} "
                      f"have={result['already_have']} gaps={result['gaps_found']} "
                      f"batches={result['batches_queued']}")

        except MySQLError:
            nack_with_retry(ch, method.delivery_tag, properties,
                            log_fn=lambda msg: log(self.tag, f"[DB ERROR] {msg}"),
                            queue_name=REQUEST_QUEUE, body=body)
            raise  # Bubble up for DB reconnect
        except Exception as e:
            log(self.tag, f"ERROR processing sync request -> DLQ: {e}")
            import traceback
            traceback.print_exc()
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


# =============================================================================
# Supervisor
# =============================================================================

def read_config(cursor):
    cfg = load_db_config(cursor, 'queue')
    return {
        'threads':          cfg_int(cfg, 'synchronizer_wrk_cnt_threads', 1),
        'supervisor_poll':  cfg_float(cfg, 'synchronizer_wrk_supervisor_poll_sec', 5.0),
        'poll_idle':        cfg_float(cfg, 'synchronizer_wrk_poll_idle_sec', 1.0),
        'reconnect':        cfg_float(cfg, 'synchronizer_wrk_reconnect_sec', 5.0),
        'shutdown_timeout': cfg_float(cfg, 'synchronizer_wrk_shutdown_timeout_sec', 30.0),
        'rpc_delay':        cfg_float(cfg, 'synchronizer_wrk_rpc_delay_sec', 0.2),
        'cascade_batch':    cfg_int(cfg, 'synchronizer_wrk_cascade_batch_size', 20),
    }


def run_supervisor(dry_run=False):
    print(f"""
+-----------------------------------------------------------+
|  Guide Synchronizer - Supervisor (config-driven)          |
|  queue: {REQUEST_QUEUE:<48} |
+-----------------------------------------------------------+
""", flush=True)

    workers = {}
    next_id = 1
    svr_conn = None
    svr_cursor = None

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
            svr_cursor = svr_conn.cursor()
            log('SVR', 'DB connected')
            return True
        except Exception as e:
            log('SVR', f'DB connect failed: {e}')
            return False

    cfg = {}

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
                log('SVR', f"Config: threads={cfg['threads']} | active={active}")

            # Scale up
            while len(workers) < cfg['threads']:
                stop_evt = threading.Event()
                w = WorkerThread(
                    next_id, dry_run, stop_evt,
                    cfg['poll_idle'], cfg['reconnect'],
                    cfg['rpc_delay'], cfg['cascade_batch'])
                w.start()
                workers[next_id] = (w, stop_evt)
                next_id += 1

            # Scale down (stop newest first)
            while len(workers) > cfg['threads']:
                wid = max(workers.keys())
                wthread, stop_evt = workers.pop(wid)
                log('SVR', f'Stopping W-{wid}...')
                stop_evt.set()

            time.sleep(cfg['supervisor_poll'])

    except KeyboardInterrupt:
        log('SVR', 'Shutting down...')

    shutdown_timeout = cfg.get('shutdown_timeout', 30.0)

    for wid, (w, stop_evt) in workers.items():
        stop_evt.set()

    for wid, (w, _) in workers.items():
        w.join(timeout=shutdown_timeout)
        if w.is_alive():
            log('SVR', f'W-{wid} did not stop in time')

    if svr_conn:
        try:
            svr_conn.close()
        except Exception:
            pass

    log('SVR', 'Shutdown complete')


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Guide Synchronizer - Gap-filling worker for mint transaction history'
    )
    parser.add_argument('--dry-run', action='store_true', help='Preview only, no changes')
    args = parser.parse_args()
    return run_supervisor(dry_run=args.dry_run)


if __name__ == '__main__':
    sys.exit(main() or 0)
