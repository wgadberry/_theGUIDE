#!/usr/bin/env python3
"""
Guide Decoder - Queue Consumer (multi-threaded, config-driven)

Supervisor thread polls config for desired thread/prefetch counts.
Worker threads each own their own DB + RabbitMQ connections.

Config keys (config_type='queue'):
    decoder_wrk_cnt_threads        - desired worker thread count (0 = idle)
    decoder_wrk_cnt_prefetch       - RabbitMQ prefetch per worker channel
    decoder_wrk_supervisor_poll_sec - supervisor config poll interval
    decoder_wrk_poll_idle_sec      - worker sleep when no message available
    decoder_wrk_reconnect_sec      - delay before reconnecting after errors
    decoder_wrk_shutdown_timeout_sec - max wait for worker thread on shutdown
    decoder_wrk_api_timeout_sec    - Solscan API request timeout

Usage:
    python guide-decoder.py
    python guide-decoder.py --dry-run
"""

import argparse
import json
import os
import sys
import time
import threading
import requests
import pika
import mysql.connector
from mysql.connector import Error as MySQLError
from datetime import datetime, timezone
from decode_v2 import process_decoded_v2, AddressCache

# =============================================================================
# Static config (from common.config → guide-config.json)
# =============================================================================

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from t16o_exchange.guide.common.config import (
    get_db_config, get_rabbitmq_config, get_solscan_config,
    get_staging_config, get_queue_names, get_retry_config,
    nack_with_retry,
    load_db_config, cfg_int, cfg_float,
)

_solscan            = get_solscan_config()
_rmq                = get_rabbitmq_config()
_queues             = get_queue_names('decoder')
_retry              = get_retry_config()
_staging            = get_staging_config()

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
STAGING_SCHEMA      = _staging['schema']
STAGING_TABLE       = _staging['table']

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
    return conn, ch

# =============================================================================
# Request logging (billing)
# =============================================================================

def log_worker_request(cursor, conn, request_id, correlation_id,
                       batch_num, batch_size, priority, api_key_id, features):
    if api_key_id is not None:
        cursor.execute(
            "SELECT id FROM tx_request_log "
            "WHERE request_id=%s AND target_worker='decoder' AND api_key_id=%s",
            (request_id, api_key_id))
    else:
        cursor.execute(
            "SELECT id FROM tx_request_log "
            "WHERE request_id=%s AND target_worker='decoder' AND api_key_id IS NULL",
            (request_id,))
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute(
        "INSERT INTO tx_request_log "
        "(request_id, correlation_id, api_key_id, source, target_worker, "
        " action, priority, features, status, payload_summary) "
        "VALUES (%s,%s,%s,'queue','decoder','decode',%s,%s,'processing',%s)",
        (request_id, correlation_id, api_key_id, priority, features,
         json.dumps({'batch_num': batch_num, 'batch_size': batch_size, 'source': 'queue'})))
    conn.commit()
    return cursor.lastrowid


def update_worker_request(cursor, conn, log_id, status, result=None):
    cursor.execute(
        "UPDATE tx_request_log SET status=%s, result_summary=%s, completed_at=NOW() WHERE id=%s",
        (status, json.dumps(result) if result else None, log_id))
    conn.commit()

# =============================================================================
# Solscan API
# =============================================================================

def solscan_session():
    s = requests.Session()
    s.headers['token'] = SOLSCAN_API_TOKEN
    return s


def fetch_decoded_batch(session, signatures, api_timeout):
    params = '&'.join(f'tx[]={sig}' for sig in signatures)
    r = session.get(f"{SOLSCAN_API_BASE}/transaction/actions/multi?{params}", timeout=api_timeout)
    r.raise_for_status()
    return r.json()

# =============================================================================
# Core processing
# =============================================================================

def filter_existing_signatures(cursor, signatures):
    """Filter out signatures that have already been decoded (bit 4 set).
    Skeleton records created by detailer (bit 4 not set) are treated as new."""
    if not signatures:
        return [], []
    ph = ','.join(['%s'] * len(signatures))
    cursor.execute(
        f"SELECT signature FROM tx WHERE signature IN ({ph}) AND tx_state & 4 != 0",
        signatures)
    already_decoded = {row[0] for row in cursor.fetchall()}
    new = [s for s in signatures if s not in already_decoded]
    old = [s for s in signatures if s in already_decoded]
    return new, old


def get_tx_state_decoded(cursor):
    cursor.execute(
        "SELECT CAST(config_value AS UNSIGNED) FROM config "
        "WHERE config_type='tx_state' AND config_key='decoded'")
    row = cursor.fetchone()
    return row[0] if row else 8


def process_signatures(tag, cursor, conn, session, signatures,
                       priority, correlation_id, sig_hash, request_log_id,
                       tx_origin, dry_run, api_timeout, features=31, addr_cache=None):
    result = {'processed': 0, 'skipped': 0, 'staging_id': None, 'tx_count': 0, 'error': None}
    if not signatures:
        return result

    new_sigs, existing_sigs = filter_existing_signatures(cursor, signatures)
    result['skipped'] = len(existing_sigs)
    if result['skipped']:
        log(tag, f"Skipped {result['skipped']}/{len(signatures)} already in tx table")
    if not new_sigs:
        log(tag, "No new signatures to decode")
        return result
    if dry_run:
        log(tag, f"[DRY] Would decode {len(new_sigs)} signatures")
        result['processed'] = len(new_sigs)
        return result

    # Fetch from Solscan
    t0 = time.time()
    try:
        decoded = fetch_decoded_batch(session, new_sigs, api_timeout)
    except requests.RequestException as e:
        result['error'] = f'Solscan API error: {e}'
        return result
    fetch_time = time.time() - t0

    if not decoded.get('success') or not decoded.get('data'):
        result['error'] = 'Solscan API returned no data'
        return result

    actual_sigs = [tx.get('tx_hash') or tx.get('signature')
                   for tx in decoded['data']
                   if tx.get('tx_hash') or tx.get('signature')]
    result['tx_count'] = len(actual_sigs)
    if len(actual_sigs) < len(new_sigs):
        log(tag, f"Solscan returned {len(actual_sigs)}/{len(new_sigs)} signatures")

    # v2: Python parses JSON, resolves IDs, populates temp tables, calls lightweight SP
    t1 = time.time()
    sp_result = process_decoded_v2(
        cursor, conn, decoded, request_log_id, tx_origin,
        features=features, cache=addr_cache, tag=tag)
    sp_time = time.time() - t1

    if sp_result.get('error'):
        result['error'] = sp_result['error']
        return result

    sp_tx   = sp_result.get('tx_count', 0)
    sp_xfer = sp_result.get('transfer_count', 0)
    sp_swap = sp_result.get('swap_count', 0)
    sp_act  = sp_result.get('activity_count', 0)
    sp_skip = sp_result.get('skipped_count', 0)

    result['processed'] = len(actual_sigs)
    result['tx_count'] = sp_tx
    log(tag, f"Decoded {len(actual_sigs)} txs: tx={sp_tx} xfer={sp_xfer} swap={sp_swap} "
            f"act={sp_act} skip={sp_skip} (fetch={fetch_time:.2f}s, sp={sp_time:.3f}s)")
    return result

# =============================================================================
# Worker thread
# =============================================================================

class WorkerThread(threading.Thread):
    def __init__(self, worker_id, prefetch, dry_run, stop_event,
                 poll_idle_sec, reconnect_sec, api_timeout_sec):
        super().__init__(daemon=True)
        self.tag = f"W-{worker_id}"
        self.worker_id = worker_id
        self.prefetch = prefetch
        self.dry_run = dry_run
        self.stop_event = stop_event
        self.poll_idle_sec = poll_idle_sec
        self.reconnect_sec = reconnect_sec
        self.api_timeout_sec = api_timeout_sec
        self.addr_cache = AddressCache()

    def run(self):
        log(self.tag, f"Starting (prefetch={self.prefetch})")
        session = solscan_session()
        db_conn = None
        cursor = None

        while not self.stop_event.is_set():
            try:
                # Connect DB — READ COMMITTED avoids gap lock deadlocks on tx_address INSERT IGNORE
                if db_conn is None:
                    db_conn = db_connect()
                    cursor = db_conn.cursor()
                    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
                    log(self.tag, "DB connected (READ COMMITTED)")

                # Connect RabbitMQ
                rmq_conn, ch = rmq_connect()
                ch.basic_qos(prefetch_count=self.prefetch)
                log(self.tag, "RabbitMQ connected, consuming...")

                # Consume one message at a time so we can check stop_event between messages
                while not self.stop_event.is_set():
                    method, properties, body = ch.basic_get(queue=REQUEST_QUEUE, auto_ack=False)
                    if method is None:
                        time.sleep(self.poll_idle_sec)
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
                self.addr_cache.clear()
                time.sleep(self.reconnect_sec)
            except pika.exceptions.AMQPConnectionError as e:
                log(self.tag, f"RabbitMQ lost: {e}, reconnecting in {self.reconnect_sec}s...")
                time.sleep(self.reconnect_sec)
            except Exception as e:
                log(self.tag, f"Unexpected error: {e}, retrying in {self.reconnect_sec}s...")
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
        worker_log_id = None
        try:
            msg = json.loads(body.decode('utf-8'))
            request_id     = msg.get('request_id', 'unknown')
            correlation_id = msg.get('correlation_id', request_id)
            request_log_id = msg.get('request_log_id')
            api_key_id     = msg.get('api_key_id')
            sig_hash       = msg.get('sig_hash')
            batch_data     = msg.get('batch', {})
            priority       = msg.get('priority', 5)
            features       = msg.get('features', 31)
            tx_origin      = msg.get('tx_origin', 0)
            signatures     = batch_data.get('signatures', [])
            batch_num      = batch_data.get('batch_num', 0)

            log(self.tag, f"Request {request_id[:8]} "
                f"(corr={correlation_id[:8]}, sig_hash={sig_hash[:8] if sig_hash else 'N/A'}, "
                f"sigs={len(signatures)}, batch={batch_num})")

            # Billing log
            worker_log_id = log_worker_request(
                cursor, db_conn, request_id, correlation_id,
                batch_num, len(signatures), priority, api_key_id, features)

            if not signatures:
                resp = {'processed': 0, 'message': 'No signatures provided'}
                update_worker_request(cursor, db_conn, worker_log_id, 'completed', resp)
                self._publish_response(ch, request_id, correlation_id, 'completed', resp, batch_num)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            result = process_signatures(
                self.tag, cursor, db_conn, session, signatures,
                priority, correlation_id, sig_hash, request_log_id,
                tx_origin, self.dry_run, self.api_timeout_sec,
                features=features, addr_cache=self.addr_cache)

            if result.get('error'):
                error_msg = result['error']
                # Transient API errors: requeue for retry instead of acking
                if any(t in error_msg for t in ['timed out', 'timeout', '429', '502', '503', '504', 'ConnectionError']):
                    log(self.tag, f"Transient API error, requeuing: {error_msg}")
                    nack_with_retry(ch, method.delivery_tag, properties,
                                    log_fn=lambda msg: log(self.tag, f"[API RETRY] {msg}"),
                                    queue_name=REQUEST_QUEUE, body=body)
                    return
                status = 'failed'
                resp = {'processed': 0, 'error': error_msg}
            elif result['processed'] == 0:
                status = 'completed'
                resp = {'processed': 0, 'skipped': result['skipped'], 'already_exist': True}
            else:
                status = 'completed'
                resp = {
                    'processed':  result['processed'],
                    'tx_count':   result['tx_count'],
                    'skipped':    result['skipped'],
                }

            update_worker_request(cursor, db_conn, worker_log_id, status, resp)
            self._publish_response(ch, request_id, correlation_id, status, resp, batch_num)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except MySQLError:
            nack_with_retry(ch, method.delivery_tag, properties,
                            max_retries=10,
                            log_fn=lambda msg: log(self.tag, f"[DB ERROR] {msg}"),
                            queue_name=REQUEST_QUEUE, body=body)
            raise  # bubble up so outer loop reconnects DB
        except Exception as e:
            log(self.tag, f"ERROR processing message -> DLQ: {e}")
            if worker_log_id:
                try:
                    update_worker_request(cursor, db_conn, worker_log_id, 'failed', {'error': str(e)})
                except Exception:
                    pass
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def _publish_response(self, ch, request_id, correlation_id, status, result, batch_num):
        result['batch_num'] = batch_num
        body = json.dumps({
            'request_id':     request_id,
            'correlation_id': correlation_id,
            'worker':         'decoder',
            'status':         status,
            'timestamp':      datetime.now(timezone.utc).isoformat(),
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
        'threads':          cfg_int(cfg, 'decoder_wrk_cnt_threads', 0),
        'prefetch':         cfg_int(cfg, 'decoder_wrk_cnt_prefetch', 5),
        'supervisor_poll':  cfg_float(cfg, 'decoder_wrk_supervisor_poll_sec', 5.0),
        'poll_idle':        cfg_float(cfg, 'decoder_wrk_poll_idle_sec', 0.25),
        'reconnect':        cfg_float(cfg, 'decoder_wrk_reconnect_sec', 5.0),
        'shutdown_timeout': cfg_float(cfg, 'decoder_wrk_shutdown_timeout_sec', 10.0),
        'api_timeout':      cfg_float(cfg, 'decoder_wrk_api_timeout_sec', 30.0),
    }


def run_supervisor(dry_run=False):
    print(f"""
+-----------------------------------------------------------+
|  Guide Decoder - Supervisor                               |
|  vhost: {RABBITMQ_VHOST:<10}  queue: {REQUEST_QUEUE:<26} |
+-----------------------------------------------------------+
""", flush=True)

    workers = {}       # worker_id -> (WorkerThread, stop_event)
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
                    cfg['poll_idle'], cfg['reconnect'], cfg['api_timeout'])
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

    # Read shutdown timeout (use last known config or default)
    shutdown_timeout = cfg['shutdown_timeout'] if 'cfg' in dir() else 10.0

    # Signal all workers to stop
    for wid, (w, stop_evt) in workers.items():
        stop_evt.set()

    # Wait for graceful exit
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
    parser = argparse.ArgumentParser(description='Guide Decoder - Queue Consumer')
    parser.add_argument('--dry-run', action='store_true', help='Preview only, no DB changes')
    args = parser.parse_args()
    return run_supervisor(dry_run=args.dry_run)


if __name__ == '__main__':
    sys.exit(main() or 0)
