#!/usr/bin/env python3
"""
Guide Detailer - Queue Consumer (multi-threaded, config-driven)

Supervisor thread polls config for desired thread/prefetch counts.
Worker threads each own their own DB + RabbitMQ + aiohttp connections.

Config keys (config_type='queue'):
    detailer_wrk_cnt_threads          - desired worker thread count (0 = idle)
    detailer_wrk_cnt_prefetch         - RabbitMQ prefetch per worker channel
    detailer_wrk_supervisor_poll_sec  - supervisor config poll interval
    detailer_wrk_poll_idle_sec        - worker sleep when no message available
    detailer_wrk_reconnect_sec        - delay before reconnecting after errors
    detailer_wrk_shutdown_timeout_sec - max wait for worker thread on shutdown
    detailer_wrk_api_timeout_sec      - Solscan API request timeout
    detailer_wrk_api_max_retries      - max retries on 502/503/504

Usage:
    python guide-detailer.py
    python guide-detailer.py --dry-run
"""

import argparse
import asyncio
import itertools
import json
import os
import sys
import time
import threading
import aiohttp
import pika
import mysql.connector
from mysql.connector import Error as MySQLError
from datetime import datetime, timezone
from decode_v2 import AddressCache
from detail_v2 import process_detail_v2
from rpc_detail_transform import rpc_batch_to_solscan_detail

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
_queues             = get_queue_names('detailer')
_retry              = get_retry_config()
_staging            = get_staging_config()

SOLSCAN_API_BASE    = _solscan['api_base']
SOLSCAN_API_TOKEN   = _solscan['token']

# Load RPC_URLs directly from guide-config.json (avoids deploying config.py)
_cfg_path = os.path.join(os.path.dirname(__file__), 'guide-config.json')
with open(_cfg_path) as _f:
    _cfg_json = json.load(_f)
RPC_URLS = _cfg_json.get('RPC_URLs', [_cfg_json['RPC_URL']])
RPC_API_KEYS = _cfg_json.get('RPC_API_KEYS', [None] * len(RPC_URLS))
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
            "WHERE request_id=%s AND target_worker='detailer' AND api_key_id=%s",
            (request_id, api_key_id))
    else:
        cursor.execute(
            "SELECT id FROM tx_request_log "
            "WHERE request_id=%s AND target_worker='detailer' AND api_key_id IS NULL",
            (request_id,))
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor.execute(
        "INSERT INTO tx_request_log "
        "(request_id, correlation_id, api_key_id, source, target_worker, "
        " action, priority, features, status, payload_summary) "
        "VALUES (%s,%s,%s,'queue','detailer','detail',%s,%s,'processing',%s)",
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
# Solscan API (async)
# =============================================================================

MAX_SAFE_INT = 9223372036854775807

def sanitize_large_ints(obj):
    if isinstance(obj, dict):
        return {k: sanitize_large_ints(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_large_ints(item) for item in obj]
    elif isinstance(obj, int) and (obj > MAX_SAFE_INT or obj < -MAX_SAFE_INT):
        return str(obj)
    return obj


async def fetch_detail(session, signatures, api_timeout, max_retries):
    """Solscan fallback for transactions not available via RPC (pruned/archival)."""
    params = '&'.join(f'tx[]={sig}' for sig in signatures)
    url = f"{SOLSCAN_API_BASE}/transaction/detail/multi?{params}"
    timeout = aiohttp.ClientTimeout(total=api_timeout)
    headers = {'token': SOLSCAN_API_TOKEN}

    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=headers, timeout=timeout) as response:
                if response.status in (502, 503, 504):
                    if attempt < max_retries - 1:
                        wait = (attempt + 1) * 5
                        await asyncio.sleep(wait)
                        continue
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < max_retries - 1:
                wait = (attempt + 1) * 5
                await asyncio.sleep(wait)
            else:
                raise
    raise Exception(f"Failed after {max_retries} retries")


# Thread-safe round-robin counter for RPC endpoint selection
_rpc_counter = itertools.count()


async def fetch_detail_rpc(session, signatures, api_timeout, max_retries):
    """Fetch transaction details via Chainstack RPC getTransaction (round-robin).

    Replaces Solscan /transaction/detail/multi with parallel RPC calls across
    multiple Chainstack endpoints. Returns data in Solscan-compatible format
    via rpc_detail_transform.
    """
    timeout = aiohttp.ClientTimeout(total=api_timeout)

    async def _fetch_one(sig):
        # Round-robin across RPC endpoints
        idx = next(_rpc_counter) % len(RPC_URLS)
        rpc_url = RPC_URLS[idx]
        api_key = RPC_API_KEYS[idx] if idx < len(RPC_API_KEYS) else None
        req_headers = {'Authorization': f'Bearer {api_key}'} if api_key else {}
        node_id = rpc_url.split('/')[-1][:8]
        log('RPC', f"Node_{idx:02d} ({node_id}..) getTransaction {sig[:12]}.. auth={'Bearer' if req_headers else 'none'}")
        payload = {
            "id": 1, "jsonrpc": "2.0", "method": "getTransaction",
            "params": [sig, {"encoding": "jsonParsed",
                             "maxSupportedTransactionVersion": 0}]
        }
        for attempt in range(max_retries):
            try:
                async with session.post(rpc_url, json=payload,
                                        headers=req_headers,
                                        timeout=timeout) as response:
                    if response.status in (429, 502, 503, 504):
                        if attempt < max_retries - 1:
                            await asyncio.sleep((attempt + 1) * 2)
                            continue
                    response.raise_for_status()
                    return sig, await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt < max_retries - 1:
                    await asyncio.sleep((attempt + 1) * 2)
                else:
                    raise
        raise Exception(f"RPC failed for {sig[:16]} after {max_retries} retries")

    # Fire all getTransaction calls concurrently
    tasks = [_fetch_one(sig) for sig in signatures]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Collect successful results, log failures
    items = []
    for r in results:
        if isinstance(r, Exception):
            # Individual tx failure is non-fatal; log and skip
            continue
        items.append(r)  # (sig, rpc_response)

    return rpc_batch_to_solscan_detail(items)


# =============================================================================
# Core processing
# =============================================================================

def get_tx_state_detailed(cursor):
    cursor.execute(
        "SELECT CAST(config_value AS UNSIGNED) FROM config "
        "WHERE config_type='tx_state' AND config_key='detailed'")
    row = cursor.fetchone()
    return row[0] if row else 16


async def process_signatures(tag, cursor, conn, session, signatures,
                             priority, correlation_id, sig_hash, request_log_id,
                             dry_run, api_timeout, max_retries, addr_cache=None):
    result = {'processed': 0, 'skipped': 0, 'staging_id': None, 'tx_count': 0, 'error': None}
    if not signatures:
        return result
    if dry_run:
        log(tag, f"[DRY] Would detail {len(signatures)} signatures")
        result['processed'] = len(signatures)
        return result

    # Fetch from Chainstack RPC (round-robin across endpoints)
    t0 = time.time()
    try:
        detail_response = await fetch_detail_rpc(session, signatures, api_timeout, max_retries)
    except Exception as e:
        result['error'] = f'RPC API error: {e}'
        return result
    fetch_time = time.time() - t0

    tx_data = detail_response.get('data', [])

    # Fallback: if RPC missed some sigs (pruned/archival), fetch remainder from Solscan
    if len(tx_data) < len(signatures):
        rpc_sigs = {tx['tx_hash'] for tx in tx_data}
        missing = [s for s in signatures if s not in rpc_sigs]
        if missing:
            log(tag, f"RPC missed {len(missing)}/{len(signatures)} txs, falling back to Solscan")
            try:
                fallback = await fetch_detail(session, missing, api_timeout, max_retries)
                if fallback.get('success') and fallback.get('data'):
                    tx_data.extend(fallback['data'])
                    log(tag, f"Solscan fallback returned {len(fallback['data'])} txs")
            except Exception as e:
                log(tag, f"Solscan fallback error (non-fatal): {e}")

    if not tx_data:
        result['error'] = 'RPC+Solscan returned no data'
        return result
    result['tx_count'] = len(tx_data)

    detail_response = {'success': True, 'data': tx_data}

    # Sanitize large ints (values > 2^63)
    sanitized = sanitize_large_ints(detail_response)

    # v2: Python parses JSON, resolves IDs, populates temp tables, calls lightweight SP
    t1 = time.time()
    sp_result = process_detail_v2(
        cursor, conn, sanitized, request_log_id,
        cache=addr_cache, tag=tag)
    sp_time = time.time() - t1

    if sp_result.get('error'):
        result['error'] = sp_result['error']
        return result

    sp_tx   = sp_result.get('tx_count', 0)
    sp_sol  = sp_result.get('sol_balance_count', 0)
    sp_tok  = sp_result.get('token_balance_count', 0)
    sp_skip = sp_result.get('skipped_count', 0)

    result['processed'] = len(tx_data)
    result['tx_count'] = sp_tx
    log(tag, f"Detailed {len(tx_data)} txs: tx={sp_tx} sol={sp_sol} tok={sp_tok} "
            f"skip={sp_skip} (rpc={fetch_time:.2f}s, sp={sp_time:.3f}s)")
    return result

# =============================================================================
# Worker thread
# =============================================================================

class WorkerThread(threading.Thread):
    def __init__(self, worker_id, prefetch, dry_run, stop_event,
                 poll_idle_sec, reconnect_sec, api_timeout_sec, api_max_retries):
        super().__init__(daemon=True)
        self.tag = f"W-{worker_id}"
        self.worker_id = worker_id
        self.prefetch = prefetch
        self.dry_run = dry_run
        self.stop_event = stop_event
        self.poll_idle_sec = poll_idle_sec
        self.reconnect_sec = reconnect_sec
        self.api_timeout_sec = api_timeout_sec
        self.api_max_retries = api_max_retries
        self.addr_cache = AddressCache()

    def run(self):
        log(self.tag, f"Starting (prefetch={self.prefetch})")

        # Each worker thread gets its own event loop + aiohttp session
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def _create_session():
            connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
            return aiohttp.ClientSession(
                headers={'Content-Type': 'application/json'},
                connector=connector)

        api_session = loop.run_until_complete(_create_session())

        db_conn = None
        cursor = None

        try:
            while not self.stop_event.is_set():
                try:
                    # Connect DB
                    if db_conn is None:
                        db_conn = db_connect()
                        cursor = db_conn.cursor()
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

                        self._handle_message(ch, method, properties, body, cursor, db_conn, api_session, loop)

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
        finally:
            # Cleanup
            loop.run_until_complete(api_session.close())
            loop.close()
            if db_conn:
                try:
                    db_conn.close()
                except Exception:
                    pass
            log(self.tag, "Stopped")

    def _handle_message(self, ch, method, properties, body, cursor, db_conn, api_session, loop):
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
            signatures     = batch_data.get('signatures', [])
            batch_num      = batch_data.get('batch_num', 0)

            log(self.tag, f"Request {request_id[:8]} "
                f"(corr={correlation_id[:8]}, sig_hash={sig_hash[:8] if sig_hash else 'N/A'}, "
                f"sigs={len(signatures)}, batch={batch_num})")

            worker_log_id = log_worker_request(
                cursor, db_conn, request_id, correlation_id,
                batch_num, len(signatures), priority, api_key_id, features)

            if not signatures:
                resp = {'processed': 0, 'message': 'No signatures provided'}
                update_worker_request(cursor, db_conn, worker_log_id, 'completed', resp)
                self._publish_response(ch, request_id, correlation_id, 'completed', resp, batch_num)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            result = loop.run_until_complete(
                process_signatures(
                    self.tag, cursor, db_conn, api_session, signatures,
                    priority, correlation_id, sig_hash, request_log_id,
                    self.dry_run, self.api_timeout_sec, self.api_max_retries,
                    addr_cache=self.addr_cache))

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
                resp = {'processed': 0, 'skipped': result['skipped'], 'message': 'No data returned'}
            else:
                status = 'completed'
                resp = {
                    'processed':  result['processed'],
                    'tx_count':   result['tx_count'],
                }

            update_worker_request(cursor, db_conn, worker_log_id, status, resp)
            self._publish_response(ch, request_id, correlation_id, status, resp, batch_num)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except MySQLError:
            nack_with_retry(ch, method.delivery_tag, properties,
                            log_fn=lambda msg: log(self.tag, f"[DB ERROR] {msg}"),
                            queue_name=REQUEST_QUEUE, body=body)
            raise
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
            'worker':         'detailer',
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
        'threads':          cfg_int(cfg, 'detailer_wrk_cnt_threads', 0),
        'prefetch':         cfg_int(cfg, 'detailer_wrk_cnt_prefetch', 5),
        'supervisor_poll':  cfg_float(cfg, 'detailer_wrk_supervisor_poll_sec', 5.0),
        'poll_idle':        cfg_float(cfg, 'detailer_wrk_poll_idle_sec', 0.25),
        'reconnect':        cfg_float(cfg, 'detailer_wrk_reconnect_sec', 5.0),
        'shutdown_timeout': cfg_float(cfg, 'detailer_wrk_shutdown_timeout_sec', 10.0),
        'api_timeout':      cfg_float(cfg, 'detailer_wrk_api_timeout_sec', 60.0),
        'api_max_retries':  cfg_int(cfg, 'detailer_wrk_api_max_retries', 3),
    }


def run_supervisor(dry_run=False):
    print(f"""
+-----------------------------------------------------------+
|  Guide Detailer - Supervisor                              |
|  vhost: {RABBITMQ_VHOST:<10}  queue: {REQUEST_QUEUE:<26} |
+-----------------------------------------------------------+
""", flush=True)

    workers = {}
    next_id = 1
    svr_conn = None
    svr_cursor = None
    cfg = {}

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
                    cfg['poll_idle'], cfg['reconnect'], cfg['api_timeout'], cfg['api_max_retries'])
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

    shutdown_timeout = cfg.get('shutdown_timeout', 10.0)

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
    parser = argparse.ArgumentParser(description='Guide Detailer - Queue Consumer')
    parser.add_argument('--dry-run', action='store_true', help='Preview only, no DB changes')
    args = parser.parse_args()
    return run_supervisor(dry_run=args.dry_run)


if __name__ == '__main__':
    sys.exit(main() or 0)
