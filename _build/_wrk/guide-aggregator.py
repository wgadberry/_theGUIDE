#!/usr/bin/env python3
"""
Guide Aggregator - Queue Consumer (multi-threaded, config-driven)

Supervisor thread polls config for desired thread/prefetch counts.
Worker threads each own their own DB + RabbitMQ connections.

Operations:
- guide:  Process tx_activity into tx_guide via sp_tx_guide_loader
- tokens: Sync tx_token_participant from tx_guide (buys, sells, transfers)

Config keys (config_type='queue'):
    aggregator_wrk_cnt_threads           - desired worker thread count (0 = idle)
    aggregator_wrk_cnt_prefetch          - RabbitMQ prefetch per worker channel
    aggregator_wrk_supervisor_poll_sec   - supervisor config poll interval
    aggregator_wrk_poll_idle_sec         - worker sleep when no message available
    aggregator_wrk_reconnect_sec         - delay before reconnecting after errors
    aggregator_wrk_shutdown_timeout_sec  - max wait for worker thread on shutdown
    aggregator_wrk_batch_size            - token participant batch size (id range per chunk)
    aggregator_wrk_deadlock_max_retries  - max deadlock retry attempts
    aggregator_wrk_deadlock_base_delay   - initial deadlock backoff delay
    aggregator_wrk_db_poll_sec           - supervisor DB poll interval (0 = disabled)

Manual modes (run directly, not as service):
    python guide-aggregator.py --sync guide             # Only sp_tx_guide_loader
    python guide-aggregator.py --sync tokens            # Only tx_token_participant
    python guide-aggregator.py --sync all               # Both operations
    python guide-aggregator.py --status                 # Show sync status
"""

import argparse
import json
import os
import sys
import time
import random
import threading
import uuid
import pika
import mysql.connector
from mysql.connector import Error as MySQLError
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any


# =============================================================================
# Static config (from common.config → guide-config.json)
# =============================================================================

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from t16o_exchange.guide.common.config import (
    get_db_config, get_rabbitmq_config, get_queue_names, get_retry_config,
    nack_with_retry,
    load_db_config, cfg_int, cfg_float,
)

_rmq                = get_rabbitmq_config()
_queues             = get_queue_names('aggregator')
_enricher_queues    = get_queue_names('enricher')
_retry              = get_retry_config()

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
ENRICHER_REQUEST_QUEUE = _enricher_queues['request']
ENRICHER_DLQ_QUEUE     = _enricher_queues['dlq']
DB_CONFIG           = get_db_config()

# Config table keys
CONFIG_TYPE_SYNC = 'sync'
TOKEN_PARTICIPANT_KEY = 'token_participant_last_guide_id'


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
    ch.queue_declare(queue=ENRICHER_REQUEST_QUEUE, durable=True,
                     arguments={'x-max-priority': 10,
                                'x-dead-letter-exchange': '',
                                'x-dead-letter-routing-key': ENRICHER_DLQ_QUEUE})
    return conn, ch


# =============================================================================
# Request logging
# =============================================================================

def log_worker_request(cursor, conn, request_id, correlation_id,
                       priority, api_key_id):
    if api_key_id is not None:
        cursor.execute(
            "SELECT id FROM tx_request_log "
            "WHERE request_id=%s AND target_worker='aggregator' AND api_key_id=%s",
            (request_id, api_key_id))
    else:
        cursor.execute(
            "SELECT id FROM tx_request_log "
            "WHERE request_id=%s AND target_worker='aggregator' AND api_key_id IS NULL",
            (request_id,))
    row = cursor.fetchone()
    if row:
        return row['id']

    cursor.execute(
        "INSERT INTO tx_request_log "
        "(request_id, correlation_id, api_key_id, source, target_worker, "
        " action, priority, status, payload_summary) "
        "VALUES (%s,%s,%s,'queue','aggregator','sync',%s,'processing',%s)",
        (request_id, correlation_id, api_key_id, priority,
         json.dumps({'source': 'queue'})))
    conn.commit()
    return cursor.lastrowid


def update_worker_request(cursor, conn, log_id, status, result=None):
    cursor.execute(
        "UPDATE tx_request_log SET status=%s, result_summary=%s, completed_at=NOW() WHERE id=%s",
        (status, json.dumps(result) if result else None, log_id))
    conn.commit()


def log_daemon_request(cursor, conn, action, total_processed):
    request_id = f"daemon-aggregator-{uuid.uuid4().hex[:12]}"
    cursor.execute(
        "INSERT INTO tx_request_log "
        "(request_id, correlation_id, api_key_id, source, target_worker, "
        " action, priority, status, payload_summary) "
        "VALUES (%s,%s,NULL,'daemon','aggregator',%s,5,'processing',%s)",
        (request_id, request_id, action,
         json.dumps({'total_processed': total_processed})))
    conn.commit()
    log_id = cursor.lastrowid
    log('DAEMON', f"request_log id={log_id}")
    return log_id


# =============================================================================
# Guide Loading (sp_tx_guide_loader)
# =============================================================================

def run_guide_loader_batch(tag, cursor, conn, batch_size, max_retries, base_delay):
    """Execute single batch of sp_tx_guide_loader with deadlock retry.
    Returns (guide_count, last_tx_id)."""
    for attempt in range(max_retries):
        try:
            cursor.execute("SET @rows = 0, @last = 0")
            cursor.execute("CALL sp_tx_guide_loader(%s, @rows, @last)", (batch_size,))
            try:
                while cursor.nextset():
                    pass
            except:
                pass
            cursor.execute("SELECT @rows AS r, @last AS l")
            result = cursor.fetchone()
            conn.commit()
            return (result['r'] or 0, result['l'] or 0)
        except mysql.connector.Error as err:
            if err.errno in (1213, 1205) and attempt < max_retries - 1:
                backoff = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
                log(tag, f"Deadlock (attempt {attempt + 1}), retrying in {backoff:.1f}s...")
                conn.rollback()
                time.sleep(backoff)
            else:
                raise
    return (0, 0)


def sync_guide_edges(tag, cursor, conn, max_retries, base_delay, stop_event=None):
    """Process all pending activities via sp_tx_guide_loader. Returns stats."""
    stats = {'batches': 0, 'edges': 0}
    batch_num = 0

    while True:
        if stop_event is not None and stop_event.is_set():
            log(tag, "[guide] Stop event received, breaking out of sync loop")
            break

        batch_cfg = load_db_config(cursor, 'batch')
        batch_size = cfg_int(batch_cfg, 'guide_batch_size', 1000)
        edges, last_tx_id = run_guide_loader_batch(tag, cursor, conn, batch_size, max_retries, base_delay)

        if last_tx_id == 0:
            break

        batch_num += 1
        stats['batches'] += 1
        stats['edges'] += edges
        log(tag, f"[guide] Batch {batch_num}: edges={edges} tx_id={last_tx_id} (batch_size={batch_size})")

    return stats


# =============================================================================
# Token Participant Sync
# =============================================================================

def execute_with_deadlock_retry(tag, cursor, conn, query, params, max_retries, base_delay):
    """Execute a query with deadlock retry logic. Returns rowcount."""
    for attempt in range(max_retries):
        try:
            cursor.execute(query, params)
            conn.commit()
            return cursor.rowcount
        except mysql.connector.Error as e:
            if e.errno in (1213, 1205) and attempt < max_retries - 1:
                conn.rollback()
                delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
                log(tag, f"Deadlock (attempt {attempt + 1}/{max_retries}), retrying in {delay:.2f}s...")
                time.sleep(delay)
            else:
                raise
    return 0


def get_last_processed_id(cursor, config_key):
    cursor.execute(
        "SELECT config_value FROM config WHERE config_type = %s AND config_key = %s",
        (CONFIG_TYPE_SYNC, config_key))
    row = cursor.fetchone()
    return int(row['config_value']) if row else 0


def set_last_processed_id(cursor, conn, config_key, last_id):
    cursor.execute("""
        INSERT INTO config (config_type, config_key, config_value, value_type, description)
        VALUES (%s, %s, %s, 'int', %s)
        ON DUPLICATE KEY UPDATE
            config_value = VALUES(config_value),
            updated_utc = CURRENT_TIMESTAMP
    """, (CONFIG_TYPE_SYNC, config_key, str(last_id), f'Last processed tx_guide.id for {config_key}'))
    conn.commit()


def get_max_guide_id(cursor):
    cursor.execute("SELECT MAX(id) AS mx FROM tx_guide")
    row = cursor.fetchone()
    return row['mx'] if row and row['mx'] else 0


def sync_token_participants(tag, cursor, conn, last_id, max_id, batch_size,
                            max_retries, base_delay):
    """Sync new records to tx_token_participant. Returns rows affected."""
    if last_id >= max_id:
        return 0

    total_rows = 0
    current_id = last_id

    query_buys = """
        INSERT INTO tx_token_participant (
            token_id, address_id, first_seen, last_seen,
            buy_count, buy_volume, net_position
        )
        SELECT g.token_id, g.to_address_id,
               MIN(g.block_time), MAX(g.block_time),
               COUNT(*),
               SUM(g.amount / POW(10, COALESCE(g.decimals, 9))),
               SUM(g.amount / POW(10, COALESCE(g.decimals, 9)))
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

    query_sells = """
        INSERT INTO tx_token_participant (
            token_id, address_id, first_seen, last_seen,
            sell_count, sell_volume, net_position
        )
        SELECT g.token_id, g.from_address_id,
               MIN(g.block_time), MAX(g.block_time),
               COUNT(*),
               SUM(g.amount / POW(10, COALESCE(g.decimals, 9))),
               -SUM(g.amount / POW(10, COALESCE(g.decimals, 9)))
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

    query_xfer_in = """
        INSERT INTO tx_token_participant (
            token_id, address_id, first_seen, last_seen, transfer_in_count
        )
        SELECT g.token_id, g.to_address_id,
               MIN(g.block_time), MAX(g.block_time), COUNT(*)
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

    query_xfer_out = """
        INSERT INTO tx_token_participant (
            token_id, address_id, first_seen, last_seen, transfer_out_count
        )
        SELECT g.token_id, g.from_address_id,
               MIN(g.block_time), MAX(g.block_time), COUNT(*)
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

        total_rows += execute_with_deadlock_retry(tag, cursor, conn, query_buys, params, max_retries, base_delay)
        total_rows += execute_with_deadlock_retry(tag, cursor, conn, query_sells, params, max_retries, base_delay)
        total_rows += execute_with_deadlock_retry(tag, cursor, conn, query_xfer_in, params, max_retries, base_delay)
        total_rows += execute_with_deadlock_retry(tag, cursor, conn, query_xfer_out, params, max_retries, base_delay)

        current_id = batch_end

    return total_rows


# =============================================================================
# Pool label backfill
# =============================================================================

def backfill_pool_labels(tag, cursor, conn):
    """Backfill tx_guide.pool_label from tx_pool where enricher has since populated it."""
    try:
        # Quick check: any guide edges missing pool_label that tx_pool can fill?
        cursor.execute("""
            SELECT 0 AS needs_work  /* pool-backfill disabled — pool_label lives on tx_pool, not tx_guide */
        """)
        row = cursor.fetchone()
        needs_work = row.get('needs_work', 0) if isinstance(row, dict) else (row[0] if row else 0)
        if not needs_work:
            return 0

        cursor.execute("CALL sp_tx_guide_backfill_pool(@updated)")
        try:
            while cursor.nextset():
                pass
        except:
            pass
        cursor.execute("SELECT @updated AS u")
        row = cursor.fetchone()
        updated = row['u'] or 0
        if updated > 0:
            log(tag, f"[pool-backfill] {updated} guide edges updated with pool labels")
        return updated
    except Exception as e:
        log(tag, f"[pool-backfill] error: {e}")
        return 0


# =============================================================================
# Combined sync (used by both worker and manual modes)
# =============================================================================

def run_sync(tag, cursor, conn, operations, batch_size, max_retries, base_delay,
             stop_event=None, rmq_channel=None):
    """Run sync for specified operations. Returns stats dict."""
    stats = {
        'guide': {'batches': 0, 'edges': 0, 'pending': 0},
        'tokens': {'last_id': 0, 'new_id': 0, 'rows': 0},
        'pool_backfill': 0,
    }

    if 'guide' in operations:
        cursor.execute("SELECT COUNT(*) AS cnt FROM tx WHERE tx_state & 32 = 0 AND tx_state & 4 != 0 AND tx_state & 16 != 0")
        row = cursor.fetchone()
        pending = row['cnt']
        stats['guide']['pending'] = pending

        if pending > 0:
            log(tag, f"[guide] {pending:,} pending tx")
            result = sync_guide_edges(tag, cursor, conn, max_retries, base_delay, stop_event)
            stats['guide']['batches'] = result['batches']
            stats['guide']['edges'] = result['edges']
        else:
            log(tag, "[guide] No pending activities")

    max_id = get_max_guide_id(cursor)

    if 'tokens' in operations:
        last_id = get_last_processed_id(cursor, TOKEN_PARTICIPANT_KEY)
        stats['tokens']['last_id'] = last_id

        if last_id < max_id:
            log(tag, f"[tokens] Syncing {last_id:,} -> {max_id:,} ({max_id - last_id:,} records)")
            rows = sync_token_participants(tag, cursor, conn, last_id, max_id,
                                           batch_size, max_retries, base_delay)
            set_last_processed_id(cursor, conn, TOKEN_PARTICIPANT_KEY, max_id)
            stats['tokens']['new_id'] = max_id
            stats['tokens']['rows'] = rows
            log(tag, f"[tokens] {rows:,} rows affected")
        else:
            log(tag, f"[tokens] Up to date (id={last_id:,})")

    # Backfill pool labels that enricher has populated since guide_loader ran
    stats['pool_backfill'] = backfill_pool_labels(tag, cursor, conn)

    return stats


# =============================================================================
# Status
# =============================================================================

def get_sync_status(cursor):
    max_id = get_max_guide_id(cursor)
    cursor.execute("SELECT COUNT(*) AS cnt FROM tx WHERE tx_state & 32 = 0 AND tx_state & 4 != 0 AND tx_state & 16 != 0")
    pending = cursor.fetchone()['cnt']
    tokens_last = get_last_processed_id(cursor, TOKEN_PARTICIPANT_KEY)

    cursor.execute("""
        SELECT 0 AS cnt  /* pool_label lives on tx_pool, not tx_guide */
    """)
    pool_missing = cursor.fetchone()['cnt']

    return {
        'max_guide_id': max_id,
        'pending_activities': pending,
        'tokens': {
            'last_synced': tokens_last,
            'behind': max_id - tokens_last,
        },
        'pool_labels_missing': pool_missing,
    }


# =============================================================================
# Worker thread
# =============================================================================

class WorkerThread(threading.Thread):
    def __init__(self, worker_id, prefetch, stop_event,
                 poll_idle_sec, reconnect_sec, batch_size,
                 deadlock_max_retries, deadlock_base_delay):
        super().__init__(daemon=True)
        self.tag = f"W-{worker_id}"
        self.worker_id = worker_id
        self.prefetch = prefetch
        self.stop_event = stop_event
        self.poll_idle_sec = poll_idle_sec
        self.reconnect_sec = reconnect_sec
        self.batch_size = batch_size
        self.deadlock_max_retries = deadlock_max_retries
        self.deadlock_base_delay = deadlock_base_delay

    def run(self):
        log(self.tag, f"Starting (prefetch={self.prefetch})")
        db_conn = None
        cursor = None

        while not self.stop_event.is_set():
            try:
                if db_conn is None:
                    db_conn = db_connect()
                    cursor = db_conn.cursor(dictionary=True)
                    log(self.tag, "DB connected")

                rmq_conn, ch = rmq_connect()
                ch.basic_qos(prefetch_count=self.prefetch)
                log(self.tag, "RabbitMQ connected, consuming...")

                while not self.stop_event.is_set():
                    method, properties, body = ch.basic_get(queue=REQUEST_QUEUE, auto_ack=False)
                    if method is None:
                        time.sleep(self.poll_idle_sec)
                        rmq_conn.process_data_events(time_limit=0)
                        continue

                    self._handle_message(ch, method, properties, body, cursor, db_conn)

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
        log(self.tag, "Stopped")

    def _handle_message(self, ch, method, properties, body, cursor, db_conn):
        worker_log_id = None
        try:
            msg = json.loads(body.decode('utf-8'))
            request_id     = msg.get('request_id', str(uuid.uuid4()))
            correlation_id = msg.get('correlation_id', request_id)
            api_key_id     = msg.get('api_key_id')
            action         = msg.get('action', 'sync')
            priority       = msg.get('priority', 5)
            batch          = msg.get('batch', {})

            # ── DB poll: supervisor-scheduled sync pass ──
            if action == 'db-poll-sync':
                self._handle_db_poll(ch, method, cursor, db_conn)
                return

            # ── Normal queue message: gateway request ──
            operations = batch.get('operations', ['guide', 'tokens'])
            if isinstance(operations, str):
                operations = [op.strip() for op in operations.split(',')]

            log(self.tag, f"Request {request_id[:8]} (action={action}, ops={operations})")

            worker_log_id = log_worker_request(
                cursor, db_conn, request_id, correlation_id, priority, api_key_id)

            stats = run_sync(self.tag, cursor, db_conn, operations,
                             self.batch_size, self.deadlock_max_retries,
                             self.deadlock_base_delay, self.stop_event,
                             rmq_channel=ch)

            total = stats['guide']['edges'] + stats['tokens']['rows']
            result = {
                'processed': total,
                'guide_edges': stats['guide']['edges'],
                'token_rows': stats['tokens']['rows'],
                'pool_backfill': stats['pool_backfill'],
            }

            update_worker_request(cursor, db_conn, worker_log_id, 'completed', result)

            self._publish_response(ch, request_id, correlation_id, 'completed', result)
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

    def _handle_db_poll(self, ch, method, cursor, db_conn):
        """Handle supervisor-scheduled DB poll — loops until no more work."""
        total_edges = 0
        total_token_rows = 0
        pass_num = 0

        while not self.stop_event.is_set():
            stats = run_sync(self.tag, cursor, db_conn, ['guide', 'tokens'],
                             self.batch_size, self.deadlock_max_retries,
                             self.deadlock_base_delay, self.stop_event,
                             rmq_channel=ch)

            edges = stats['guide']['edges']
            token_rows = stats['tokens']['rows']
            total_edges += edges
            total_token_rows += token_rows
            pass_num += 1

            if edges == 0 and token_rows == 0:
                break

            log(self.tag, f"  pass {pass_num}: {edges} edges, {token_rows} token rows")

        if total_edges > 0 or total_token_rows > 0:
            log(self.tag, f"DB poll complete: {total_edges} edges, {total_token_rows} token rows ({pass_num} passes)")
            try:
                dl_id = log_daemon_request(cursor, db_conn, 'db-poll-sync', total_edges + total_token_rows)
                update_worker_request(cursor, db_conn, dl_id, 'completed', {
                    'passes': pass_num,
                    'guide_edges': total_edges,
                    'token_rows': total_token_rows,
                })
            except Exception as e:
                log(self.tag, f"Failed to log request: {e}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def _publish_response(self, ch, request_id, correlation_id, status, result):
        body = json.dumps({
            'request_id':     request_id,
            'correlation_id': correlation_id,
            'worker':         'aggregator',
            'status':         status,
            'timestamp':      datetime.now(timezone.utc).isoformat(),
            'result':         result,
        })
        ch.basic_publish(
            exchange='', routing_key=RESPONSE_QUEUE,
            body=body.encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2, content_type='application/json'))

    def _publish_cascade_to_enricher(self, ch, request_id, correlation_id, result, priority):
        cascade_msg = json.dumps({
            'request_id': f"{request_id}-enricher",
            'correlation_id': correlation_id,
            'parent_request_id': request_id,
            'action': 'cascade',
            'source_worker': 'aggregator',
            'priority': priority,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'batch': {
                'operations': 'tokens,pools',
                'source_processed': result.get('processed', 0),
            }
        })
        try:
            ch.basic_publish(
                exchange='', routing_key=ENRICHER_REQUEST_QUEUE,
                body=cascade_msg.encode('utf-8'),
                properties=pika.BasicProperties(
                    delivery_mode=2, content_type='application/json', priority=priority))
            log(self.tag, "[CASCADE] -> enricher (tokens,pools)")
        except Exception as e:
            log(self.tag, f"Failed to cascade to enricher: {e}")


# =============================================================================
# Supervisor
# =============================================================================

def read_config(cursor):
    cfg = load_db_config(cursor, 'queue')
    return {
        'threads':          cfg_int(cfg, 'aggregator_wrk_cnt_threads', 0),
        'prefetch':         cfg_int(cfg, 'aggregator_wrk_cnt_prefetch', 5),
        'supervisor_poll':  cfg_float(cfg, 'aggregator_wrk_supervisor_poll_sec', 5.0),
        'poll_idle':        cfg_float(cfg, 'aggregator_wrk_poll_idle_sec', 0.25),
        'reconnect':        cfg_float(cfg, 'aggregator_wrk_reconnect_sec', 5.0),
        'shutdown_timeout': cfg_float(cfg, 'aggregator_wrk_shutdown_timeout_sec', 10.0),
        'batch_size':       cfg_int(cfg, 'aggregator_wrk_batch_size', 10000),
        'deadlock_max':     cfg_int(cfg, 'aggregator_wrk_deadlock_max_retries', 5),
        'deadlock_delay':   cfg_float(cfg, 'aggregator_wrk_deadlock_base_delay', 0.1),
        'db_poll':          cfg_float(cfg, 'aggregator_wrk_db_poll_sec', 0),
    }


def run_supervisor():
    print(f"""
+-----------------------------------------------------------+
|  Guide Aggregator - Supervisor                            |
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
        # Skip if queue already has pending messages (workers loop until drained)
        result = svr_rmq_ch.queue_declare(queue=REQUEST_QUEUE, passive=True)
        if result.method.message_count > 0:
            return
        msg = json.dumps({
            'request_id': f"dbpoll-{uuid.uuid4().hex[:12]}",
            'action': 'db-poll-sync',
        })
        svr_rmq_ch.basic_publish(
            exchange='', routing_key=REQUEST_QUEUE,
            body=msg.encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2, content_type='application/json'))
        log('SVR', 'Published db-poll-sync to queue')

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
                    next_id, cfg['prefetch'], stop_evt,
                    cfg['poll_idle'], cfg['reconnect'], cfg['batch_size'],
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

            # DB poll scheduler
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
# Manual modes
# =============================================================================

def run_manual(args):
    """Run sync manually (single run)."""
    conn = db_connect()
    cursor = conn.cursor(dictionary=True)

    try:
        if args.status:
            status = get_sync_status(cursor)
            print(f"\n{'='*60}")
            print(f"  Guide Aggregator Status")
            print(f"{'='*60}")
            print(f"  tx_guide max id:          {status['max_guide_id']:,}")
            print(f"  Pending activities:       {status['pending_activities']:,}")
            print(f"  Token participant synced: {status['tokens']['last_synced']:,} (behind: {status['tokens']['behind']:,})")
            print(f"  Pool labels missing:      {status['pool_labels_missing']:,}")
            print(f"{'='*60}")
            return 0

        if args.sync.lower() == 'all':
            operations = ['guide', 'tokens']
        else:
            operations = [op.strip().lower() for op in args.sync.split(',')]
            valid_ops = {'guide', 'tokens'}
            invalid = set(operations) - valid_ops
            if invalid:
                print(f"Error: Invalid operations: {invalid}. Valid: guide, tokens, all")
                return 1

        log('MANUAL', f"Operations: {', '.join(operations)}, batch_size={args.batch_size}")

        stats = run_sync('MANUAL', cursor, conn, operations, args.batch_size, 5, 0.1)

        total = stats['guide']['edges'] + stats['tokens']['rows']
        print(f"\n{'='*60}")
        print(f"  Sync Complete")
        print(f"{'='*60}")
        print(f"  Pool backfill:     {stats['pool_backfill']:,}")
        print(f"  Guide edges:       {stats['guide']['edges']:,}")
        print(f"  Token participant: {stats['tokens']['rows']:,}")
        print(f"  Total:             {total:,}")
        print(f"{'='*60}")

    finally:
        conn.close()

    return 0


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Guide Aggregator - Guide edges & token participant sync')
    parser.add_argument('--sync', '-s', type=str, default='all',
                        help='Operations: guide,tokens,all (default: all)')
    parser.add_argument('--batch-size', '-b', type=int, default=10000,
                        help='Token participant batch size (default: 10000)')
    parser.add_argument('--status', action='store_true',
                        help='Show sync status')
    args = parser.parse_args()

    if args.status or args.sync != 'all' or args.batch_size != 10000:
        return run_manual(args)
    else:
        return run_supervisor()


if __name__ == '__main__':
    sys.exit(main() or 0)
