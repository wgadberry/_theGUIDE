#!/usr/bin/env python3
"""
Guide Aggregator - Consolidated daemon for guide table synchronization

Merges functionality from:
- guide-loader.py: Processes tx_activity into tx_guide via sp_tx_guide_batch
- guide-sync-funding.py: Syncs tx_token_participant

Pipeline position:
    guide-shredder.py → tx_activity, tx_swap, tx_transfer
                             ↓
                    guide-aggregator.py (this script)
                             ↓
              tx_guide, tx_token_participant

Usage:
    python guide-aggregator.py                          # All operations, single run
    python guide-aggregator.py --daemon --interval 30   # Continuous mode
    python guide-aggregator.py --sync guide             # Only sp_tx_guide_batch
    python guide-aggregator.py --sync tokens            # Only tx_token_participant
    python guide-aggregator.py --status                 # Show sync status
"""

import argparse
import sys
import time
import random
import os
import json
import functools
import uuid
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Callable, List, Tuple

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
# Config Loading
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

    # Environment variable fallback/override
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

# Database configuration
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
RABBITMQ_REQUEST_QUEUE = 'mq.guide.aggregator.request'
RABBITMQ_RESPONSE_QUEUE = 'mq.guide.aggregator.response'
ENRICHER_REQUEST_QUEUE = 'mq.guide.enricher.request'  # For incremental cascade

# Deadlock retry settings
MAX_DEADLOCK_RETRIES = 5
DEADLOCK_BASE_DELAY = 0.1

# Config table keys for last processed ID tracking
CONFIG_TYPE = 'sync'
TOKEN_PARTICIPANT_KEY = 'token_participant_last_guide_id'


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
        sys.exit(1)


def execute_with_deadlock_retry(cursor, conn, query: str, params: tuple = None,
                                 max_retries: int = MAX_DEADLOCK_RETRIES) -> int:
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


# =============================================================================
# Daemon Request Logging (for billing)
# =============================================================================

def log_daemon_request(cursor, conn, worker: str, action: str, operations: list = None) -> int:
    """
    Create a request_log entry for daemon activity.
    Returns the request_log_id for billing linkage.
    """
    request_id = f"daemon-{worker}-{uuid.uuid4().hex[:12]}"
    payload_summary = json.dumps({
        'operations': operations or [],
        'mode': 'daemon'
    })

    cursor.execute("""
        INSERT INTO tx_request_log
        (request_id, correlation_id, api_key_id, source, target_worker, action, priority, status, payload_summary)
        VALUES (%s, %s, NULL, 'daemon', %s, %s, 5, 'processing', %s)
    """, (request_id, request_id, worker, action, payload_summary))
    conn.commit()
    return cursor.lastrowid


def update_daemon_request(cursor, conn, request_log_id: int, status: str, result: dict = None):
    """Update daemon request_log entry with completion status and results."""
    result_summary = json.dumps(result) if result else None
    cursor.execute("""
        UPDATE tx_request_log
        SET status = %s, result_summary = %s, completed_at = NOW()
        WHERE id = %s
    """, (status, result_summary, request_log_id))
    conn.commit()


# =============================================================================
# Config Table Helpers
# =============================================================================

def get_last_processed_id(cursor, config_key: str) -> int:
    """Get last processed tx_guide.id from config table."""
    cursor.execute("""
        SELECT config_value FROM config
        WHERE config_type = %s AND config_key = %s
    """, (CONFIG_TYPE, config_key))
    row = cursor.fetchone()
    return int(row[0]) if row else 0


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


# =============================================================================
# Guide Loading (from guide-loader.py)
# =============================================================================

def get_config_int(cursor, config_key: str, default: int) -> int:
    """Get an integer config value from config table (runtime-editable)."""
    try:
        cursor.execute("CALL sp_config_get(%s, %s)", ('batch', config_key))
        result = cursor.fetchone()
        try:
            while cursor.nextset():
                pass
        except:
            pass
        if result:
            value = result[0] if isinstance(result, tuple) else result.get('config_value')
            if value:
                return int(value)
    except Exception:
        pass
    return default


def get_guide_batch_size(cursor, default: int = 1000) -> int:
    """Get guide batch size from config table (runtime-editable)."""
    return get_config_int(cursor, 'guide_batch_size', default)


def get_guide_batch_interval(cursor, default: int = 30) -> int:
    """Get guide batch interval (seconds) from config table (runtime-editable)."""
    return get_config_int(cursor, 'guide_batch_interval', default)


def get_pending_activity_count(cursor) -> int:
    """Get count of tx records not yet guide-loaded (bit 32)."""
    cursor.execute("SELECT COUNT(*) FROM tx WHERE tx_state & 32 = 0")
    return cursor.fetchone()[0]


def run_guide_loader(cursor, conn, batch_size: int = 1000) -> tuple:
    """
    Execute single batch of sp_tx_guide_loader with deadlock retry.
    Proc selects next batch_size tx records where tx_state bit 32 is unset.
    Returns (guide_count, last_tx_id).
    """
    for attempt in range(MAX_DEADLOCK_RETRIES):
        try:
            cursor.execute("SET @rows = 0, @last = 0")
            cursor.execute("CALL sp_tx_guide_loader(%s, @rows, @last)",
                           (batch_size,))
            try:
                while cursor.nextset():
                    pass
            except:
                pass
            cursor.execute("SELECT @rows, @last")
            result = cursor.fetchone()
            conn.commit()
            return (result[0] or 0, result[1] or 0)
        except mysql.connector.Error as err:
            if err.errno == 1213 and attempt < MAX_DEADLOCK_RETRIES - 1:
                backoff = DEADLOCK_BASE_DELAY * (2 ** attempt) + random.uniform(0, 0.5)
                print(f"    [!] Deadlock (attempt {attempt + 1}), retrying in {backoff:.1f}s...")
                conn.rollback()
                time.sleep(backoff)
            else:
                raise
    return (0, 0)


def sync_guide_edges(cursor, conn, batch_size: int = 10000,
                     max_batches: int = 0, verbose: bool = True) -> Dict[str, int]:
    """
    Process pending activities via sp_tx_guide_loader.
    Returns stats dict.
    """
    stats = {'batches': 0, 'guide': 0, 'deadlocks': 0}
    batch_num = 0

    while True:
        # Re-read batch size from config each iteration (runtime-editable)
        batch_size = get_guide_batch_size(cursor, batch_size)
        guide, last_tx_id = run_guide_loader(cursor, conn, batch_size)

        if last_tx_id == 0:
            break  # No tx records to process

        batch_num += 1
        stats['batches'] += 1
        stats['guide'] += guide

        if verbose:
            print(f"    Batch {batch_num}: guide={guide} tx_id={last_tx_id} (batch_size={batch_size})")

        if max_batches > 0 and batch_num >= max_batches:
            if verbose:
                print(f"    Reached max batches ({max_batches})")
            break

    return stats


# =============================================================================
# Token Participant Sync (from guide-sync-funding.py)
# =============================================================================

def sync_token_participants(cursor, conn, last_id: int, max_id: int,
                            batch_size: int = 10000) -> int:
    """Sync new records to tx_token_participant. Returns rows affected."""
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

    # Sells (swap_out - wallet sends tokens)
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

    # Transfers in
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

    # Transfers out
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

        total_rows += execute_with_deadlock_retry(cursor, conn, query_buys, params)
        total_rows += execute_with_deadlock_retry(cursor, conn, query_sells, params)
        total_rows += execute_with_deadlock_retry(cursor, conn, query_xfer_in, params)
        total_rows += execute_with_deadlock_retry(cursor, conn, query_xfer_out, params)

        current_id = batch_end

    return total_rows


# =============================================================================
# Main Aggregation Logic
# =============================================================================

def run_sync(cursor, conn, operations: List[str], batch_size: int = 10000,
             guide_batch_size: int = 1000, max_batches: int = 0,
             verbose: bool = True) -> Dict[str, Any]:
    """
    Run sync for specified operations.
    operations: list containing 'guide', 'tokens'
    Returns stats dict.
    """
    stats = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'guide': {'pending': 0, 'batches': 0, 'edges': 0},
        'tokens': {'last_id': 0, 'new_id': 0, 'rows': 0},
    }

    # Guide edges (sp_tx_guide_loader)
    if 'guide' in operations:
        pending = get_pending_activity_count(cursor)
        stats['guide']['pending'] = pending

        if pending > 0:
            # Get batch size from config table (runtime-editable), fallback to parameter
            db_batch_size = get_guide_batch_size(cursor, guide_batch_size)
            if verbose:
                print(f"  [guide] Processing {pending:,} pending tx (batch_size={db_batch_size})...")
            result = sync_guide_edges(cursor, conn, db_batch_size, max_batches, verbose)
            stats['guide']['batches'] = result['batches']
            stats['guide']['edges'] = result['guide']
        else:
            if verbose:
                print(f"  [guide] No pending activities")

    max_id = get_max_guide_id(cursor)

    # Token participants
    if 'tokens' in operations:
        last_id = get_last_processed_id(cursor, TOKEN_PARTICIPANT_KEY)
        stats['tokens']['last_id'] = last_id

        if last_id < max_id:
            if verbose:
                print(f"  [tokens] Syncing {last_id:,} -> {max_id:,} ({max_id - last_id:,} records)")
            rows = sync_token_participants(cursor, conn, last_id, max_id, batch_size)
            set_last_processed_id(cursor, conn, TOKEN_PARTICIPANT_KEY, max_id)
            stats['tokens']['new_id'] = max_id
            stats['tokens']['rows'] = rows
            if verbose:
                print(f"  [tokens] {rows:,} rows affected")
        else:
            if verbose:
                print(f"  [tokens] Up to date (id={last_id:,})")

    return stats


def get_sync_status(cursor) -> Dict[str, Any]:
    """Get current sync status for all operations."""
    max_id = get_max_guide_id(cursor)
    pending = get_pending_activity_count(cursor)
    tokens_last = get_last_processed_id(cursor, TOKEN_PARTICIPANT_KEY)

    return {
        'max_guide_id': max_id,
        'pending_activities': pending,
        'tokens': {
            'last_synced': tokens_last,
            'behind': max_id - tokens_last,
        },
    }


def print_status(status: Dict):
    """Print sync status in human-readable format."""
    print(f"\n{'='*60}")
    print(f"Guide Aggregator Status")
    print(f"{'='*60}")
    print(f"  tx_guide max id:          {status['max_guide_id']:,}")
    print(f"  Pending activities:       {status['pending_activities']:,}")
    print(f"  Token participant synced: {status['tokens']['last_synced']:,} (behind: {status['tokens']['behind']:,})")
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


def publish_response(channel, request_id: str, status: str, result: dict,
                     error: str = None, request_log_id: int = None):
    """Publish response to gateway response queue"""
    response = {
        'request_id': request_id,
        'worker': 'aggregator',
        'status': status,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'result': result
    }
    if error:
        response['error'] = error
    if request_log_id:
        response['request_log_id'] = request_log_id

    channel.basic_publish(
        exchange='',
        routing_key=RABBITMQ_RESPONSE_QUEUE,
        body=json.dumps(response).encode('utf-8'),
        properties=pika.BasicProperties(delivery_mode=2, priority=5)
    )


def publish_cascade_to_enricher(channel, request_id: str, correlation_id: str,
                                 result: dict, priority: int = 5,
                                 request_log_id: int = None) -> bool:
    """Publish cascade directly to enricher request queue (incremental cascade)"""
    cascade_msg = {
        'request_id': f"{request_id}-enricher",
        'correlation_id': correlation_id,  # Track original REST request
        'parent_request_id': request_id,
        'action': 'cascade',
        'source_worker': 'aggregator',
        'priority': priority,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'batch': {
            'operations': 'tokens,pools',
            'source_processed': result.get('processed', 0)
        }
    }
    if request_log_id:
        cascade_msg['request_log_id'] = request_log_id

    try:
        channel.basic_publish(
            exchange='',
            routing_key=ENRICHER_REQUEST_QUEUE,
            body=json.dumps(cascade_msg).encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='application/json',
                priority=priority
            )
        )
        print(f"  [CASCADE] → enricher (tokens,pools)")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to cascade to enricher: {e}")
        return False


def run_queue_consumer(prefetch: int = 1):
    """Run as a queue consumer, listening for gateway requests"""
    print(f"""
+-----------------------------------------------------------+
|         Guide Aggregator - Queue Consumer Mode            |
|                                                           |
|  vhost:     {RABBITMQ_VHOST}                                     |
|  queue:     {RABBITMQ_REQUEST_QUEUE}           |
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
                    correlation_id = message.get('correlation_id', request_id)  # Track original request
                    request_log_id = message.get('request_log_id')  # For billing linkage
                    batch = message.get('batch', {})
                    priority = message.get('priority', 5)

                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Received request {request_id[:8]} (correlation: {correlation_id[:8]}, log_id: {request_log_id or 'N/A'})")

                    # Ensure DB connection is alive
                    cursor, conn = ensure_db_connection()
                    if not cursor:
                        raise Exception("Database connection unavailable")

                    # Get operations from batch or default to guide,tokens
                    operations = batch.get('operations', ['guide', 'tokens'])
                    if isinstance(operations, str):
                        operations = [op.strip() for op in operations.split(',')]

                    print(f"  Running operations: {', '.join(operations)}")

                    try:
                        # Run sync operations
                        stats = run_sync(cursor, conn, operations,
                                        batch_size=batch.get('batch_size', 10000),
                                        guide_batch_size=batch.get('guide_batch_size', 1000),
                                        max_batches=batch.get('max_batches', 0),
                                        verbose=True)

                        total = (stats['guide']['edges'] +
                                stats['tokens']['rows'])

                        result = {
                            'processed': total,
                            'guide_edges': stats['guide']['edges'],
                            'token_rows': stats['tokens']['rows']
                        }
                        status = 'completed'
                        print(f"  Completed: {total} total rows processed")

                    except Exception as e:
                        print(f"  [ERROR] {e}")
                        result = {'processed': 0, 'error': str(e)}
                        status = 'failed'

                    # Publish response
                    publish_response(gateway_channel, request_id, status, result,
                                    request_log_id=request_log_id)

                    # Cascade to enricher if we processed data
                    if status == 'completed' and result.get('processed', 0) > 0:
                        publish_cascade_to_enricher(gateway_channel, request_id, correlation_id, result,
                                                   priority, request_log_id)

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
        description='Guide Aggregator - Consolidated daemon for guide table synchronization',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Operations (--sync):
  guide   - Process tx_activity into tx_guide via sp_tx_guide_batch
  tokens  - Sync tx_token_participant from tx_guide
  all     - All operations (default)

Examples:
  python guide-aggregator.py                          # All operations, single run
  python guide-aggregator.py --daemon --interval 30   # Continuous mode
  python guide-aggregator.py --sync guide             # Only sp_tx_guide_batch
  python guide-aggregator.py --sync tokens            # Only tx_token_participant
  python guide-aggregator.py --status                 # Show sync status
        """
    )

    parser.add_argument('--sync', '-s', type=str, default='all',
                        help='Operations to run: guide,tokens,all (default: all)')
    parser.add_argument('--daemon', '-d', action='store_true',
                        help='Run continuously in daemon mode')
    parser.add_argument('--interval', '-i', type=int, default=30,
                        help='Sync interval in seconds for daemon mode (default: 30)')
    parser.add_argument('--batch-size', '-b', type=int, default=10000,
                        help='Batch size for tokens sync (default: 10000)')
    parser.add_argument('--guide-batch-size', type=int, default=1000,
                        help='Batch size for guide edge processing (default: 1000)')
    parser.add_argument('--max-batches', type=int, default=0,
                        help='Max batches for guide processing (0 = unlimited)')
    parser.add_argument('--status', action='store_true',
                        help='Show current sync status and exit')
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

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        return 1

    # Parse operations
    if args.sync.lower() == 'all':
        operations = ['guide', 'tokens']
    else:
        operations = [op.strip().lower() for op in args.sync.split(',')]
        valid_ops = {'guide', 'tokens'}
        invalid = set(operations) - valid_ops
        if invalid:
            print(f"Error: Invalid operations: {invalid}. Valid: guide, tokens, all")
            return 1

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if args.status:
            status = get_sync_status(cursor)
            if args.json:
                print(json.dumps(status, indent=2))
            else:
                print_status(status)
            return 0

        if not args.quiet and not args.json:
            print(f"\n{'='*60}")
            print(f"Guide Aggregator")
            print(f"{'='*60}")
            print(f"  Database:   {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
            print(f"  Operations: {', '.join(operations)}")
            print(f"  Mode:       {'DAEMON' if args.daemon else 'SINGLE RUN'}")
            if args.daemon:
                print(f"  Interval:   {args.interval}s")
            print(f"{'='*60}\n")

        if args.daemon:
            print("Press Ctrl+C to stop\n")

            while True:
                request_log_id = None
                try:
                    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                    if not args.quiet:
                        print(f"[{timestamp}] Running sync...")

                    stats = run_sync(cursor, conn, operations,
                                    args.batch_size, args.guide_batch_size,
                                    args.max_batches, verbose=not args.quiet)

                    total = (stats['guide']['edges'] +
                            stats['tokens']['rows'])

                    # Only log if there were actual changes (avoid cluttering request_log)
                    if total > 0:
                        request_log_id = log_daemon_request(cursor, conn, 'aggregator', 'sync', operations)
                        update_daemon_request(cursor, conn, request_log_id, 'completed', {
                            'guide_edges': stats['guide']['edges'],
                            'token_rows': stats['tokens']['rows'],
                            'total': total
                        })

                    if args.json:
                        print(json.dumps(stats))
                    elif not args.quiet:
                        if total > 0:
                            print(f"[{timestamp}] Complete: {total:,} rows affected\n")
                        else:
                            print(f"[{timestamp}] No changes\n")

                    # Re-read interval from config (runtime-editable)
                    interval = get_guide_batch_interval(cursor, args.interval)

                    # Close connection before sleeping
                    try:
                        cursor.close()
                        conn.close()
                    except:
                        pass

                    time.sleep(interval)

                    # Reconnect after sleep
                    conn = get_db_connection()
                    cursor = conn.cursor()

                except KeyboardInterrupt:
                    print("\nStopping daemon...")
                    break
                except Exception as e:
                    print(f"Error: {e}")
                    # Update request_log with error if we have one
                    if request_log_id:
                        try:
                            update_daemon_request(cursor, conn, request_log_id, 'failed', {'error': str(e)})
                        except:
                            pass
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
            stats = run_sync(cursor, conn, operations,
                            args.batch_size, args.guide_batch_size,
                            args.max_batches, verbose=not args.quiet)

            if args.json:
                print(json.dumps(stats, indent=2))
            elif not args.quiet:
                total = (stats['guide']['edges'] +
                        stats['tokens']['rows'])
                print(f"\n{'='*60}")
                print(f"Sync Complete")
                print(f"{'='*60}")
                print(f"  Guide edges:        {stats['guide']['edges']:,}")
                print(f"  Token participant:  {stats['tokens']['rows']:,}")
                print(f"  Total:              {total:,}")
                print(f"{'='*60}\n")

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

    return 0


if __name__ == '__main__':
    sys.exit(main())
