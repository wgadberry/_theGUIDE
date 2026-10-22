#!/usr/bin/env python3
"""
Guide Decoder - Fetches decoded transaction data and stages for shredding

Consumes signature batches from gateway, fetches decoded data from Solscan,
and inserts into staging table with tx_state=8 (decoded).

Usage:
    python guide-decoder.py --queue-consumer          # Gateway mode (primary)
    python guide-decoder.py --queue-consumer --dry-run
"""

import argparse
import json
import os
import time
import requests
from datetime import datetime

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

try:
    import pika
    HAS_PIKA = True
except ImportError:
    HAS_PIKA = False

# =============================================================================
# Configuration (from guide-config.json)
# =============================================================================

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'guide-config.json')
    try:
        with open(config_path) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

_cfg = load_config()

SOLSCAN_API_BASE = _cfg.get('SOLSCAN_API', "https://pro-api.solscan.io/v2.0")
SOLSCAN_API_TOKEN = _cfg.get('SOLSCAN_TOKEN', "")

RABBITMQ_HOST = _cfg.get('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = _cfg.get('RABBITMQ_PORT', 5692)
RABBITMQ_USER = _cfg.get('RABBITMQ_USER', 'admin')
RABBITMQ_PASS = _cfg.get('RABBITMQ_PASSWORD', 'admin123')
RABBITMQ_VHOST = _cfg.get('RABBITMQ_VHOST', 't16o_mq')
RABBITMQ_REQUEST_QUEUE = 'mq.guide.decoder.request'
RABBITMQ_RESPONSE_QUEUE = 'mq.guide.decoder.response'

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

# Staging table config
STAGING_SCHEMA = 't16o_db_staging'
STAGING_TABLE = 'txs'
TX_STATE_DECODED = 8  # Will be fetched from config table


# =============================================================================
# Request Logging (for billing tracking)
# =============================================================================

def log_worker_request(cursor, conn, request_id: str, correlation_id: str,
                       worker: str, action: str, batch_num: int = 0,
                       batch_size: int = 0, priority: int = 5,
                       api_key_id: int = None, features: int = 0) -> int:
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
        return existing[0]

    # Insert new record
    payload_summary = json.dumps({
        'batch_num': batch_num,
        'batch_size': batch_size,
        'source': 'queue'
    })

    cursor.execute("""
        INSERT INTO tx_request_log
        (request_id, correlation_id, api_key_id, source, target_worker, action, priority, features, status, payload_summary)
        VALUES (%s, %s, %s, 'queue', %s, %s, %s, %s, 'processing', %s)
    """, (request_id, correlation_id, api_key_id, worker, action, priority, features, payload_summary))
    conn.commit()
    return cursor.lastrowid


def update_worker_request(cursor, conn, log_id: int, status: str, result: dict = None):
    """Update worker request_log entry with completion status and results."""
    result_summary = json.dumps(result) if result else None
    cursor.execute("""
        UPDATE tx_request_log
        SET status = %s, result_summary = %s, completed_at = NOW()
        WHERE id = %s
    """, (status, result_summary, log_id))
    conn.commit()


# =============================================================================
# Solscan API
# =============================================================================

def create_solscan_session() -> requests.Session:
    """Create requests session with Solscan auth"""
    session = requests.Session()
    session.headers.update({"token": SOLSCAN_API_TOKEN})
    return session


def fetch_decoded_batch(session: requests.Session, signatures: list) -> dict:
    """Fetch decoded data for batch of signatures"""
    tx_params = '&'.join([f'tx[]={sig}' for sig in signatures])
    url = f"{SOLSCAN_API_BASE}/transaction/actions/multi?{tx_params}"
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


# =============================================================================
# Pre-filter: Check for existing signatures
# =============================================================================

def filter_existing_signatures(cursor, signatures: list) -> tuple:
    """
    Filter signatures based on existence in tx table.
    Returns tuple: (new_signatures, existing_signatures)
    """
    if not signatures:
        return [], []

    placeholders = ','.join(['%s'] * len(signatures))
    cursor.execute(f"SELECT signature FROM tx WHERE signature IN ({placeholders})", signatures)

    existing = {row[0] for row in cursor.fetchall()}

    if not existing:
        return signatures, []

    new_sigs = [sig for sig in signatures if sig not in existing]
    existing_sigs = [sig for sig in signatures if sig in existing]

    return new_sigs, existing_sigs


# =============================================================================
# Core Processing
# =============================================================================

class DecoderProcessor:
    """Core decoder processing logic - fetches and stages decoded data"""

    def __init__(self, db_conn, solscan_session: requests.Session, dry_run: bool = False):
        self.db_conn = db_conn
        self.cursor = db_conn.cursor()
        self.solscan_session = solscan_session
        self.dry_run = dry_run
        self._tx_state_decoded = None

    def get_tx_state_decoded(self) -> int:
        """Get decoded tx_state value from config table"""
        if self._tx_state_decoded is None:
            self.cursor.execute(
                "SELECT CAST(config_value AS UNSIGNED) FROM config "
                "WHERE config_type = 'tx_state' AND config_key = 'decoded'"
            )
            row = self.cursor.fetchone()
            self._tx_state_decoded = row[0] if row else TX_STATE_DECODED
        return self._tx_state_decoded

    def insert_staging(self, decoded_response: dict, priority: int = 5, correlation_id: str = None,
                        sig_hash: str = None, request_log_id: int = None) -> int:
        """Insert decoded JSON into staging table, return staging_id"""
        tx_state = self.get_tx_state_decoded()

        self.cursor.execute(f"""
            INSERT INTO {STAGING_SCHEMA}.{STAGING_TABLE} (txs, tx_state, priority, correlation_id, sig_hash, request_log_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (json.dumps(decoded_response), tx_state, priority, correlation_id, sig_hash, request_log_id))

        staging_id = self.cursor.lastrowid
        self.db_conn.commit()
        return staging_id

    def process_signatures(self, signatures: list, priority: int = 5, correlation_id: str = None,
                          sig_hash: str = None, request_log_id: int = None) -> dict:
        """Core processing logic for a batch of signatures"""
        result = {
            'processed': 0,
            'skipped': 0,
            'new_sigs': [],
            'staging_id': None,
            'tx_count': 0,
            'error': None
        }

        if not signatures:
            return result

        # Phase 0: Pre-filter existing signatures
        original_count = len(signatures)
        new_sigs, existing_sigs = filter_existing_signatures(self.cursor, signatures)
        result['skipped'] = len(existing_sigs)

        if result['skipped'] > 0:
            print(f"  [~] Skipped {result['skipped']}/{original_count} already in tx table")

        if not new_sigs:
            print(f"  [=] No new signatures to decode")
            return result

        if self.dry_run:
            print(f"  [DRY] Would decode {len(new_sigs)} signatures")
            result['processed'] = len(new_sigs)
            return result

        # Phase 1: Fetch decoded data from Solscan
        start_time = time.time()
        try:
            decoded_response = fetch_decoded_batch(self.solscan_session, new_sigs)
        except requests.RequestException as e:
            result['error'] = f'Solscan API error: {e}'
            return result
        fetch_time = time.time() - start_time

        if not decoded_response.get('success') or not decoded_response.get('data'):
            result['error'] = 'Solscan API returned no data'
            return result

        # Get actual signatures returned by Solscan
        actual_sigs = [
            tx.get('tx_hash') or tx.get('signature')
            for tx in decoded_response.get('data', [])
            if tx.get('tx_hash') or tx.get('signature')
        ]
        result['new_sigs'] = actual_sigs
        result['tx_count'] = len(actual_sigs)

        if len(actual_sigs) < len(new_sigs):
            print(f"  [~] Solscan returned {len(actual_sigs)}/{len(new_sigs)} signatures")

        # Phase 2: Insert into staging table
        staging_start = time.time()
        staging_id = self.insert_staging(decoded_response, priority, correlation_id, sig_hash, request_log_id)
        staging_time = time.time() - staging_start

        result['staging_id'] = staging_id
        result['processed'] = len(actual_sigs)

        print(f"  [+] Staged {len(actual_sigs)} txs → staging.id={staging_id} "
              f"(fetch={fetch_time:.2f}s, insert={staging_time:.3f}s)")

        return result


# =============================================================================
# Gateway Integration
# =============================================================================

def setup_gateway_rabbitmq():
    """Setup RabbitMQ connection to t16o_mq vhost"""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT, virtual_host=RABBITMQ_VHOST,
        credentials=credentials, heartbeat=600, blocked_connection_timeout=300
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    for queue in [RABBITMQ_REQUEST_QUEUE, RABBITMQ_RESPONSE_QUEUE]:
        channel.queue_declare(queue=queue, durable=True, arguments={'x-max-priority': 10})

    return connection, channel


def publish_response(channel, request_id: str, correlation_id: str, status: str, result: dict, batch_num: int = 0, error: str = None):
    """Publish response to decoder response queue"""
    # Include batch_num in result for gateway tracking
    result['batch_num'] = batch_num

    response = {
        'request_id': request_id,
        'correlation_id': correlation_id,
        'worker': 'decoder',
        'status': status,
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'result': result
    }
    if error:
        response['error'] = error

    channel.basic_publish(
        exchange='', routing_key=RABBITMQ_RESPONSE_QUEUE,
        body=json.dumps(response).encode('utf-8'),
        properties=pika.BasicProperties(delivery_mode=2, content_type='application/json')
    )


def run_queue_consumer(prefetch: int = 1, dry_run: bool = False):
    """Run as a queue consumer, listening for gateway requests"""
    print(f"""
+-----------------------------------------------------------+
|         Guide Decoder - Queue Consumer Mode               |
|  vhost: {RABBITMQ_VHOST}  |  queue: {RABBITMQ_REQUEST_QUEUE}  |
+-----------------------------------------------------------+
""")
    if dry_run:
        print("MODE: DRY RUN")

    solscan_session = create_solscan_session()
    db_state = {'conn': None, 'processor': None}

    def ensure_db_connection():
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
                db_state['processor'] = DecoderProcessor(db_state['conn'], solscan_session, dry_run)
                print("[OK] Database (re)connected")
            return db_state['processor']
        except Exception as e:
            print(f"[WARN] Database connection failed: {e}")
            return None

    ensure_db_connection()

    while True:
        try:
            gateway_conn, gateway_channel = setup_gateway_rabbitmq()
            gateway_channel.basic_qos(prefetch_count=prefetch)
            print(f"[OK] Connected, waiting for requests...")

            def callback(ch, method, properties, body):
                worker_log_id = None
                try:
                    message = json.loads(body.decode('utf-8'))
                    request_id = message.get('request_id', 'unknown')
                    correlation_id = message.get('correlation_id', request_id)
                    request_log_id = message.get('request_log_id')  # For billing linkage
                    api_key_id = message.get('api_key_id')  # For billing tracking
                    sig_hash = message.get('sig_hash')  # For pairing with detailer
                    batch_data = message.get('batch', {})
                    priority = message.get('priority', 5)
                    features = message.get('features', 0)

                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Request {request_id[:8]} "
                          f"(correlation: {correlation_id[:8]}, sig_hash: {sig_hash[:8] if sig_hash else 'N/A'})")

                    processor = ensure_db_connection()
                    if not processor:
                        raise Exception("Database connection unavailable")

                    signatures = batch_data.get('signatures', [])
                    batch_num = batch_data.get('batch_num', 0)

                    # Log this worker's request for billing tracking
                    worker_log_id = log_worker_request(
                        processor.cursor, processor.db_conn,
                        request_id, correlation_id, 'decoder', 'decode',
                        batch_num, len(signatures), priority, api_key_id, features
                    )

                    if not signatures:
                        print(f"  No signatures in batch")
                        update_worker_request(processor.cursor, processor.db_conn, worker_log_id, 'completed',
                                            {'processed': 0, 'message': 'No signatures provided'})
                        publish_response(gateway_channel, request_id, correlation_id, 'completed',
                                       {'processed': 0, 'message': 'No signatures provided'}, batch_num)
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    print(f"  Processing {len(signatures)} signatures (batch {batch_num})...")
                    result = processor.process_signatures(signatures, priority, correlation_id, sig_hash, request_log_id)

                    # Build response
                    if result.get('error'):
                        status = 'failed'
                        response_data = {'processed': 0, 'error': result['error']}
                    elif result['processed'] == 0:
                        status = 'completed'
                        response_data = {'processed': 0, 'skipped': result['skipped'], 'already_exist': True}
                    else:
                        status = 'completed'
                        response_data = {
                            'processed': result['processed'],
                            'tx_count': result['tx_count'],
                            'staging_id': result['staging_id'],
                            'skipped': result['skipped']
                        }

                    # Update worker request_log with results
                    update_worker_request(processor.cursor, processor.db_conn, worker_log_id, status, response_data)

                    publish_response(gateway_channel, request_id, correlation_id, status, response_data, batch_num)
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except MySQLError as e:
                    print(f"[ERROR] MySQL error: {e}, requeuing...")
                    db_state['conn'] = None
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                except Exception as e:
                    print(f"[ERROR] Failed to process message: {e}")
                    import traceback
                    traceback.print_exc()
                    # Try to update worker log with error
                    if worker_log_id and db_state.get('conn'):
                        try:
                            processor = db_state.get('processor')
                            if processor:
                                update_worker_request(processor.cursor, processor.db_conn,
                                                    worker_log_id, 'failed', {'error': str(e)})
                        except:
                            pass
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            gateway_channel.basic_consume(queue=RABBITMQ_REQUEST_QUEUE, on_message_callback=callback)
            gateway_channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            print(f"[ERROR] Connection lost: {e}, reconnecting in 5s...")
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n[INFO] Shutting down...")
            break

    if db_state['conn']:
        db_state['conn'].close()
    solscan_session.close()


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Guide Decoder - Fetch and stage decoded transactions')
    parser.add_argument('--prefetch', type=int, default=5, help='RabbitMQ prefetch count')
    parser.add_argument('--dry-run', action='store_true', help='Preview only, no DB changes')
    parser.add_argument('--queue-consumer', action='store_true', help='Run as gateway queue consumer')

    args = parser.parse_args()

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        return 1
    if not HAS_PIKA:
        print("Error: pika not installed")
        return 1

    if args.queue_consumer:
        return run_queue_consumer(prefetch=args.prefetch, dry_run=args.dry_run)

    print("Error: --queue-consumer is required")
    print("Usage: python guide-decoder.py --queue-consumer")
    return 1


if __name__ == '__main__':
    exit(main())
