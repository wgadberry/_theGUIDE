#!/usr/bin/env python3
"""
Guide Detailer - Fetches transaction detail data and stages for shredding

Consumes signature batches from gateway, fetches detail data from Solscan,
and inserts into staging table with tx_state=16 (detailed).

Usage:
    python guide-detailer.py --queue-consumer          # Gateway mode (primary)
    python guide-detailer.py --queue-consumer --dry-run
"""

import argparse
import json
import os
import time
import asyncio
from datetime import datetime

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

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
RABBITMQ_REQUEST_QUEUE = 'mq.guide.detailer.request'
RABBITMQ_RESPONSE_QUEUE = 'mq.guide.detailer.response'

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
TX_STATE_DETAILED = 16  # Will be fetched from config table


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
# Solscan API (Async)
# =============================================================================

async def fetch_detail(session: aiohttp.ClientSession, signatures: list, max_retries: int = 3) -> dict:
    """Fetch detail data for transactions with retry"""
    tx_params = '&'.join([f'tx[]={sig}' for sig in signatures])
    url = f"{SOLSCAN_API_BASE}/transaction/detail/multi?{tx_params}"

    for attempt in range(max_retries):
        try:
            async with session.get(url) as response:
                if response.status in (502, 503, 504):
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 5
                        print(f"    API {response.status}, retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                        continue
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                print(f"    API error ({e}), retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
            else:
                raise

    raise Exception(f"Failed after {max_retries} retries")


async def create_api_session() -> aiohttp.ClientSession:
    """Create a reusable aiohttp session"""
    headers = {"token": SOLSCAN_API_TOKEN}
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
    timeout = aiohttp.ClientTimeout(total=60)
    return aiohttp.ClientSession(headers=headers, connector=connector, timeout=timeout)


# =============================================================================
# Data Processing
# =============================================================================

MAX_SAFE_INT = 9223372036854775807

def sanitize_large_ints(obj):
    """Recursively convert integers > 64-bit to strings"""
    if isinstance(obj, dict):
        return {k: sanitize_large_ints(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_large_ints(item) for item in obj]
    elif isinstance(obj, int) and (obj > MAX_SAFE_INT or obj < -MAX_SAFE_INT):
        return str(obj)
    return obj


# =============================================================================
# Core Processing
# =============================================================================

class DetailerProcessor:
    """Core detailer processing logic - fetches and stages detail data"""

    def __init__(self, db_conn, dry_run: bool = False):
        self.db_conn = db_conn
        self.cursor = db_conn.cursor()
        self.dry_run = dry_run
        self.loop = asyncio.new_event_loop()
        self._api_session = None
        self._tx_state_detailed = None

    async def get_api_session(self):
        if self._api_session is None:
            self._api_session = await create_api_session()
        return self._api_session

    async def close_api_session(self):
        if self._api_session is not None:
            await self._api_session.close()
            self._api_session = None

    def get_tx_state_detailed(self) -> int:
        """Get detailed tx_state value from config table"""
        if self._tx_state_detailed is None:
            self.cursor.execute(
                "SELECT CAST(config_value AS UNSIGNED) FROM config "
                "WHERE config_type = 'tx_state' AND config_key = 'detailed'"
            )
            row = self.cursor.fetchone()
            self._tx_state_detailed = row[0] if row else TX_STATE_DETAILED
        return self._tx_state_detailed

    def insert_staging(self, detail_response: dict, priority: int = 5, correlation_id: str = None,
                        sig_hash: str = None, request_log_id: int = None) -> int:
        """Insert detail JSON into staging table, return staging_id"""
        tx_state = self.get_tx_state_detailed()

        # Sanitize large integers and wrap response
        sanitized = sanitize_large_ints(detail_response)

        self.cursor.execute(f"""
            INSERT INTO {STAGING_SCHEMA}.{STAGING_TABLE} (txs, tx_state, priority, correlation_id, sig_hash, request_log_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (json.dumps(sanitized), tx_state, priority, correlation_id, sig_hash, request_log_id))

        staging_id = self.cursor.lastrowid
        self.db_conn.commit()
        return staging_id

    async def process_signatures(self, signatures: list, priority: int = 5, correlation_id: str = None,
                                  sig_hash: str = None, request_log_id: int = None) -> dict:
        """Core processing logic for a batch of signatures"""
        result = {
            'processed': 0,
            'skipped': 0,
            'staging_id': None,
            'tx_count': 0,
            'error': None
        }

        if not signatures:
            return result

        if self.dry_run:
            print(f"  [DRY] Would detail {len(signatures)} signatures")
            result['processed'] = len(signatures)
            return result

        # Phase 1: Fetch detail data from Solscan
        start_time = time.time()
        try:
            session = await self.get_api_session()
            detail_response = await fetch_detail(session, signatures)
        except Exception as e:
            result['error'] = f'Solscan API error: {e}'
            return result
        fetch_time = time.time() - start_time

        if not detail_response.get('success'):
            result['error'] = 'Detail API returned unsuccessful response'
            return result

        # Get actual transactions returned
        tx_data = detail_response.get('data', [])
        result['tx_count'] = len(tx_data)

        if not tx_data:
            result['error'] = 'Detail API returned no data'
            return result

        if len(tx_data) < len(signatures):
            print(f"  [~] Solscan returned {len(tx_data)}/{len(signatures)} transactions")

        # Phase 2: Insert into staging table
        staging_start = time.time()
        staging_id = self.insert_staging(detail_response, priority, correlation_id, sig_hash, request_log_id)
        staging_time = time.time() - staging_start

        result['staging_id'] = staging_id
        result['processed'] = len(tx_data)

        print(f"  [+] Staged {len(tx_data)} txs → staging.id={staging_id} "
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


def publish_response(channel, request_id: str, correlation_id: str, status: str, result: dict, batch_num: int = 0):
    """Publish response to detailer response queue"""
    # Include batch_num in result for gateway tracking
    result['batch_num'] = batch_num

    response = {
        'request_id': request_id,
        'correlation_id': correlation_id,
        'worker': 'detailer',
        'status': status,
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'result': result
    }

    channel.basic_publish(
        exchange='', routing_key=RABBITMQ_RESPONSE_QUEUE,
        body=json.dumps(response).encode('utf-8'),
        properties=pika.BasicProperties(delivery_mode=2, content_type='application/json')
    )


def run_queue_consumer(prefetch: int = 1, dry_run: bool = False):
    """Run as a queue consumer, listening for gateway requests"""
    print(f"""
+-----------------------------------------------------------+
|         Guide Detailer - Queue Consumer Mode              |
|  vhost: {RABBITMQ_VHOST}  |  queue: {RABBITMQ_REQUEST_QUEUE}  |
+-----------------------------------------------------------+
""")
    if dry_run:
        print("MODE: DRY RUN")

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
                db_state['processor'] = DetailerProcessor(db_state['conn'], dry_run)
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
                    sig_hash = message.get('sig_hash')  # For pairing with decoder
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
                        request_id, correlation_id, 'detailer', 'detail',
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
                    result = processor.loop.run_until_complete(
                        processor.process_signatures(signatures, priority, correlation_id, sig_hash, request_log_id)
                    )

                    # Build response
                    if result.get('error'):
                        status = 'failed'
                        response_data = {'processed': 0, 'error': result['error']}
                    elif result['processed'] == 0:
                        status = 'completed'
                        response_data = {'processed': 0, 'skipped': result['skipped'], 'message': 'No data returned'}
                    else:
                        status = 'completed'
                        response_data = {
                            'processed': result['processed'],
                            'tx_count': result['tx_count'],
                            'staging_id': result['staging_id']
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
            if db_state['processor']:
                db_state['processor'].loop.run_until_complete(db_state['processor'].close_api_session())
                db_state['processor'].loop.close()
            break

    if db_state['conn']:
        db_state['conn'].close()


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Guide Detailer - Fetch and stage transaction details')
    parser.add_argument('--prefetch', type=int, default=3, help='RabbitMQ prefetch count')
    parser.add_argument('--dry-run', action='store_true', help='Preview only, no DB changes')
    parser.add_argument('--queue-consumer', action='store_true', help='Run as gateway queue consumer')

    args = parser.parse_args()

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        return 1
    if not HAS_PIKA:
        print("Error: pika not installed")
        return 1
    if not HAS_AIOHTTP:
        print("Error: aiohttp not installed (pip install aiohttp)")
        return 1

    if args.queue_consumer:
        return run_queue_consumer(prefetch=args.prefetch, dry_run=args.dry_run)

    print("Error: --queue-consumer is required")
    print("Usage: python guide-detailer.py --queue-consumer")
    return 1


if __name__ == '__main__':
    exit(main())
