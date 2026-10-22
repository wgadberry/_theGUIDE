#!/usr/bin/env python3
"""
Guide Detailer - Transaction detail enrichment daemon

Consumes signature batches from RabbitMQ and fetches detail data from Solscan
to populate balance change tables via sp_tx_detail_batch.

Pipeline position:
    guide-shredder.py → tx.detail.transactions queue
                             ↓
                    guide-detailer.py (this script)
                             ↓
              sp_tx_detail_batch → tx_sol_balance_change, tx_token_balance_change

Flow:
1. Consume batch of signatures from RabbitMQ queue 'tx.detail.transactions'
2. Pre-filter: skip signatures not in tx table (must exist from shredder)
3. Call Solscan /transaction/detail/multi for balance change data
4. Pass JSON to sp_tx_detail_batch (DB handles all parsing)
5. ACK message on success

Usage:
    python guide-detailer.py [--prefetch 3]
    python guide-detailer.py --max-messages 100
    python guide-detailer.py --dry-run
"""

import argparse
import sys
import json
import time
import asyncio

# HTTP client (async)
try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

# MySQL connector
try:
    import mysql.connector
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

# RabbitMQ client
try:
    import pika
    HAS_PIKA = True
except ImportError:
    HAS_PIKA = False

# =============================================================================
# Configuration
# =============================================================================

# Solscan API
SOLSCAN_API_BASE = "https://pro-api.solscan.io/v2.0"
SOLSCAN_API_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjcmVhdGVkQXQiOjE3NTg0MzQzODI5MTEsImVtYWlsIjoid2dhZGJlcnJ5QGdtYWlsLmNvbSIsImFjdGlvbiI6InRva2VuLWFwaSIsImFwaVZlcnNpb24iOiJ2MiIsImlhdCI6MTc1ODQzNDM4Mn0.d_ZYyfZGaFdLfYXsuB_M2rT_Q6WkwpxVp0ZqNE_zUAk"

# RabbitMQ (legacy - default vhost)
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5692
RABBITMQ_USER = 'admin'
RABBITMQ_PASS = 'admin123'
RABBITMQ_QUEUE_IN = 'tx.detail.transactions'

# RabbitMQ (new - t16o_mq vhost for gateway integration)
RABBITMQ_VHOST = 't16o_mq'
RABBITMQ_REQUEST_QUEUE = 'mq.guide.detailer.request'
RABBITMQ_RESPONSE_QUEUE = 'mq.guide.detailer.response'

# Database
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3396,
    'user': 'root',
    'password': 'rootpassword',
    'database': 't16o_db'
}


# =============================================================================
# Solscan API (Async)
# =============================================================================

def build_multi_url(endpoint: str, signatures: list) -> str:
    """Build URL for multi-transaction API call"""
    tx_params = '&'.join([f'tx[]={sig}' for sig in signatures])
    return f"{SOLSCAN_API_BASE}/{endpoint}?{tx_params}"


async def fetch_endpoint(session: aiohttp.ClientSession, endpoint: str, signatures: list, max_retries: int = 3) -> dict:
    """Fetch data from a Solscan endpoint asynchronously with retry"""
    url = build_multi_url(endpoint, signatures)

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


async def fetch_detail(session: aiohttp.ClientSession, signatures: list) -> dict:
    """Fetch detail data for transactions (sp_tx_detail_batch only uses detail, not decoded)"""
    return await fetch_endpoint(session, "transaction/detail/multi", signatures)


async def create_api_session() -> aiohttp.ClientSession:
    """Create a reusable aiohttp session with connection pooling (must be called in async context)"""
    headers = {"token": SOLSCAN_API_TOKEN}
    connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
    timeout = aiohttp.ClientTimeout(total=60)
    return aiohttp.ClientSession(headers=headers, connector=connector, timeout=timeout)


# =============================================================================
# Data Processing
# =============================================================================

MAX_SAFE_INT = 9223372036854775807  # 2^63 - 1

def sanitize_large_ints(obj):
    """Recursively convert integers > 64-bit to strings for JSON serialization"""
    if isinstance(obj, dict):
        return {k: sanitize_large_ints(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_large_ints(item) for item in obj]
    elif isinstance(obj, int) and (obj > MAX_SAFE_INT or obj < -MAX_SAFE_INT):
        return str(obj)
    return obj


def wrap_detail_response(detail_response: dict) -> dict:
    """Wrap detail response in expected format for sp_tx_detail_batch"""
    return {
        "detail": sanitize_large_ints(detail_response)
    }


# =============================================================================
# Pre-filter: Check for existing signatures
# =============================================================================

def filter_to_existing_signatures(cursor, signatures: list) -> list:
    """
    Filter to only signatures that exist in tx table.
    Returns only signatures that can be enriched with detail.
    """
    if not signatures:
        return []

    placeholders = ','.join(['%s'] * len(signatures))
    cursor.execute(f"""
        SELECT signature FROM tx WHERE signature IN ({placeholders})
    """, signatures)

    existing = {row[0] for row in cursor.fetchall()}

    if not existing:
        return []

    return [sig for sig in signatures if sig in existing]


# =============================================================================
# RabbitMQ Consumer
# =============================================================================

class GuideDetailerConsumer:
    """RabbitMQ consumer for detail enrichment"""

    def __init__(self, db_conn, channel,
                 max_messages: int = 0, dry_run: bool = False):
        self.db_conn = db_conn
        self.cursor = db_conn.cursor()
        self.channel = channel
        self.max_messages = max_messages
        self.dry_run = dry_run
        self.message_count = 0
        self.total_tx = 0
        self.total_details = 0
        self.should_stop = False
        self.loop = asyncio.new_event_loop()
        self._api_session = None  # Created lazily in async context

    async def get_api_session(self):
        """Get or create the API session (must be called in async context)"""
        if self._api_session is None:
            self._api_session = await create_api_session()
        return self._api_session

    async def close_api_session(self):
        """Close the API session if it exists"""
        if self._api_session is not None:
            await self._api_session.close()
            self._api_session = None

    def call_detail_batch(self, json_data: dict) -> tuple:
        """Call sp_tx_detail_batch with JSON, return (tx_count, detail_count)"""
        json_str = json.dumps(json_data)

        # Call stored procedure using session variables (callproc doesn't bind OUT params properly)
        self.cursor.execute("SET @p_tx = 0, @p_detail = 0")
        self.cursor.execute("CALL sp_tx_detail_batch(%s, @p_tx, @p_detail)", (json_str,))
        self.cursor.execute("SELECT @p_tx, @p_detail")
        result = self.cursor.fetchone()

        self.db_conn.commit()

        tx_count = result[0] or 0
        detail_count = result[1] or 0

        return tx_count, detail_count

    async def process_signatures(self, signatures: list) -> dict:
        """
        Core processing logic for a batch of signatures.
        Returns dict with: processed, skipped, tx_count, detail_count, error

        Used by both on_message() and run_queue_consumer() to avoid code duplication.
        """
        result = {
            'processed': 0,
            'skipped': 0,
            'tx_count': 0,
            'detail_count': 0,
            'error': None
        }

        if not signatures:
            return result

        # Phase 0: Pre-filter to existing signatures
        original_count = len(signatures)
        valid_sigs = filter_to_existing_signatures(self.cursor, signatures)
        result['skipped'] = original_count - len(valid_sigs)

        if result['skipped'] > 0:
            print(f"  [~] Skipped {result['skipped']}/{original_count} non-existing signatures")

        if not valid_sigs:
            print(f"  [=] No valid signatures found")
            return result

        # Phase 1: Fetch detail data from Solscan
        start_time = time.time()
        session = await self.get_api_session()
        detail_response = await fetch_detail(session, valid_sigs)
        fetch_time = time.time() - start_time

        if not detail_response.get('success'):
            result['error'] = 'Detail API returned unsuccessful response'
            return result

        # Phase 2: Wrap response for stored procedure
        combined = wrap_detail_response(detail_response)

        # Phase 3: Call stored procedure
        sp_start = time.time()
        tx_count, detail_count = self.call_detail_batch(combined)
        sp_time = time.time() - sp_start

        total_time = time.time() - start_time
        print(f"  [+] tx={tx_count} details={detail_count} "
              f"(fetch={fetch_time:.2f}s, SP={sp_time:.2f}s, total={total_time:.2f}s)")

        result['processed'] = len(valid_sigs)
        result['tx_count'] = tx_count
        result['detail_count'] = detail_count

        # Update totals
        self.total_tx += tx_count
        self.total_details += detail_count

        return result

    def on_message(self, channel, method, properties, body):
        """Handle incoming message (legacy queue mode)"""
        try:
            data = json.loads(body)
            signatures = data.get('signatures', [])

            if not signatures:
                print(f"  [!] Empty signature batch, skipping")
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return

            print(f"  [>] Processing detail batch of {len(signatures)} signatures...")

            if self.dry_run:
                print(f"      DRY RUN - would fetch detail:")
                for sig in signatures[:3]:
                    print(f"        {sig[:20]}...")
                if len(signatures) > 3:
                    print(f"        ... and {len(signatures) - 3} more")
                channel.basic_ack(delivery_tag=method.delivery_tag)
                self.message_count += 1
                return

            # Use shared processing logic
            result = self.loop.run_until_complete(self.process_signatures(signatures))

            if result.get('error'):
                print(f"  [!] Error: {result['error']}")
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                return

            channel.basic_ack(delivery_tag=method.delivery_tag)
            self.message_count += 1

            if self.max_messages > 0 and self.message_count >= self.max_messages:
                print(f"\nReached max messages ({self.max_messages})")
                self.should_stop = True
                channel.stop_consuming()

        except Exception as e:
            print(f"  [!] Error processing message: {e}")
            import traceback
            traceback.print_exc()
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def start(self):
        """Start consuming"""
        self.channel.basic_consume(
            queue=RABBITMQ_QUEUE_IN,
            on_message_callback=self.on_message
        )

        print(f"Waiting for messages on queue '{RABBITMQ_QUEUE_IN}'...")
        print("Press Ctrl+C to exit\n")

        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("\nShutting down...")
            self.channel.stop_consuming()
        finally:
            # Close API session before closing event loop
            if self._api_session is not None:
                self.loop.run_until_complete(self.close_api_session())
            self.loop.close()


# =============================================================================
# Gateway Integration (t16o_mq vhost)
# =============================================================================

from datetime import datetime

def setup_gateway_rabbitmq():
    """Setup RabbitMQ connection to t16o_mq vhost for gateway integration"""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    for queue in [RABBITMQ_REQUEST_QUEUE, RABBITMQ_RESPONSE_QUEUE]:
        channel.queue_declare(queue=queue, durable=True, arguments={'x-max-priority': 10})

    return connection, channel


def publish_response(channel, request_id: str, status: str, result: dict):
    """Publish response to gateway response queue"""
    response = {
        'request_id': request_id,
        'worker': 'detailer',
        'status': status,
        'timestamp': datetime.now().isoformat() + 'Z',
        'result': result
    }

    try:
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_RESPONSE_QUEUE,
            body=json.dumps(response).encode('utf-8'),
            properties=pika.BasicProperties(delivery_mode=2, content_type='application/json')
        )
        return True
    except Exception as e:
        print(f"[ERROR] Failed to publish response: {e}")
        return False


def run_queue_consumer(prefetch: int = 1):
    """Run as a queue consumer, listening for gateway requests.

    Uses GuideDetailerConsumer.process_signatures() for core processing logic.
    Gateway-specific publishing (response) happens after processing.
    """
    print(f"""
+-----------------------------------------------------------+
|         Guide Detailer - Queue Consumer Mode              |
|                                                           |
|  vhost:     {RABBITMQ_VHOST}                                     |
|  queue:     {RABBITMQ_REQUEST_QUEUE}             |
|  prefetch:  {prefetch}                                            |
+-----------------------------------------------------------+
""")

    # Setup DB connection with reconnect capability
    db_state = {'conn': None, 'consumer': None}

    def ensure_db_connection(gateway_channel):
        """Ensure DB connection is alive, reconnect and recreate consumer if needed"""
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
                db_state['consumer'] = GuideDetailerConsumer(
                    db_conn=db_state['conn'],
                    channel=gateway_channel
                )
                print("[OK] Database (re)connected")
            return db_state['consumer']
        except Exception as e:
            print(f"[WARN] Database connection failed: {e}")
            db_state['conn'] = None
            db_state['consumer'] = None
            return None

    while True:
        try:
            gateway_conn, gateway_channel = setup_gateway_rabbitmq()
            gateway_channel.basic_qos(prefetch_count=prefetch)

            # Initial connection and consumer creation
            ensure_db_connection(gateway_channel)

            print(f"[OK] Connected to {RABBITMQ_REQUEST_QUEUE}")
            print("[INFO] Waiting for requests...")

            def callback(ch, method, properties, body):
                nonlocal gateway_channel
                try:
                    message = json.loads(body.decode('utf-8'))
                    request_id = message.get('request_id', 'unknown')
                    batch_data = message.get('batch', {})

                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Received request {request_id[:8]}")

                    # Ensure DB connection is alive
                    consumer = ensure_db_connection(gateway_channel)
                    if not consumer:
                        raise Exception("Database connection unavailable")

                    signatures = batch_data.get('signatures', [])
                    if not signatures:
                        print(f"  [SKIP] No signatures in batch (keys: {list(batch_data.keys())})")
                        publish_response(gateway_channel, request_id, 'completed',
                                       {'processed': 0, 'message': 'No signatures provided'})
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    print(f"  Processing {len(signatures)} signatures...")

                    # === USE PROVEN CLASS METHOD FOR CORE PROCESSING ===
                    result = consumer.loop.run_until_complete(
                        consumer.process_signatures(signatures)
                    )

                    # Build response from result
                    if result.get('error'):
                        status = 'failed'
                        response_data = {'processed': 0, 'error': result['error']}
                    elif result['processed'] == 0:
                        status = 'completed'
                        response_data = {
                            'processed': 0,
                            'skipped': result['skipped'],
                            'message': 'No matching tx found'
                        }
                    else:
                        status = 'completed'
                        response_data = {
                            'processed': result['processed'],
                            'tx_count': result['tx_count'],
                            'detail_count': result['detail_count']
                        }

                    publish_response(gateway_channel, request_id, status, response_data)
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    print(f"[ERROR] Failed to process message: {e}")
                    import traceback
                    traceback.print_exc()
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            gateway_channel.basic_consume(queue=RABBITMQ_REQUEST_QUEUE, on_message_callback=callback)
            gateway_channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            print(f"[ERROR] Connection lost: {e}")
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n[INFO] Shutting down...")
            # Cleanup consumer resources
            if 'consumer' in dir():
                consumer.loop.run_until_complete(consumer.close_api_session())
                consumer.loop.close()
            break
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")
            time.sleep(5)

    db_conn.close()


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Guide Detailer - Transaction detail enrichment')
    parser.add_argument('--max-messages', type=int, default=0,
                        help='Maximum messages to process (0 = unlimited)')
    parser.add_argument('--prefetch', type=int, default=3,
                        help='Prefetch count for RabbitMQ (lower for detail since API is slower)')
    parser.add_argument('--db-host', default='localhost', help='MySQL host')
    parser.add_argument('--db-port', type=int, default=3396, help='MySQL port')
    parser.add_argument('--db-user', default='root', help='MySQL user')
    parser.add_argument('--db-pass', default='rootpassword', help='MySQL password')
    parser.add_argument('--db-name', default='t16o_db', help='MySQL database')
    parser.add_argument('--rabbitmq-host', default=RABBITMQ_HOST, help='RabbitMQ host')
    parser.add_argument('--rabbitmq-port', type=int, default=RABBITMQ_PORT, help='RabbitMQ port')
    parser.add_argument('--rabbitmq-user', default=RABBITMQ_USER, help='RabbitMQ user')
    parser.add_argument('--rabbitmq-pass', default=RABBITMQ_PASS, help='RabbitMQ password')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be processed without making changes')
    parser.add_argument('--queue-consumer', action='store_true',
                        help='Run as queue consumer, listening for gateway requests')

    args = parser.parse_args()

    if args.queue_consumer:
        return run_queue_consumer(prefetch=args.prefetch)

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        return 1

    if not HAS_PIKA:
        print("Error: pika not installed")
        return 1

    if not HAS_AIOHTTP:
        print("Error: aiohttp not installed")
        print("Install with: pip install aiohttp")
        return 1

    print(f"Guide Detailer (Balance change enrichment)")
    print(f"{'='*60}")
    print(f"RabbitMQ: {args.rabbitmq_host}:{args.rabbitmq_port}")
    print(f"Queue In:  {RABBITMQ_QUEUE_IN}")
    print(f"Prefetch: {args.prefetch}")
    print(f"Max messages: {args.max_messages if args.max_messages > 0 else 'unlimited'}")
    if args.dry_run:
        print(f"MODE: DRY RUN")
    print()

    # Connect to MySQL
    print(f"Connecting to MySQL {args.db_host}:{args.db_port}/{args.db_name}...")
    db_conn = mysql.connector.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_pass,
        database=args.db_name
    )

    # Connect to RabbitMQ
    print(f"Connecting to RabbitMQ...")
    credentials = pika.PlainCredentials(args.rabbitmq_user, args.rabbitmq_pass)
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
    channel.basic_qos(prefetch_count=args.prefetch)

    # Start consumer (API session created lazily in async context)
    consumer = GuideDetailerConsumer(
        db_conn, channel,
        args.max_messages, args.dry_run
    )

    try:
        consumer.start()
    finally:
        connection.close()
        db_conn.close()

    print(f"\n{'='*60}")
    print(f"Done! Processed {consumer.message_count} messages")
    print(f"  Total transactions: {consumer.total_tx}")
    print(f"  Total details: {consumer.total_details}")
    print(f"{'='*60}")

    return 0


if __name__ == '__main__':
    exit(main())
