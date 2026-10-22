#!/usr/bin/env python3
"""
Guide Shredder - Core transaction shredding daemon

Consumes signature batches from RabbitMQ, fetches decoded data from Solscan,
sends addresses to funding queue, and calls sp_tx_shred_batch for DB processing.

Pipeline position:
    guide-producer.py → tx.guide.signatures queue
                             ↓
                    guide-shredder.py (this script)
                             ↓
              sp_tx_shred_batch → tx_guide, tx_address, tx_token
                             ↓
                    tx.guide.addresses queue → guide-funder.py

Flow:
1. Consume batch of signatures from RabbitMQ queue 'tx.guide.signatures'
2. Pre-filter: skip signatures already in tx table (save API calls)
3. Call Solscan /transaction/actions/multi (only new signatures)
4. Extract distinct addresses -> send to 'tx.guide.addresses' queue
5. Pass ENTIRE JSON to sp_tx_shred_batch (DB handles all parsing/normalization)
6. ACK message on success

Usage:
    python guide-shredder.py [--prefetch 5]
    python guide-shredder.py --max-messages 100
    python guide-shredder.py --no-funding  # Skip address queue
    python guide-shredder.py --dry-run
"""

import argparse
import sys
import json
import time

# HTTP client
import requests

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
RABBITMQ_QUEUE_IN = 'tx.guide.signatures'
RABBITMQ_QUEUE_OUT = 'tx.guide.addresses'
RABBITMQ_QUEUE_DETAIL = 'tx.detail.transactions'

# RabbitMQ (new - t16o_mq vhost for gateway integration)
RABBITMQ_VHOST = 't16o_mq'
RABBITMQ_REQUEST_QUEUE = 'mq.guide.shredder.request'
RABBITMQ_RESPONSE_QUEUE = 'mq.guide.shredder.response'
RABBITMQ_GATEWAY_QUEUE = 'mq.guide.gateway.request'

# Database
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3396,
    'user': 'root',
    'password': 'rootpassword',
    'database': 't16o_db'
}


# =============================================================================
# Solscan API
# =============================================================================

def build_multi_url(signatures: list) -> str:
    """Build URL for multi-transaction decoded API call"""
    tx_params = '&'.join([f'tx[]={sig}' for sig in signatures])
    return f"{SOLSCAN_API_BASE}/transaction/actions/multi?{tx_params}"


def fetch_decoded_batch(session: requests.Session, signatures: list) -> dict:
    """Fetch decoded data for batch of signatures"""
    url = build_multi_url(signatures)
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def create_solscan_session() -> requests.Session:
    """Create requests session with Solscan auth"""
    session = requests.Session()
    session.headers.update({"token": SOLSCAN_API_TOKEN})
    return session


# =============================================================================
# Pre-filter: Check for existing signatures before Solscan call
# =============================================================================

def filter_existing_signatures(cursor, signatures: list) -> tuple:
    """
    Filter signatures based on existence and detail status.
    Returns tuple: (new_signatures, need_detail_signatures)
      - new_signatures: don't exist in tx table, need full shredding
      - need_detail_signatures: exist but tx_state missing DETAILED bit (64)
    """
    if not signatures:
        return [], []

    # tx_state is now a bitmask: bit 6 (64) = DETAILED
    DETAILED_BIT = 64

    placeholders = ','.join(['%s'] * len(signatures))
    cursor.execute(f"""
        SELECT signature, tx_state FROM tx WHERE signature IN ({placeholders})
    """, signatures)

    existing = {}
    for row in cursor.fetchall():
        # Handle both old VARCHAR ('shredded', 'detailed') and new BIGINT bitmask
        state_val = row[1]
        if state_val is None:
            existing[row[0]] = 0
        elif isinstance(state_val, int):
            existing[row[0]] = state_val
        elif state_val == 'detailed':
            existing[row[0]] = 64 | 63  # DETAILED + SHREDDED bits
        elif state_val == 'shredded':
            existing[row[0]] = 63  # SHREDDED bits only
        elif state_val.isdigit():
            existing[row[0]] = int(state_val)
        else:
            existing[row[0]] = 0  # Unknown state, treat as new

    if not existing:
        return signatures, []

    new_sigs = [sig for sig in signatures if sig not in existing]
    # Need detail if DETAILED bit (64) is not set
    need_detail = [sig for sig, state in existing.items() if (state & DETAILED_BIT) == 0]

    return new_sigs, need_detail


# =============================================================================
# Signer Extraction
# =============================================================================

def extract_signers_from_json(data: dict) -> dict:
    """
    Extract signers for each transaction from Solscan response.
    Returns dict: {signature: [signer_addresses]}
    """
    tx_signers = {}

    if not data.get('success') or not data.get('data'):
        return tx_signers

    for tx in data['data']:
        sig = tx.get('tx_hash') or tx.get('signature')
        if not sig:
            continue

        signers = []
        # Solscan includes signers in the transaction data
        if 'signers' in tx:
            signers = tx['signers'] if isinstance(tx['signers'], list) else [tx['signers']]
        # Fallback: try to get from fee_payer or first signer
        elif 'fee_payer' in tx:
            signers = [tx['fee_payer']]
        # Fallback: look in parsed data
        elif 'signer' in tx:
            signers = [tx['signer']] if isinstance(tx['signer'], str) else tx['signer']

        if signers:
            tx_signers[sig] = signers

    return tx_signers


# =============================================================================
# Address Extraction (Python-side for queue, before SP call)
# =============================================================================

def extract_addresses_from_json(data: dict) -> set:
    """
    Extract only wallet addresses (source/destination owners) from transfers.

    These are the actual wallets that need funding lookup.
    We no longer extract:
    - Token mint addresses (they're programs, not fundable wallets)
    - Activity account addresses (ATAs, vaults, pools - not fundable)
    - Swap token_1/token_2 (mint addresses, not wallets)

    This reduces addresses by ~60-80% per batch.
    """
    addresses = set()

    if not data.get('success') or not data.get('data'):
        return addresses

    for tx in data['data']:
        # Only extract wallet owners from transfers
        for transfer in tx.get('transfers', []):
            if so := transfer.get('source_owner'):
                addresses.add(so)
            if do := transfer.get('destination_owner'):
                addresses.add(do)

    return addresses


# =============================================================================
# RabbitMQ Consumer
# =============================================================================

class GuideConsumerV2:
    """RabbitMQ consumer for signature batches - v2 with SP processing"""

    def __init__(self, db_conn, channel, solscan_session: requests.Session,
                 max_messages: int = 0, dry_run: bool = False, no_funding: bool = False,
                 no_detail: bool = False):
        self.db_conn = db_conn
        self.cursor = db_conn.cursor()
        self.channel = channel
        self.solscan_session = solscan_session
        self.max_messages = max_messages
        self.dry_run = dry_run
        self.no_funding = no_funding
        self.no_detail = no_detail
        self.message_count = 0
        self.total_tx = 0
        self.total_edges = 0
        self.total_addresses = 0
        self.total_transfers = 0
        self.total_swaps = 0
        self.total_activities = 0
        self.total_signers = 0
        self.total_instructions = 0
        self.total_detail_queued = 0
        self.should_stop = False

        # Declare output queue for addresses
        if not no_funding:
            channel.queue_declare(
                queue=RABBITMQ_QUEUE_OUT,
                durable=True,
                arguments={'x-max-priority': 10}
            )

        # Declare output queue for detail enrichment
        if not no_detail:
            channel.queue_declare(
                queue=RABBITMQ_QUEUE_DETAIL,
                durable=True,
                arguments={'x-max-priority': 10}
            )

    def publish_addresses(self, addresses: set):
        """Publish addresses to funding queue"""
        if not addresses or self.no_funding:
            return 0

        # Send as JSON array
        body = json.dumps(list(addresses))
        self.channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE_OUT,
            body=body.encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent
                priority=5
            )
        )
        return len(addresses)

    def publish_to_detail_queue(self, signatures: list):
        """Publish signatures to detail enrichment queue"""
        if not signatures or self.no_detail:
            return 0

        # Send as JSON object with signatures array
        body = json.dumps({'signatures': signatures})
        self.channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE_DETAIL,
            body=body.encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent
                priority=5
            )
        )
        return len(signatures)

    def store_signers(self, tx_signers: dict):
        """
        Store signers in tx_signer table.
        tx_signers: {signature: [signer_addresses]}
        """
        if not tx_signers:
            return 0

        stored = 0
        for sig, signers in tx_signers.items():
            # Get tx_id
            self.cursor.execute("SELECT id FROM tx WHERE signature = %s", (sig,))
            row = self.cursor.fetchone()
            if not row:
                continue
            tx_id = row[0]

            for idx, signer_addr in enumerate(signers):
                # Ensure signer address exists
                self.cursor.execute("""
                    INSERT IGNORE INTO tx_address (address, address_type)
                    VALUES (%s, 'wallet')
                """, (signer_addr,))

                # Get signer address_id
                self.cursor.execute("SELECT id FROM tx_address WHERE address = %s", (signer_addr,))
                addr_row = self.cursor.fetchone()
                if not addr_row:
                    continue
                signer_id = addr_row[0]

                # Insert into tx_signer
                self.cursor.execute("""
                    INSERT IGNORE INTO tx_signer (tx_id, signer_address_id, signer_index)
                    VALUES (%s, %s, %s)
                """, (tx_id, signer_id, idx))
                stored += 1

        self.db_conn.commit()
        return stored

    def store_instruction_data(self, decoded_response: dict):
        """
        Compress and store raw instruction data in tx.instruction_data.
        Uses MySQL COMPRESS() for storage efficiency.
        """
        if not decoded_response.get('success') or not decoded_response.get('data'):
            return 0

        stored = 0
        for tx in decoded_response['data']:
            sig = tx.get('tx_hash') or tx.get('signature')
            if not sig:
                continue

            # Extract the instruction-relevant data (activities, transfers)
            instruction_payload = {
                'activities': tx.get('activities', []),
                'transfers': tx.get('transfers', []),
                'block_time': tx.get('block_time'),
            }

            # Compress JSON and store
            json_str = json.dumps(instruction_payload, separators=(',', ':'))

            self.cursor.execute("""
                UPDATE tx SET instruction_data = COMPRESS(%s)
                WHERE signature = %s AND instruction_data IS NULL
            """, (json_str, sig))
            if self.cursor.rowcount > 0:
                stored += 1

        self.db_conn.commit()
        return stored

    def call_shred_batch(self, json_data: dict) -> tuple:
        """Call sp_tx_shred_batch with JSON, return (tx_count, edge_count, address_count, transfer_count, swap_count, activity_count)"""
        # Convert to JSON string
        json_str = json.dumps(json_data)

        # Call stored procedure using session variables (callproc doesn't bind OUT params properly)
        self.cursor.execute("SET @p_tx = 0, @p_edge = 0, @p_addr = 0, @p_xfer = 0, @p_swap = 0, @p_act = 0")
        self.cursor.execute("CALL sp_tx_shred_batch(%s, @p_tx, @p_edge, @p_addr, @p_xfer, @p_swap, @p_act)", (json_str,))
        self.cursor.execute("SELECT @p_tx, @p_edge, @p_addr, @p_xfer, @p_swap, @p_act")
        result = self.cursor.fetchone()

        self.db_conn.commit()

        tx_count = result[0] or 0
        edge_count = result[1] or 0
        address_count = result[2] or 0
        transfer_count = result[3] or 0
        swap_count = result[4] or 0
        activity_count = result[5] or 0

        return tx_count, edge_count, address_count, transfer_count, swap_count, activity_count

    def process_signatures(self, signatures: list, skip_legacy_queues: bool = False) -> dict:
        """
        Core processing logic for a batch of signatures.
        Returns dict with: processed, skipped, need_detail_sigs, addresses, funder_addresses, error

        Used by both on_message() and run_queue_consumer() to avoid code duplication.
        """
        result = {
            'processed': 0,
            'skipped': 0,
            'need_detail_sigs': [],
            'new_sigs': [],
            'addresses': set(),
            'funder_addresses': [],
            'tx_count': 0,
            'edge_count': 0,
            'transfer_count': 0,
            'swap_count': 0,
            'activity_count': 0,
            'signers_stored': 0,
            'instructions_stored': 0,
            'error': None
        }

        if not signatures:
            return result

        # Phase 0: Pre-filter existing signatures
        original_count = len(signatures)
        new_sigs, need_detail_sigs = filter_existing_signatures(self.cursor, signatures)
        result['skipped'] = original_count - len(new_sigs) - len(need_detail_sigs)
        result['need_detail_sigs'] = need_detail_sigs
        result['new_sigs'] = new_sigs

        # Queue existing signatures that need detail enrichment (legacy mode only)
        if need_detail_sigs and not self.no_detail and not skip_legacy_queues:
            detail_backfill = self.publish_to_detail_queue(need_detail_sigs)
            self.total_detail_queued += detail_backfill
            print(f"  [>] Queued {detail_backfill} existing sigs for detail enrichment")

        if result['skipped'] > 0:
            print(f"  [~] Skipped {result['skipped']}/{original_count} already detailed signatures")

        if not new_sigs:
            print(f"  [=] No new signatures to shred")
            return result

        # Phase 1: Fetch decoded data from Solscan
        start_time = time.time()
        decoded_response = fetch_decoded_batch(self.solscan_session, new_sigs)
        fetch_time = time.time() - start_time

        if not decoded_response.get('success') or not decoded_response.get('data'):
            result['error'] = 'Solscan API returned no data'
            return result

        # Phase 2: Extract addresses
        addresses = extract_addresses_from_json(decoded_response)
        result['addresses'] = addresses

        # Publish to legacy funding queue (legacy mode only)
        if not self.no_funding and not skip_legacy_queues:
            addr_queued = self.publish_addresses(addresses)
            if addr_queued > 0:
                print(f"  [>] Queued {addr_queued} addresses for funding")

        # Phase 3: Call stored procedure
        sp_start = time.time()
        tx_count, edge_count, address_count, transfer_count, swap_count, activity_count = self.call_shred_batch(decoded_response)
        sp_time = time.time() - sp_start

        result['tx_count'] = tx_count
        result['edge_count'] = edge_count
        result['transfer_count'] = transfer_count
        result['swap_count'] = swap_count
        result['activity_count'] = activity_count
        result['processed'] = len(new_sigs)

        # Phase 3b: Store signers and instruction data
        tx_signers = extract_signers_from_json(decoded_response)
        signers_stored = self.store_signers(tx_signers)
        instructions_stored = self.store_instruction_data(decoded_response)
        result['signers_stored'] = signers_stored
        result['instructions_stored'] = instructions_stored

        total_time = time.time() - start_time
        print(f"  [+] tx={tx_count} edges={edge_count} xfers={transfer_count} swaps={swap_count} acts={activity_count} addrs={address_count} "
              f"signers={signers_stored} instrs={instructions_stored} "
              f"(fetch={fetch_time:.2f}s, SP={sp_time:.2f}s, total={total_time:.2f}s)")

        # Phase 4: Queue signatures for detail enrichment (legacy mode only)
        if not self.no_detail and not skip_legacy_queues:
            detail_queued = self.publish_to_detail_queue(new_sigs)
            self.total_detail_queued += detail_queued
            if detail_queued > 0:
                print(f"  [>] Queued {detail_queued} signatures for detail enrichment")

        # Get addresses needing funding lookup (for gateway cascade)
        if addresses:
            placeholders = ','.join(['%s'] * len(addresses))
            self.cursor.execute(f"""
                SELECT address FROM tx_address
                WHERE address IN ({placeholders})
                AND (init_tx_fetched = 0 OR init_tx_fetched IS NULL)
                AND address_type IN ('wallet', 'unknown')
            """, list(addresses))
            result['funder_addresses'] = [row[0] for row in self.cursor.fetchall()]
            if result['funder_addresses']:
                print(f"  [>] Found {len(result['funder_addresses'])} addresses needing funding lookup")

        # Update totals
        self.total_tx += tx_count
        self.total_edges += edge_count
        self.total_addresses += address_count
        self.total_transfers += transfer_count
        self.total_swaps += swap_count
        self.total_activities += activity_count
        self.total_signers += signers_stored
        self.total_instructions += instructions_stored

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

            print(f"  [>] Processing batch of {len(signatures)} signatures...")

            if self.dry_run:
                print(f"      DRY RUN - would fetch and process:")
                for sig in signatures[:3]:
                    print(f"        {sig[:20]}...")
                if len(signatures) > 3:
                    print(f"        ... and {len(signatures) - 3} more")
                channel.basic_ack(delivery_tag=method.delivery_tag)
                self.message_count += 1
                return

            # Use shared processing logic (publishes to legacy queues)
            result = self.process_signatures(signatures, skip_legacy_queues=False)

            if result.get('error'):
                print(f"  [!] Error: {result['error']}")
                # Still ACK - no point retrying if Solscan returned nothing
                channel.basic_ack(delivery_tag=method.delivery_tag)
                self.message_count += 1
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

    # Declare queues
    for queue in [RABBITMQ_REQUEST_QUEUE, RABBITMQ_RESPONSE_QUEUE, RABBITMQ_GATEWAY_QUEUE]:
        channel.queue_declare(queue=queue, durable=True, arguments={'x-max-priority': 10})

    return connection, channel


def publish_response(channel, request_id: str, status: str, result: dict, error: str = None):
    """Publish response to gateway response queue"""
    response = {
        'request_id': request_id,
        'worker': 'shredder',
        'status': status,
        'timestamp': datetime.now().isoformat() + 'Z',
        'result': result
    }
    if error:
        response['error'] = error

    try:
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_RESPONSE_QUEUE,
            body=json.dumps(response).encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='application/json'
            )
        )
        return True
    except Exception as e:
        print(f"[ERROR] Failed to publish response: {e}")
        return False


def publish_cascade(channel, request_id: str, correlation_id: str, targets: list,
                    batch: dict, api_key: str = 'internal_cascade_key'):
    """Publish cascade message to gateway for downstream workers"""
    message = {
        'request_id': f"cascade-{request_id[:8]}-{datetime.now().strftime('%H%M%S')}",
        'correlation_id': correlation_id,  # Track original REST request
        'source_worker': 'shredder',
        'source_request_id': request_id,
        'api_key': api_key,
        'action': 'cascade',
        'targets': targets,
        'batch': batch
    }

    try:
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_GATEWAY_QUEUE,
            body=json.dumps(message).encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='application/json',
                priority=5
            )
        )
        print(f"  [CASCADE] Sent to gateway for {targets}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to publish cascade: {e}")
        return False


def run_queue_consumer(prefetch: int = 1, no_funding: bool = False, no_detail: bool = False):
    """Run as a queue consumer, listening for gateway requests.

    Uses GuideConsumerV2.process_signatures() for core processing logic.
    Gateway-specific publishing (response + cascade) happens after processing.
    """
    print(f"""
+-----------------------------------------------------------+
|         Guide Shredder - Queue Consumer Mode              |
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
                # Recreate consumer with new connection
                db_state['consumer'] = GuideConsumerV2(
                    db_conn=db_state['conn'],
                    channel=gateway_channel,
                    solscan_session=solscan_session,
                    no_funding=True,
                    no_detail=True
                )
                print("[OK] Database (re)connected")
            return db_state['consumer']
        except Exception as e:
            print(f"[WARN] Database connection failed: {e}")
            db_state['conn'] = None
            db_state['consumer'] = None
            return None

    # Setup Solscan session
    solscan_session = create_solscan_session()

    while True:
        try:
            # Setup gateway connection (for response/cascade publishing)
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
                    correlation_id = message.get('correlation_id', request_id)
                    batch_data = message.get('batch', {})
                    api_key = message.get('api_key', 'internal_cascade_key')

                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Received request {request_id[:8]} (correlation: {correlation_id[:8]})")

                    # Ensure DB connection is alive
                    consumer = ensure_db_connection(gateway_channel)
                    if not consumer:
                        raise Exception("Database connection unavailable")

                    # Extract signatures from batch
                    signatures = batch_data.get('signatures', [])
                    if not signatures:
                        print(f"  No signatures in batch, skipping")
                        publish_response(gateway_channel, request_id, 'completed',
                                       {'processed': 0, 'message': 'No signatures provided'})
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        return

                    print(f"  Processing {len(signatures)} signatures...")

                    # === USE PROVEN CLASS METHOD FOR CORE PROCESSING ===
                    # skip_legacy_queues=True because we use gateway cascades instead
                    result = consumer.process_signatures(signatures, skip_legacy_queues=True)

                    # Build response from result
                    if result.get('error'):
                        status = 'failed'
                        response_data = {'processed': 0, 'error': result['error']}
                    elif result['processed'] == 0:
                        status = 'completed'
                        response_data = {
                            'processed': 0,
                            'skipped': result['skipped'],
                            'already_exist': True
                        }
                    else:
                        status = 'completed'
                        response_data = {
                            'processed': result['processed'],
                            'transactions': result['tx_count'],
                            'addresses': len(result['addresses']),
                            'funder_addresses': result['funder_addresses']
                            # NOTE: Don't include 'cascade_to' - shredder handles cascading
                            # directly via publish_cascade() with proper signatures
                        }

                    # Publish response to gateway
                    publish_response(gateway_channel, request_id, status, response_data)

                    # Send cascade if we processed transactions or have sigs needing detail
                    sigs_for_detail = result['new_sigs'] + result['need_detail_sigs']
                    if sigs_for_detail and (result['processed'] > 0 or result['need_detail_sigs']):
                        cascade_batch = {
                            'signatures': sigs_for_detail,
                            'source_signatures': signatures[:5],  # Sample for reference
                            'processed_count': result['processed'],
                            'address_count': len(result['addresses']),
                            'funder_addresses': result['funder_addresses']
                        }
                        publish_cascade(gateway_channel, request_id, correlation_id,
                                      ['detailer', 'funder', 'aggregator'], cascade_batch, api_key)

                    # Ack the message
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    print(f"[ERROR] Failed to process message: {e}")
                    import traceback
                    traceback.print_exc()
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

            gateway_channel.basic_consume(
                queue=RABBITMQ_REQUEST_QUEUE,
                on_message_callback=callback
            )
            gateway_channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e:
            print(f"[ERROR] Connection lost: {e}")
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

    # Cleanup
    if db_conn:
        db_conn.close()
    solscan_session.close()


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Guide Shredder Consumer v2 - SP-based processing')
    parser.add_argument('--max-messages', type=int, default=0,
                        help='Maximum messages to process (0 = unlimited)')
    parser.add_argument('--prefetch', type=int, default=5,
                        help='Prefetch count for RabbitMQ')
    parser.add_argument('--db-host', default='localhost', help='MySQL host')
    parser.add_argument('--db-port', type=int, default=3396, help='MySQL port')
    parser.add_argument('--db-user', default='root', help='MySQL user')
    parser.add_argument('--db-pass', default='rootpassword', help='MySQL password')
    parser.add_argument('--db-name', default='t16o_db', help='MySQL database')
    parser.add_argument('--rabbitmq-host', default=RABBITMQ_HOST, help='RabbitMQ host')
    parser.add_argument('--rabbitmq-port', type=int, default=RABBITMQ_PORT, help='RabbitMQ port')
    parser.add_argument('--rabbitmq-user', default=RABBITMQ_USER, help='RabbitMQ user')
    parser.add_argument('--rabbitmq-pass', default=RABBITMQ_PASS, help='RabbitMQ password')
    parser.add_argument('--no-funding', action='store_true',
                        help='Skip sending addresses to funding queue')
    parser.add_argument('--no-detail', action='store_true',
                        help='Skip sending signatures to detail enrichment queue')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be processed without making changes')
    parser.add_argument('--queue-consumer', action='store_true',
                        help='Run as queue consumer, listening for gateway requests')

    args = parser.parse_args()

    # Queue consumer mode - listen to gateway requests
    if args.queue_consumer:
        return run_queue_consumer(
            prefetch=args.prefetch,
            no_funding=args.no_funding,
            no_detail=args.no_detail
        )

    if not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        return 1

    if not HAS_PIKA:
        print("Error: pika not installed")
        return 1

    print(f"Guide Shredder Consumer v2 (SP-based)")
    print(f"{'='*60}")
    print(f"RabbitMQ: {args.rabbitmq_host}:{args.rabbitmq_port}")
    print(f"Queue In:  {RABBITMQ_QUEUE_IN}")
    print(f"Queue Out (addresses): {RABBITMQ_QUEUE_OUT}")
    print(f"Queue Out (detail):    {RABBITMQ_QUEUE_DETAIL}")
    print(f"Prefetch: {args.prefetch}")
    print(f"Max messages: {args.max_messages if args.max_messages > 0 else 'unlimited'}")
    print(f"Address queue: {'DISABLED' if args.no_funding else 'ENABLED'}")
    print(f"Detail queue:  {'DISABLED' if args.no_detail else 'ENABLED'}")
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

    # Create API session
    solscan_session = create_solscan_session()

    # Start consumer
    consumer = GuideConsumerV2(
        db_conn, channel, solscan_session,
        args.max_messages, args.dry_run, args.no_funding, args.no_detail
    )

    try:
        consumer.start()
    finally:
        solscan_session.close()
        connection.close()
        db_conn.close()

    print(f"\n{'='*60}")
    print(f"Done! Processed {consumer.message_count} messages")
    print(f"  Total transactions: {consumer.total_tx}")
    print(f"  Total edges: {consumer.total_edges}")
    print(f"  Total transfers: {consumer.total_transfers}")
    print(f"  Total swaps: {consumer.total_swaps}")
    print(f"  Total activities: {consumer.total_activities}")
    print(f"  Total addresses: {consumer.total_addresses}")
    print(f"  Total signers: {consumer.total_signers}")
    print(f"  Total instructions: {consumer.total_instructions}")
    print(f"{'='*60}")

    return 0


if __name__ == '__main__':
    exit(main())
