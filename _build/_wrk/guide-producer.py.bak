#!/usr/bin/env python3
"""
Guide Producer - Chainstack Signature Fetcher for theGuide Pipeline
Fetches transaction signatures from Chainstack RPC and publishes batches to RabbitMQ
for consumption by shredder-guide.py.

Workflow:
1. Fetch signatures via Chainstack RPC (getSignaturesForAddress)
2. Smart sync: only fetch new signatures since last known (unless --force-all)
3. Optionally filter out already-processed signatures
4. Batch signatures into groups of 20
5. Publish batches to RabbitMQ queue 'tx.guide.signatures'

Usage:
    # Single address (smart sync - fetches only new signatures)
    python guide-producer.py <address>

    # Multiple addresses
    python guide-producer.py addr1 addr2 addr3

    # From file (one address per line)
    python guide-producer.py --address-file wallets.txt

    # Sync all mints from tx_address table
    python guide-producer.py --sync-mint-transactions

    # Combine CLI addresses with mint sync
    python guide-producer.py addr1 addr2 --sync-mint-transactions

    # Sync only top N mints by transaction count
    python guide-producer.py --sync-mint-transactions --top-address-limit 10

    # Force full fetch (bypass smart sync, let dups be handled downstream)
    python guide-producer.py addr1 --force-all

    # Direct-to-detail: queue shredded txs for detail enrichment
    python guide-producer.py --direct-to-detail
    python guide-producer.py --direct-to-detail --detail-max-batches 100

    # Reprocess signatures from file (delete + re-queue)
    python guide-producer.py --reprocess-signatures-file reprocess.txt
    python guide-producer.py --reprocess-signatures-file reprocess.txt --reprocess-priority 8

    # Limit signatures per address
    python guide-producer.py addr1 --max-signatures 5000

    # With pagination bounds
    python guide-producer.py <address> --before <signature> --until <signature>

Examples:
    python guide-producer.py 7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU
    python guide-producer.py --sync-mint-transactions
    python guide-producer.py --address-file suspects.txt --sync-mint-transactions --force-all
"""

import argparse
import json
import requests
import time
import orjson
from typing import Optional, List, Generator
from datetime import datetime

# RabbitMQ client
try:
    import pika
    HAS_PIKA = True
except ImportError:
    HAS_PIKA = False

# MySQL connector (optional, for filtering)
try:
    import mysql.connector
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

# =============================================================================
# Configuration
# =============================================================================

# Chainstack Solana RPC
CHAINSTACK_RPC_URL = "https://solana-mainnet.core.chainstack.com/d0eda0bf942f17f68a75b67030395ceb"

# RabbitMQ (legacy - default vhost)
RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5692
RABBITMQ_USER = 'admin'
RABBITMQ_PASS = 'admin123'
RABBITMQ_QUEUE = 'tx.guide.signatures'
RABBITMQ_QUEUE_DETAIL = 'tx.detail.transactions'

# RabbitMQ (new - t16o_mq vhost for gateway integration)
RABBITMQ_VHOST = 't16o_mq'
RABBITMQ_REQUEST_QUEUE = 'mq.guide.producer.request'
RABBITMQ_RESPONSE_QUEUE = 'mq.guide.producer.response'
SHREDDER_REQUEST_QUEUE = 'mq.guide.shredder.request'  # For incremental cascade

# Database
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3396,
    'user': 'root',
    'password': 'rootpassword',
    'database': 't16o_db'
}


# =============================================================================
# RPC Functions
# =============================================================================

def create_rpc_session() -> requests.Session:
    """Create a requests session with persistent connection"""
    session = requests.Session()
    session.headers.update({"Content-Type": "application/json"})
    return session


def fetch_signatures_rpc(
    session: requests.Session,
    address: str,
    rpc_url: str,
    limit: int = 1000,
    before: Optional[str] = None,
    until: Optional[str] = None
) -> dict:
    """Fetch signatures from Solana RPC using getSignaturesForAddress

    Args:
        session: requests session
        address: Account address to fetch signatures for
        rpc_url: Solana RPC endpoint URL
        limit: Max signatures per call (up to 1000)
        before: Signature to fetch before (for pagination)
        until: Signature to fetch until (optional)

    Returns:
        RPC response dict with 'result' array of signatures
    """
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

    response = session.post(rpc_url, json=payload)
    response.raise_for_status()
    return response.json()


def fetch_all_signatures(
    session: requests.Session,
    address: str,
    rpc_url: str,
    max_signatures: float = float('inf'),
    before: Optional[str] = None,
    until: Optional[str] = None,
    delay: float = 0.2,
    skip_failed: bool = True
) -> Generator[dict, None, None]:
    """Generator that fetches all signatures for an address with pagination

    Yields individual signature objects from RPC response.
    max_signatures can be float('inf') for unlimited.
    """
    total_fetched = 0
    batch_num = 0

    while total_fetched < max_signatures:
        batch_num += 1
        remaining = max_signatures - total_fetched
        fetch_limit = min(1000, int(remaining) if remaining != float('inf') else 1000)

        try:
            response = fetch_signatures_rpc(
                session, address, rpc_url,
                limit=fetch_limit,
                before=before,
                until=until
            )
        except requests.RequestException as e:
            print(f"  [!] RPC Error: {e}")
            break

        if 'error' in response:
            print(f"  [!] RPC returned error: {response['error']}")
            break

        signatures = response.get('result', [])
        if not signatures:
            break

        # Yield each signature
        for sig in signatures:
            # Skip failed transactions if requested
            if skip_failed and sig.get('err') is not None:
                continue
            yield sig

        total_fetched += len(signatures)

        # Update pagination cursor
        before = signatures[-1].get('signature') if signatures else None

        # Check if we got fewer than requested (end of data)
        if len(signatures) < fetch_limit:
            break

        # Rate limiting between RPC calls
        if delay > 0:
            time.sleep(delay)


# =============================================================================
# Batching
# =============================================================================

def batch_signatures(signatures: Generator, batch_size: int = 20) -> Generator[List[str], None, None]:
    """Batch signatures into groups

    Args:
        signatures: Generator of signature objects
        batch_size: Number of signatures per batch

    Yields:
        Lists of signature strings
    """
    batch = []
    for sig_obj in signatures:
        sig = sig_obj.get('signature')
        if sig:
            batch.append(sig)
            if len(batch) >= batch_size:
                yield batch
                batch = []

    # Yield remaining
    if batch:
        yield batch


# =============================================================================
# RabbitMQ
# =============================================================================

def setup_rabbitmq(host: str, port: int, user: str, password: str):
    """Setup RabbitMQ connection and channel"""
    credentials = pika.PlainCredentials(user, password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host, port=port, credentials=credentials)
    )
    channel = connection.channel()
    channel.queue_declare(
        queue=RABBITMQ_QUEUE,
        durable=True,
        arguments={'x-max-priority': 10}
    )
    return connection, channel


def publish_batch(channel, signatures: List[str], priority: int = 5) -> bool:
    """Publish a batch of signatures to RabbitMQ"""
    try:
        message = {"signatures": signatures}
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=orjson.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Persistent
                content_type='application/json',
                priority=priority
            )
        )
        return True
    except Exception as e:
        print(f"  [!] Publish error: {e}")
        return False


def publish_batch_to_detail(channel, signatures: List[str], priority: int = 5) -> bool:
    """Publish a batch of signatures to detail queue"""
    try:
        message = {"signatures": signatures}
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE_DETAIL,
            body=orjson.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Persistent
                content_type='application/json',
                priority=priority
            )
        )
        return True
    except Exception as e:
        print(f"  [!] Publish error: {e}")
        return False


# =============================================================================
# Gateway Integration (t16o_mq vhost)
# =============================================================================

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
    for queue in [RABBITMQ_REQUEST_QUEUE, RABBITMQ_RESPONSE_QUEUE]:
        channel.queue_declare(queue=queue, durable=True, arguments={'x-max-priority': 10})

    return connection, channel


def publish_response(channel, request_id: str, status: str, result: dict, error: str = None):
    """Publish response to gateway response queue"""
    response = {
        'request_id': request_id,
        'worker': 'producer',
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


def publish_cascade_to_shredder(channel, request_id: str, correlation_id: str,
                                 signatures: list, batch_num: int,
                                 total_batches: int, priority: int = 5) -> bool:
    """Publish a batch of signatures directly to shredder request queue (incremental cascade)"""
    cascade_msg = {
        'request_id': f"{request_id}-batch{batch_num}",
        'correlation_id': correlation_id,  # Track original REST request
        'parent_request_id': request_id,
        'action': 'cascade',
        'source_worker': 'producer',
        'priority': priority,
        'timestamp': datetime.now().isoformat() + 'Z',
        'batch': {
            'signatures': signatures,
            'batch_num': batch_num,
            'total_batches': total_batches
        }
    }

    try:
        channel.basic_publish(
            exchange='',
            routing_key=SHREDDER_REQUEST_QUEUE,
            body=json.dumps(cascade_msg).encode('utf-8'),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='application/json',
                priority=priority
            )
        )
        return True
    except Exception as e:
        print(f"[ERROR] Failed to cascade to shredder: {e}")
        return False


def process_gateway_request(message: dict, rpc_session, gateway_channel, db_cursor=None) -> dict:
    """Process a request from the gateway with incremental cascade to shredder

    Args:
        message: Gateway request message with batch descriptor
        rpc_session: Requests session for RPC calls
        gateway_channel: Channel for publishing cascades to shredder
        db_cursor: Optional DB cursor for smart sync

    Returns:
        Result dict with processed/errors counts
    """
    request_id = message.get('request_id', 'unknown')
    correlation_id = message.get('correlation_id', request_id)  # Track original request
    batch = message.get('batch', {})
    priority = message.get('priority', 5)

    # Extract batch parameters
    filters = batch.get('filters', {})
    address = filters.get('mint_address') or filters.get('address')
    max_signatures = batch.get('size', 100)

    # Extract pagination parameters from request (override smart sync if provided)
    request_before = filters.get('before')
    request_until = filters.get('until')

    if not address:
        return {'processed': 0, 'errors': 1, 'error': 'No address provided in batch.filters'}

    print(f"[{request_id[:8]}] Processing request for {address[:20]}... (correlation: {correlation_id[:8]})")
    if request_before:
        print(f"  Using request 'before': {request_before[:20]}...")
    if request_until:
        print(f"  Using request 'until': {request_until[:20]}...")

    # Fetch signatures
    total_fetched = 0
    total_batched = 0
    batch_size = 20
    estimated_batches = (max_signatures + batch_size - 1) // batch_size  # Ceiling division

    try:
        # Smart sync: determine if we need NEW signatures or MORE historical ones
        # Request-provided before/until take precedence over smart sync
        until_sig = request_until  # For fetching new signatures (stop at this one)
        before_sig = request_before  # For fetching older signatures (start from this one going back)
        existing_count = 0

        if db_cursor:
            # Get address_id and count existing signatures
            db_cursor.execute("SELECT id FROM tx_address WHERE address = %s", (address,))
            addr_row = db_cursor.fetchone()
            if addr_row:
                addr_id = addr_row[0]

                # Count existing signatures for this address
                db_cursor.execute("""
                    SELECT COUNT(DISTINCT t.id) FROM tx t
                    LEFT JOIN tx_guide g ON g.tx_id = t.id
                    WHERE t.signer_address_id = %s
                       OR g.from_address_id = %s
                       OR g.to_address_id = %s
                """, (addr_id, addr_id, addr_id))
                existing_count = db_cursor.fetchone()[0] or 0

                # Only use smart sync if request didn't provide before/until
                if existing_count > 0 and not request_before and not request_until:
                    if max_signatures > existing_count:
                        # User wants MORE historical data - use 'before' with oldest sig
                        db_cursor.execute("""
                            SELECT t.signature FROM tx t
                            LEFT JOIN tx_guide g ON g.tx_id = t.id
                            WHERE t.signer_address_id = %s
                               OR g.from_address_id = %s
                               OR g.to_address_id = %s
                            ORDER BY t.block_time ASC LIMIT 1
                        """, (addr_id, addr_id, addr_id))
                        row = db_cursor.fetchone()
                        if row:
                            before_sig = row[0]
                            print(f"  Smart sync: fetching {max_signatures - existing_count} MORE historical sigs (have {existing_count}, want {max_signatures})")
                            print(f"  Starting before: {before_sig[:20]}...")
                    else:
                        # Only fetch NEW signatures - use 'until' with newest sig
                        db_cursor.execute("""
                            SELECT t.signature FROM tx t
                            LEFT JOIN tx_guide g ON g.tx_id = t.id
                            WHERE t.signer_address_id = %s
                               OR g.from_address_id = %s
                               OR g.to_address_id = %s
                            ORDER BY t.block_time DESC LIMIT 1
                        """, (addr_id, addr_id, addr_id))
                        row = db_cursor.fetchone()
                        if row:
                            until_sig = row[0]
                            print(f"  Smart sync: fetching new sigs after {until_sig[:20]}...")

        signatures = []
        all_signatures = []  # Track all signatures for final response
        for sig_obj in fetch_all_signatures(
            rpc_session, address, CHAINSTACK_RPC_URL,
            max_signatures=max_signatures, until=until_sig, before=before_sig
        ):
            # Extract signature string from the RPC response object
            sig_str = sig_obj.get('signature') if isinstance(sig_obj, dict) else sig_obj
            if sig_str:
                signatures.append(sig_str)
                all_signatures.append(sig_str)
            total_fetched += 1

            # INCREMENTAL CASCADE: Publish to shredder every batch_size signatures
            while len(signatures) >= batch_size:
                batch_to_send = signatures[:batch_size]
                signatures = signatures[batch_size:]
                total_batched += 1

                if publish_cascade_to_shredder(gateway_channel, request_id, correlation_id,
                                                batch_to_send, total_batched, estimated_batches, priority):
                    print(f"  [CASCADE] Batch {total_batched}/{estimated_batches} -> shredder ({len(batch_to_send)} sigs)")

        # Publish remaining signatures
        if signatures:
            total_batched += 1
            if publish_cascade_to_shredder(gateway_channel, request_id, correlation_id,
                                            signatures, total_batched, total_batched, priority):
                print(f"  [CASCADE] Batch {total_batched}/{total_batched} -> shredder ({len(signatures)} sigs)")

        print(f"  Fetched {total_fetched} signatures, cascaded {total_batched} batches to shredder")

        return {
            'processed': total_fetched,
            'batches': total_batched,
            'errors': 0,
            # Don't include all signatures - they were already cascaded incrementally
            'cascade_to': []  # Already cascaded directly
        }

    except Exception as e:
        print(f"  [ERROR] {e}")
        return {'processed': total_fetched, 'batches': total_batched, 'errors': 1, 'error': str(e)}


def run_queue_consumer(rpc_url: str = CHAINSTACK_RPC_URL, prefetch: int = 1):
    """Run as a queue consumer, listening for gateway requests"""
    print(f"""
+-----------------------------------------------------------+
|         Guide Producer - Queue Consumer Mode              |
|                                                           |
|  vhost:     {RABBITMQ_VHOST}                                     |
|  queue:     {RABBITMQ_REQUEST_QUEUE}              |
|  prefetch:  {prefetch}                                            |
+-----------------------------------------------------------+
""")

    rpc_session = create_rpc_session()

    # In gateway mode, we don't need legacy queue - signatures pass via cascade
    # Set legacy_channel to None to skip legacy publishing
    legacy_channel = None
    print("[INFO] Gateway mode: legacy queue publishing disabled (using cascade)")

    # Setup DB connection with reconnect capability
    db_state = {'conn': None, 'cursor': None}

    def ensure_db_connection():
        """Ensure DB connection is alive, reconnect if needed"""
        if not HAS_MYSQL:
            return None
        try:
            # Check if connection exists and is truly alive (ping tests actual connectivity)
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
                db_state['cursor'] = db_state['conn'].cursor()
                print("[OK] Database (re)connected")
            return db_state['cursor']
        except Exception as e:
            print(f"[WARN] Database connection failed: {e}")
            db_state['conn'] = None
            db_state['cursor'] = None
            return None

    # Initial connection
    ensure_db_connection()

    while True:
        try:
            # Setup gateway connection
            gateway_conn, gateway_channel = setup_gateway_rabbitmq()
            gateway_channel.basic_qos(prefetch_count=prefetch)

            print(f"[OK] Connected to {RABBITMQ_REQUEST_QUEUE}")
            print("[INFO] Waiting for requests...")

            def callback(ch, method, properties, body):
                try:
                    message = json.loads(body.decode('utf-8'))
                    request_id = message.get('request_id', 'unknown')

                    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Received request {request_id[:8]}")

                    # Ensure DB connection is alive before processing
                    db_cursor = ensure_db_connection()

                    # Process the request (pass gateway_channel for incremental cascade)
                    result = process_gateway_request(message, rpc_session, gateway_channel, db_cursor)

                    # Determine status
                    status = 'completed' if result.get('errors', 0) == 0 else 'partial'

                    # Publish response
                    publish_response(gateway_channel, request_id, status, result)

                    # Ack the message
                    ch.basic_ack(delivery_tag=method.delivery_tag)

                except Exception as e:
                    print(f"[ERROR] Failed to process message: {e}")
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
            time.sleep(5)

    # Cleanup
    if legacy_conn:
        legacy_conn.close()
    if db_state['conn']:
        db_state['conn'].close()


# =============================================================================
# Address Loading
# =============================================================================

def load_addresses_from_file(filepath: str) -> List[str]:
    """Load addresses from a file (one per line)"""
    addresses = []
    with open(filepath, 'r') as f:
        for line in f:
            addr = line.strip()
            if addr and not addr.startswith('#'):
                addresses.append(addr)
    return addresses


# =============================================================================
# Optional DB Filtering
# =============================================================================

def get_existing_signatures(cursor, signatures: List[str]) -> set:
    """Get signatures that already exist in tx_guide"""
    if not signatures:
        return set()

    placeholders = ','.join(['%s'] * len(signatures))
    cursor.execute(f"""
        SELECT DISTINCT t.signature
        FROM tx t
        INNER JOIN tx_guide g ON g.tx_id = t.id
        WHERE t.signature IN ({placeholders})
    """, signatures)

    return {row[0] for row in cursor.fetchall()}


def filter_batch(batch: List[str], cursor) -> List[str]:
    """Filter out signatures that already have edges in tx_guide"""
    if cursor is None:
        return batch

    existing = get_existing_signatures(cursor, batch)
    if not existing:
        return batch

    filtered = [s for s in batch if s not in existing]
    return filtered


def get_all_mints(cursor) -> List[str]:
    """Get all mint addresses from tx_address, excluding SOL/WSOL"""
    # SOL and WSOL mint addresses - too much traffic
    SOL_MINT = 'So11111111111111111111111111111111111111111'
    WSOL_MINT = 'So11111111111111111111111111111111111111112'

    cursor.execute("""
        SELECT address FROM tx_address
        WHERE address_type = 'mint'
        AND address NOT IN (%s, %s)
    """, (SOL_MINT, WSOL_MINT))
    return [row[0] for row in cursor.fetchall()]


def get_top_mints(cursor, limit: int) -> List[str]:
    """Get top N mint addresses by transaction count in tx_guide, excluding SOL/WSOL"""
    SOL_MINT = 'So11111111111111111111111111111111111111111'
    WSOL_MINT = 'So11111111111111111111111111111111111111112'

    cursor.execute("""
        SELECT a.address, COUNT(DISTINCT g.tx_id) as tx_count
        FROM tx_address a
        JOIN tx_token tk ON tk.mint_address_id = a.id
        JOIN tx_guide g ON g.token_id = tk.id
        WHERE a.address_type = 'mint'
          AND a.address NOT IN (%s, %s)
        GROUP BY a.id, a.address
        ORDER BY tx_count DESC
        LIMIT %s
    """, (SOL_MINT, WSOL_MINT, limit))
    
    results = cursor.fetchall()
    if results:
        print(f"  Top {len(results)} mints by tx_count:")
        for addr, count in results[:5]:  # Show first 5
            print(f"    {addr[:16]}... : {count} txs")
        if len(results) > 5:
            print(f"    ... and {len(results) - 5} more")
    
    return [row[0] for row in results]


def get_last_known_signature(cursor, address: str) -> Optional[str]:
    """Get the most recent signature for an address from tx_guide

    Works for both wallets (via from/to_address_id) and mints (via token_id).
    Returns None if no transactions found.
    """
    # First get the address ID
    cursor.execute("SELECT id, address_type FROM tx_address WHERE address = %s", (address,))
    row = cursor.fetchone()
    if not row:
        return None

    addr_id, addr_type = row

    if addr_type == 'mint':
        # For mints, look up via tx_token -> tx_guide.token_id
        cursor.execute("""
            SELECT t.signature
            FROM tx t
            JOIN tx_guide g ON g.tx_id = t.id
            JOIN tx_token tk ON tk.id = g.token_id
            WHERE tk.mint_address_id = %s
            ORDER BY t.block_time DESC
            LIMIT 1
        """, (addr_id,))
    else:
        # For wallets/other, look up via tx_guide.from_address_id or to_address_id
        cursor.execute("""
            SELECT t.signature
            FROM tx t
            JOIN tx_guide g ON g.tx_id = t.id
            WHERE g.from_address_id = %s OR g.to_address_id = %s
            ORDER BY t.block_time DESC
            LIMIT 1
        """, (addr_id, addr_id))

    row = cursor.fetchone()
    return row[0] if row else None


def load_signatures_from_file(filepath: str) -> List[str]:
    """Load signatures from a file (one per line), skip header if present"""
    signatures = []
    with open(filepath, 'r') as f:
        for line in f:
            sig = line.strip()
            # Skip empty lines, comments, and header
            if sig and not sig.startswith('#') and sig != 'signature' and len(sig) >= 64:
                signatures.append(sig)
    return signatures


def delete_signatures_from_db(cursor, conn, signatures: List[str], dry_run: bool = False) -> int:
    """
    Delete signatures from tx table (cascades to all child tables).
    Returns count of deleted rows.
    """
    if not signatures:
        return 0

    deleted = 0
    for sig in signatures:
        if dry_run:
            # Just check if exists
            cursor.execute("SELECT id FROM tx WHERE signature = %s", (sig,))
            if cursor.fetchone():
                deleted += 1
        else:
            cursor.execute("DELETE FROM tx WHERE signature = %s", (sig,))
            if cursor.rowcount > 0:
                deleted += 1

    if not dry_run:
        conn.commit()

    return deleted


def get_signatures_needing_detail(cursor, batch_size: int = 20, max_batches: int = 0) -> Generator[List[str], None, None]:
    """Generator that yields batches of signatures that need detail enrichment.

    Queries tx table for transactions that have SHREDDER_COMPLETE (63) but missing DETAILED (64).
    tx_state is a bitmask: bit 6 (64) = DETAILED

    Args:
        cursor: MySQL cursor
        batch_size: Number of signatures per batch
        max_batches: Maximum batches to yield (0 = unlimited)

    Yields:
        Lists of signature strings
    """
    # tx_state bitmask: SHREDDER_COMPLETE = 63, DETAILED = 64
    SHREDDER_COMPLETE = 63
    DETAILED_BIT = 64

    offset = 0
    batch_num = 0

    while True:
        # Find tx with shredder complete but missing DETAILED bit
        cursor.execute("""
            SELECT signature
            FROM tx
            WHERE (tx_state & %s) = %s AND (tx_state & %s) = 0
            ORDER BY block_time DESC
            LIMIT %s OFFSET %s
        """, (SHREDDER_COMPLETE, SHREDDER_COMPLETE, DETAILED_BIT, batch_size, offset))
        
        rows = cursor.fetchall()
        if not rows:
            break
            
        signatures = [row[0] for row in rows]
        yield signatures
        
        batch_num += 1
        offset += batch_size
        
        if max_batches > 0 and batch_num >= max_batches:
            break


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Guide Producer - Fetch signatures from Chainstack and publish to RabbitMQ'
    )
    parser.add_argument('addresses', nargs='*', help='Solana address(es) to fetch signatures for')
    parser.add_argument('--address-file', help='File containing addresses (one per line)')
    parser.add_argument('--before', help='Signature to fetch before (pagination start)')
    parser.add_argument('--until', help='Signature to fetch until (pagination end)')
    parser.add_argument('--max-signatures', default='all',
                        help='Maximum signatures per address (default: all)')
    parser.add_argument('--sync-mint-transactions', action='store_true',
                        help='Sync transactions for all mints in tx_address (incremental)')
    parser.add_argument('--top-address-limit', type=int, default=None,
                        help='Limit to top N mints by tx_guide transaction count (use with --sync-mint-transactions)')
    parser.add_argument('--force-all', action='store_true',
                        help='Force full fetch for all addresses, bypass smart sync')
    parser.add_argument('--batch-size', type=int, default=20,
                        help='Signatures per RabbitMQ message (default: 20)')
    parser.add_argument('--rpc-url', default=CHAINSTACK_RPC_URL, help='Solana RPC URL')
    parser.add_argument('--rpc-delay', type=float, default=0.2,
                        help='Delay between RPC calls in seconds')
    parser.add_argument('--rabbitmq-host', default=RABBITMQ_HOST, help='RabbitMQ host')
    parser.add_argument('--rabbitmq-port', type=int, default=RABBITMQ_PORT, help='RabbitMQ port')
    parser.add_argument('--rabbitmq-user', default=RABBITMQ_USER, help='RabbitMQ user')
    parser.add_argument('--rabbitmq-pass', default=RABBITMQ_PASS, help='RabbitMQ password')
    parser.add_argument('--priority', type=int, default=5, help='Message priority 1-10 (default: 5)')
    parser.add_argument('--filter-existing', action='store_true',
                        help='Filter out signatures already in tx_guide (requires DB)')
    parser.add_argument('--db-host', default='localhost', help='MySQL host')
    parser.add_argument('--db-port', type=int, default=3396, help='MySQL port')
    parser.add_argument('--db-user', default='root', help='MySQL user')
    parser.add_argument('--db-pass', default='rootpassword', help='MySQL password')
    parser.add_argument('--db-name', default='t16o_db', help='MySQL database')
    parser.add_argument('--dry-run', action='store_true',
                        help='Fetch and print only, do not publish to RabbitMQ')
    parser.add_argument('--include-failed', action='store_true',
                        help='Include failed transactions (default: skip)')
    parser.add_argument('--direct-to-detail', action='store_true',
                        help='Query DB for shredded txs missing detail and send to tx.detail.transactions queue')
    parser.add_argument('--detail-max-batches', type=int, default=0,
                        help='Max batches to send in direct-to-detail mode (0 = unlimited)')
    parser.add_argument('--reprocess-signatures-file', type=str, default=None,
                        help='File with signatures to reprocess (delete existing + re-queue)')
    parser.add_argument('--reprocess-priority', type=int, default=8,
                        help='Priority for reprocessed signatures (default: 8)')
    parser.add_argument('--queue-consumer', action='store_true',
                        help='Run as queue consumer, listening for gateway requests')
    parser.add_argument('--prefetch', type=int, default=1,
                        help='Queue prefetch count for queue-consumer mode (default: 1)')

    args = parser.parse_args()

    # Queue consumer mode - ignore other args and run consumer
    if args.queue_consumer:
        return run_queue_consumer(rpc_url=args.rpc_url, prefetch=args.prefetch)

    # Parse max_signatures ('all' or integer)
    if args.max_signatures == 'all':
        max_signatures = float('inf')
    else:
        try:
            max_signatures = int(args.max_signatures)
        except ValueError:
            print(f"Error: --max-signatures must be 'all' or an integer, got '{args.max_signatures}'")
            return 1

    # Collect CLI addresses
    addresses = list(args.addresses) if args.addresses else []
    if args.address_file:
        addresses.extend(load_addresses_from_file(args.address_file))

    # Validate: need addresses OR --sync-mint-transactions OR --direct-to-detail OR --reprocess-signatures-file
    if not addresses and not args.sync_mint_transactions and not args.direct_to_detail and not args.reprocess_signatures_file:
        print("Error: No addresses provided")
        print("Usage: python guide-producer.py <address> [--address-file file.txt] [--sync-mint-transactions] [--direct-to-detail] [--reprocess-signatures-file file.txt]")
        return 1

    # Check dependencies
    if not args.dry_run and not HAS_PIKA:
        print("Error: pika not installed")
        print("Install with: pip install pika")
        return 1

    # DB required for --sync-mint-transactions, smart sync, --direct-to-detail, or --reprocess-signatures-file
    needs_db = args.sync_mint_transactions or args.filter_existing or args.direct_to_detail or args.reprocess_signatures_file or (not args.force_all and addresses)
    if needs_db and not HAS_MYSQL:
        print("Error: mysql-connector-python not installed")
        print("Install with: pip install mysql-connector-python")
        return 1

    print(f"Guide Producer - Chainstack to RabbitMQ")
    print(f"{'='*60}")
    print(f"CLI Addresses: {len(addresses)}")
    print(f"Sync mint transactions: {args.sync_mint_transactions}")
    if args.top_address_limit:
        print(f"Top address limit: {args.top_address_limit}")
    print(f"Max signatures per address: {args.max_signatures}")
    print(f"Batch size: {args.batch_size}")
    print(f"Smart sync: {not args.force_all} (force-all: {args.force_all})")
    print(f"RPC: {args.rpc_url[:50]}...")
    print(f"RabbitMQ: {args.rabbitmq_host}:{args.rabbitmq_port}")
    print(f"Queue: {RABBITMQ_QUEUE}")
    if args.filter_existing:
        print(f"Filter existing: enabled")
    if args.dry_run:
        print(f"MODE: DRY RUN")
    print()

    # Setup connections
    rpc_session = create_rpc_session()
    rabbitmq_conn = None
    rabbitmq_channel = None
    db_conn = None
    db_cursor = None

    if not args.dry_run:
        print(f"Connecting to RabbitMQ...")
        rabbitmq_conn, rabbitmq_channel = setup_rabbitmq(
            args.rabbitmq_host, args.rabbitmq_port,
            args.rabbitmq_user, args.rabbitmq_pass
        )
        print(f"  Connected to queue: {RABBITMQ_QUEUE}")
        
        # Also declare detail queue if needed
        if args.direct_to_detail:
            rabbitmq_channel.queue_declare(
                queue=RABBITMQ_QUEUE_DETAIL,
                durable=True,
                arguments={'x-max-priority': 10}
            )
            print(f"  Also connected to queue: {RABBITMQ_QUEUE_DETAIL}")

    if needs_db:
        print(f"Connecting to MySQL...")
        db_conn = mysql.connector.connect(
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_pass,
            database=args.db_name
        )
        db_cursor = db_conn.cursor()
        print(f"  Connected")

    # =========================================================================
    # Direct-to-Detail Mode
    # =========================================================================
    if args.direct_to_detail:
        print()
        print("--- Direct-to-Detail Mode ---")
        print(f"Querying for shredded transactions needing detail...")
        
        # Count total needing detail (shredder complete but missing DETAILED bit)
        db_cursor.execute("SELECT COUNT(*) FROM tx WHERE (tx_state & 63) = 63 AND (tx_state & 64) = 0")
        total_needing = db_cursor.fetchone()[0]
        print(f"  Found {total_needing} transactions needing detail")
        
        if total_needing == 0:
            print(f"  Nothing to do!")
        else:
            detail_batches = 0
            detail_signatures = 0
            
            try:
                for batch in get_signatures_needing_detail(db_cursor, args.batch_size, args.detail_max_batches):
                    if args.dry_run:
                        print(f"  [DRY RUN] Batch {detail_batches + 1}: {len(batch)} signatures")
                        print(f"    First: {batch[0][:20]}...")
                    else:
                        if publish_batch_to_detail(rabbitmq_channel, batch, args.priority):
                            detail_batches += 1
                            detail_signatures += len(batch)
                            
                            if detail_batches % 10 == 0:
                                print(f"  Published {detail_batches} batches ({detail_signatures} signatures)")
                                
            except KeyboardInterrupt:
                print("\nInterrupted by user")
            
            print()
            print("=" * 60)
            print(f"Direct-to-Detail Complete!")
            print(f"  Batches published: {detail_batches}")
            print(f"  Signatures queued: {detail_signatures}")
            print(f"{'='*60}")
        
        # Cleanup and exit
        if rabbitmq_conn:
            rabbitmq_conn.close()
        if db_conn:
            db_conn.close()
        return 0

    # =========================================================================
    # Reprocess Signatures Mode
    # =========================================================================
    if args.reprocess_signatures_file:
        import os
        print()
        print("--- Reprocess Signatures Mode ---")

        if not os.path.exists(args.reprocess_signatures_file):
            print(f"Error: File not found: {args.reprocess_signatures_file}")
            return 1

        # Load signatures from file
        print(f"Loading signatures from {args.reprocess_signatures_file}...")
        reprocess_sigs = load_signatures_from_file(args.reprocess_signatures_file)
        print(f"  Found {len(reprocess_sigs)} signatures to reprocess")

        if not reprocess_sigs:
            print("  No signatures to reprocess!")
            if rabbitmq_conn:
                rabbitmq_conn.close()
            if db_conn:
                db_conn.close()
            return 0

        # Step 1: Delete existing records
        print(f"\nStep 1: Deleting existing records from DB...")
        deleted_count = delete_signatures_from_db(db_cursor, db_conn, reprocess_sigs, args.dry_run)
        print(f"  {'Would delete' if args.dry_run else 'Deleted'} {deleted_count} transactions (cascades to all child tables)")

        # Step 2: Publish to queue
        print(f"\nStep 2: Publishing to queue with priority {args.reprocess_priority}...")
        reprocess_batches = 0
        reprocess_queued = 0

        for i in range(0, len(reprocess_sigs), args.batch_size):
            batch = reprocess_sigs[i:i + args.batch_size]

            if args.dry_run:
                print(f"  [DRY RUN] Batch {reprocess_batches + 1}: {len(batch)} signatures")
                reprocess_batches += 1
                reprocess_queued += len(batch)
            else:
                if publish_batch(rabbitmq_channel, batch, args.reprocess_priority):
                    reprocess_batches += 1
                    reprocess_queued += len(batch)

                    if reprocess_batches % 10 == 0:
                        print(f"  Published {reprocess_batches} batches ({reprocess_queued} signatures)")

        print()
        print("=" * 60)
        print(f"Reprocess Complete!")
        print(f"  Signatures processed: {len(reprocess_sigs)}")
        print(f"  DB records deleted: {deleted_count}")
        print(f"  Batches published: {reprocess_batches}")
        print(f"  Signatures queued: {reprocess_queued}")
        print(f"  Priority: {args.reprocess_priority}")
        print(f"{'='*60}")

        # Cleanup and exit
        if rabbitmq_conn:
            rabbitmq_conn.close()
        if db_conn:
            db_conn.close()
        return 0

    # Load mints if --sync-mint-transactions
    mint_addresses = []
    if args.sync_mint_transactions:
        if args.top_address_limit:
            print(f"Loading top {args.top_address_limit} mints by tx_guide count...")
            mint_addresses = get_top_mints(db_cursor, args.top_address_limit)
            print(f"  Selected {len(mint_addresses)} mints")
        else:
            print(f"Loading all mints from tx_address...")
            mint_addresses = get_all_mints(db_cursor)
            print(f"  Found {len(mint_addresses)} mints")

    # Combine all addresses: CLI addresses + mints
    all_addresses = addresses + mint_addresses
    print(f"Total addresses to process: {len(all_addresses)}")

    # Stats
    total_signatures = 0
    total_batches = 0
    total_filtered = 0

    try:
        for addr_idx, address in enumerate(all_addresses, 1):
            print(f"\n--- Address {addr_idx}/{len(all_addresses)}: {address[:16]}... ---")

            addr_signatures = 0
            addr_batches = 0

            # Smart sync: determine 'until' signature
            until_sig = args.until  # CLI override takes precedence
            if not args.force_all and not until_sig and db_cursor:
                last_known = get_last_known_signature(db_cursor, address)
                if last_known:
                    until_sig = last_known
                    print(f"  Smart sync: will fetch until {last_known[:20]}...")
                else:
                    print(f"  No prior transactions found, full fetch")
            elif args.force_all:
                print(f"  Force-all: full fetch (bypassing smart sync)")

            # Fetch signatures generator
            sig_generator = fetch_all_signatures(
                rpc_session, address, args.rpc_url,
                max_signatures=max_signatures,
                before=args.before,
                until=until_sig,
                delay=args.rpc_delay,
                skip_failed=not args.include_failed
            )

            # Batch and publish
            for batch in batch_signatures(sig_generator, args.batch_size):
                # Optional filtering
                original_count = len(batch)
                if db_cursor:
                    batch = filter_batch(batch, db_cursor)
                    filtered_count = original_count - len(batch)
                    total_filtered += filtered_count

                if not batch:
                    continue

                if args.dry_run:
                    print(f"  [DRY RUN] Batch {addr_batches + 1}: {len(batch)} signatures")
                    print(f"    First: {batch[0][:20]}...")
                    print(f"    Last:  {batch[-1][:20]}...")
                else:
                    if publish_batch(rabbitmq_channel, batch, args.priority):
                        addr_batches += 1
                        addr_signatures += len(batch)

                        if addr_batches % 10 == 0:
                            print(f"  Published {addr_batches} batches ({addr_signatures} signatures)")

            total_signatures += addr_signatures
            total_batches += addr_batches

            print(f"  Address complete: {addr_batches} batches, {addr_signatures} signatures")

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")

    finally:
        rpc_session.close()
        if rabbitmq_conn:
            rabbitmq_conn.close()
        if db_conn:
            db_conn.close()

    print(f"\n{'='*60}")
    print(f"Done!")
    print(f"  CLI addresses processed: {len(addresses)}")
    print(f"  Mint addresses processed: {len(mint_addresses)}")
    print(f"  Total addresses processed: {len(all_addresses)}")
    print(f"  Total signatures: {total_signatures}")
    print(f"  Total batches published: {total_batches}")
    if args.filter_existing:
        print(f"  Total filtered (existing): {total_filtered}")
    print(f"{'='*60}")

    return 0


if __name__ == '__main__':
    exit(main())
