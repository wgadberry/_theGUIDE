"""
guide-cons-addr-enrich.py -- Enrich addresses from Solscan account/metadata/multi

Consumes batches of 50 addresses from Redis queue, fetches metadata
from Solscan (funded_by, label, tags, type), and updates ArangoDB.

Also creates funded_by edges in the graph.

Usage:
    python guide-cons-addr-enrich.py [--threads 4]
"""

import argparse, sys, time, json, signal, threading
import redis as redis_lib
import requests
from graph_common import log, bootstrap_arango, get_arango_db, SOLSCAN_API, SOLSCAN_TOKEN

TAG = 'cons-enrich'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_QUEUE = 't16o:addr:enrich'
RUNNING = True


def shutdown(signum, frame):
    global RUNNING
    RUNNING = False
    log('supervisor', 'Shutdown signal received')


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


def fetch_metadata(session, addresses):
    """Fetch account metadata from Solscan for up to 50 addresses."""
    if not addresses:
        return {}

    params = '&'.join(f'address[]={a}' for a in addresses)
    try:
        r = session.get(f'{SOLSCAN_API}/account/metadata/multi?{params}',
                        headers={'token': SOLSCAN_TOKEN}, timeout=30)
        r.raise_for_status()
        results = {}
        for item in (r.json().get('data') or []):
            addr = item.get('account_address', '')
            if addr:
                results[addr] = item
        return results
    except Exception as e:
        log(TAG, f'Solscan error: {e}')
        return {}


# Type hierarchy — higher rank = more specific, never downgrade
TYPE_RANK = {
    'unknown': 0, 'wallet': 1, 'ata': 2, 'mint': 3, 'program': 4,
    'market': 5, 'dex_wallet': 5, 'lp_token': 6, 'pool': 7,
    'vault': 8, 'bot': 9,
}


def classify_address_type(label, tags, account_type):
    """Derive address_type from Solscan metadata (ported from guide-funder.py)."""
    import re
    tags_lower = [t.lower() for t in tags] if tags else []
    label_lower = (label or '').lower()

    if ('vault' in label_lower
            and not re.search(r'\([^)]*vault[^)]*\)', label_lower)):
        return 'vault'
    if 'pool' in tags_lower:
        return 'pool'
    if 'market' in tags_lower:
        return 'market'
    if any('vault' in tag for tag in tags_lower):
        return 'vault'
    if 'lptoken' in tags_lower:
        return 'lp_token'
    if 'dex_wallet' in tags_lower:
        return 'dex_wallet'
    if 'fee_vault' in tags_lower:
        return 'vault'
    if 'arbitrage_bot' in tags_lower:
        return 'bot'
    if 'token_creator' in tags_lower:
        return 'wallet'
    if 'memecoin' in tags_lower:
        return 'mint'
    if account_type == 'token_account':
        return 'ata'
    if account_type == 'mint':
        return 'mint'
    if account_type == 'address':
        return 'wallet'
    return None


def should_upgrade_type(current_type, new_type):
    """Only upgrade to a more specific type, never downgrade."""
    return TYPE_RANK.get(new_type, 0) > TYPE_RANK.get(current_type, 0)


def process_batch(tag, db, session, addresses):
    """Fetch metadata and update ArangoDB addresses + create funded_by edges.

    Ported from guide-funder.py: classifies address types, tracks attempt_cnt,
    only upgrades types (never downgrades), creates pool docs for pool addresses.
    """
    if not addresses:
        return 0, 0

    metadata = fetch_metadata(session, addresses)
    if not metadata:
        # Increment attempt_cnt for addresses that failed API fetch — batch read
        now = int(time.time())
        current = {}
        try:
            for doc in db.collection('addresses').get_many(addresses):
                if doc:
                    current[doc['_key']] = (doc.get('attempt_cnt') or 0)
        except Exception:
            pass
        failed_updates = [{
            '_key': addr,
            'enriched_at': now,
            'attempt_cnt': current.get(addr, 0) + 1,
        } for addr in addresses]
        db.collection('addresses').insert_many(
            failed_updates, overwrite_mode='update', merge=True, silent=True)
        return 0, len(addresses)

    addr_updates = []
    funded_edges = []
    pool_docs = []

    # Pre-fetch current docs so we have both type and attempt_cnt
    current_docs = {}   # addr -> {'type': str, 'attempt_cnt': int}
    try:
        for doc in db.collection('addresses').get_many(addresses):
            if doc:
                current_docs[doc['_key']] = {
                    'type': doc.get('type', 'unknown'),
                    'attempt_cnt': doc.get('attempt_cnt') or 0,
                }
    except Exception:
        pass

    # Convenience: current type string, falls back to 'unknown'
    def cur_type_for(addr):
        return current_docs.get(addr, {}).get('type', 'unknown')

    def cur_attempt_cnt(addr):
        return current_docs.get(addr, {}).get('attempt_cnt', 0)

    for addr in addresses:
        meta = metadata.get(addr)
        now = int(time.time())

        if not meta:
            addr_updates.append({
                '_key': addr,
                'funded_by': 'none',
                'enriched': 1,
                'enriched_at': now,
                'attempt_cnt': cur_attempt_cnt(addr) + 1,
            })
            continue

        update = {
            '_key': addr,
            'enriched': 1,
            'enriched_at': now,
        }

        # Classify address type from metadata
        label = meta.get('account_label')
        tags = meta.get('account_tags')
        account_type = meta.get('account_type')

        if label:
            update['label'] = label
        if tags:
            update['tags'] = tags
        if account_type:
            update['account_type'] = account_type

        mapped_type = classify_address_type(label, tags, account_type)
        cur_type = cur_type_for(addr)
        if mapped_type and should_upgrade_type(cur_type, mapped_type):
            update['type'] = mapped_type

        # Active age
        age = meta.get('active_age')
        if age is not None:
            update['active_age'] = age

        # Funded by
        fb = meta.get('funded_by')
        if fb and isinstance(fb, dict) and fb.get('funded_by'):
            funder = fb['funded_by']
            update['funded_by'] = funder
            update['funded_by_tx'] = fb.get('tx_hash', '')
            update['funded_by_time'] = fb.get('block_time', 0)

            # Create funded_by edge
            funded_edges.append({
                '_from': f'addresses/{funder}',
                '_to': f'addresses/{addr}',
                'edge_type': 'funded_by',
                'signature': fb.get('tx_hash', ''),
                'block_time': fb.get('block_time', 0),
            })

            # Ensure funder address exists — no is_funder flag here to prevent
            # cascade. Only v5 process_holder sets is_funder on direct holder funders.
            # The enricher's discovered funders go through normal 2-edge threshold.
            try:
                db.collection('addresses').insert({
                    '_key': funder,
                    'type': 'wallet',
                    'first_seen': fb.get('block_time', 0),
                    'last_seen': fb.get('block_time', 0),
                    'enriched': 0,
                }, silent=True)
            except Exception:
                pass  # already exists — don't touch it
        else:
            update['funded_by'] = 'none'

        # Create pool doc if address is a pool
        effective_type = update.get('type', cur_type)
        if effective_type in ('pool', 'lp_token'):
            pool_docs.append({
                '_key': addr,
                'label': label,
                'type': effective_type,
            })

        addr_updates.append(update)

    # Batch update addresses
    if addr_updates:
        db.collection('addresses').insert_many(
            addr_updates, overwrite_mode='update', merge=True, silent=True)

    # Insert funded_by edges
    if funded_edges:
        db.collection('edges').insert_many(funded_edges, silent=True, overwrite=False)

    # Upsert pool docs
    if pool_docs:
        try:
            if not db.has_collection('pools'):
                db.create_collection('pools')
            db.collection('pools').insert_many(
                pool_docs, overwrite_mode='update', merge=True, silent=True)
        except Exception:
            pass

    enriched = sum(1 for a in addresses if a in metadata)
    return enriched, len(addresses) - enriched


class WorkerThread(threading.Thread):
    def __init__(self, worker_id):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.tag = f'cons-enrich-{worker_id}'
        self.total_enriched = 0
        self.total_failed = 0

    def run(self):
        db = get_arango_db()
        rdb = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        session = requests.Session()

        log(self.tag, 'Started')

        while RUNNING:
            try:
                result = rdb.brpop(REDIS_QUEUE, timeout=5)
                if result is None:
                    continue

                _, key = result
                raw = rdb.get(key)
                if raw is None:
                    continue

                payload = json.loads(raw)
                addresses = payload.get('addresses', [])
                rdb.delete(key)

                if not addresses:
                    continue

                enriched, failed = process_batch(self.tag, db, session, addresses)
                self.total_enriched += enriched
                self.total_failed += failed

                log(self.tag, f'{enriched} enriched, {failed} failed '
                    f'(total: {self.total_enriched})')

            except Exception as e:
                log(self.tag, f'Error: {e}')
                time.sleep(2)

        log(self.tag, f'Stopped. {self.total_enriched} enriched, '
            f'{self.total_failed} failed')


SWEEP_INTERVAL = 10     # seconds between self-feed sweeps
SWEEP_BATCH = 5000      # max addresses per sweep
SWEEP_MSG_SIZE = 50     # addresses per Redis message


def self_feed_sweep(db, rdb):
    """Check for unenriched addresses and queue qualifying ones.

    Two-pass approach:
      1. Find unenriched addresses with 4+ edges → queue for enrichment
      2. Mark remaining unenriched addresses with <4 edges as skipped (-1)
         so they never appear in future sweeps
    """
    queue_depth = rdb.llen(REDIS_QUEUE)
    if queue_depth > 0:
        return 0  # workers are still busy

    SKIP_RETRY_DAYS = 30  # re-evaluate skipped addresses after 30 days

    try:
        # Pass 1: Find new candidates with 4+ edges
        # Also re-evaluate skipped (-1) addresses older than 30 days
        cutoff = int(time.time()) - (SKIP_RETRY_DAYS * 86400)
        addrs = list(db.aql.execute('''
            FOR a IN addresses
                FILTER (a.enriched == 0 OR a.enriched == null)
                    OR (a.enriched == -1 AND (a.enriched_at || 0) < @cutoff)
                FILTER a.type IN ["wallet", "unknown", "pool"]
                LET is_funder = (a.is_funder == true)
                LET edge_cnt = is_funder ? 2 : LENGTH(
                    FOR e IN edges
                        FILTER e._from == CONCAT("addresses/", a._key)
                            OR e._to == CONCAT("addresses/", a._key)
                        LIMIT 2
                        RETURN 1
                )
                FILTER edge_cnt >= 2
                LIMIT @limit
                RETURN a._key
        ''', bind_vars={'limit': SWEEP_BATCH, 'cutoff': cutoff}))
    except Exception as e:
        log('sweep', f'AQL error: {e}')
        return 0

    # Pass 2: Mark low-edge addresses as skipped (-1) with timestamp
    if not addrs:
        try:
            now = int(time.time())
            skipped = db.aql.execute('''
                FOR a IN addresses
                    FILTER a.enriched == 0 OR a.enriched == null
                    FILTER a.type IN ["wallet", "unknown", "pool"]
                    LIMIT @limit
                    UPDATE a WITH { enriched: -1, enriched_at: @now } IN addresses
                    RETURN 1
            ''', bind_vars={'limit': SWEEP_BATCH, 'now': now}).statistics()
            writes = skipped.get('writesExecuted', 0)
            if writes > 0:
                log('sweep', f'Marked {writes} low-edge addresses as skipped (-1)')
        except Exception as e:
            log('sweep', f'Skip-mark error: {e}')
        return 0

    import uuid
    queued = 0
    for i in range(0, len(addrs), SWEEP_MSG_SIZE):
        chunk = addrs[i:i + SWEEP_MSG_SIZE]
        key = f't16o:sigs:enrich:{uuid.uuid4().hex[:12]}'
        rdb.set(key, json.dumps({
            'addresses': chunk,
            'count': len(chunk),
            'created_at': int(time.time()),
        }))
        rdb.lpush(REDIS_QUEUE, key)
        queued += len(chunk)

    log('sweep', f'Self-feed: queued {queued} addresses ({len(addrs)} found)')
    return queued


def main():
    global RUNNING
    parser = argparse.ArgumentParser(description='Address enrichment consumer')
    parser.add_argument('--threads', type=int, default=4, help='Worker thread count (default: 4)')
    parser.add_argument('--sweep-interval', type=int, default=SWEEP_INTERVAL,
                        help=f'Seconds between self-feed sweeps (default: {SWEEP_INTERVAL})')
    args = parser.parse_args()

    thread_count = args.threads

    log('supervisor', f'Starting {thread_count} enrichment workers (sweep every {args.sweep_interval}s)')
    db = bootstrap_arango()
    rdb = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    workers = []
    for wid in range(1, thread_count + 1):
        w = WorkerThread(wid)
        w.start()
        workers.append(w)

    log('supervisor', f'{thread_count} workers running, waiting for work...')

    last_sweep = 0
    try:
        while RUNNING:
            time.sleep(1)
            now = time.time()
            if now - last_sweep >= args.sweep_interval:
                # Keep sweeping until no more unenriched addresses
                while RUNNING:
                    queued = self_feed_sweep(db, rdb)
                    if queued == 0:
                        break
                    # Wait for workers to drain before queuing more
                    while RUNNING and rdb.llen(REDIS_QUEUE) > 50:
                        time.sleep(2)
                last_sweep = time.time()
    except KeyboardInterrupt:
        pass

    log('supervisor', 'Shutting down...')
    RUNNING = False

    for w in workers:
        w.join(timeout=10)

    total_e = sum(w.total_enriched for w in workers)
    total_f = sum(w.total_failed for w in workers)
    log('supervisor', f'Done. {total_e} enriched, {total_f} failed')


if __name__ == '__main__':
    main()
