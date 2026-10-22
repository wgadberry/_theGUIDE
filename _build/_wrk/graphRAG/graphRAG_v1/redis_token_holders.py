#!/usr/bin/env python3
"""
redis_token_holders.py -- Generate top-holder fingerprints for all tokens

For each token in ArangoDB:
  1. Fetch top 80 holders from Solscan /token/holders
  2. Sort by amount descending
  3. Concatenate address+amount for each holder
  4. SHA-256 hash the concatenated string
  5. Store as Redis hash: token:<mint_address> { topHolderHash: <sha256> }

Usage:
    python redis_token_holders.py                   # process all tokens
    python redis_token_holders.py --limit 100       # first 100 tokens
    python redis_token_holders.py --mint <address>  # single token
"""

import argparse, hashlib, json, sys, time
from datetime import datetime
from arango import ArangoClient
import redis as redis_lib
import requests

ARANGO_HOST = 'http://localhost:8529'
ARANGO_DB = 't16o'
ARANGO_USER = 'root'
ARANGO_PASS = 'localdev'

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

SOLSCAN_API = 'https://pro-api.solscan.io/v2.0'

# Load Solscan token from config
import os
_dir = os.path.dirname(os.path.abspath(__file__))
_config_path = os.path.join(os.path.dirname(_dir), 'guide-config.json')
with open(_config_path) as f:
    _cfg = json.load(f)
SOLSCAN_TOKEN = _cfg.get('SOLSCAN_TOKEN', '')


def log(msg):
    print(f'[{datetime.now().strftime("%H:%M:%S")}] {msg}', flush=True)


def fetch_top_holders(session, mint, limit=80):
    """Fetch top holders from Solscan, sorted by amount descending."""
    holders = []
    for page in range(1, (limit // 40) + 2):
        try:
            r = session.get(
                f'{SOLSCAN_API}/token/holders',
                params={'address': mint, 'page': page, 'page_size': 40},
                headers={'token': SOLSCAN_TOKEN},
                timeout=30,
            )
            r.raise_for_status()
            data = r.json().get('data', {})
            items = data.get('items', [])
            if not items:
                break
            holders.extend(items)
            if len(holders) >= limit or len(holders) >= (data.get('total', 0)):
                break
        except Exception as e:
            log(f'  Solscan error: {e}')
            break
        time.sleep(0.3)

    # Sort by amount descending (largest holders first)
    holders = sorted(holders, key=lambda h: int(h.get('amount', 0)), reverse=True)
    return holders[:limit]


def compute_holder_hash(holders):
    """SHA-256 of concatenated holder addresses in rank order.

    Only hashes addresses, not amounts — amounts change every block
    but the holder roster changes slowly.
    """
    parts = [h.get('owner', '') for h in holders]
    return hashlib.sha256(''.join(parts).encode('utf-8')).hexdigest()


def process_token(rdb, session, mint, symbol='', name=''):
    """Fetch holders, compute hash, store in Redis."""
    holders = fetch_top_holders(session, mint, limit=80)
    if not holders:
        return None

    holder_hash = compute_holder_hash(holders)

    key = f'token:{mint}'
    rdb.hset(key, mapping={
        'topHolderHash': holder_hash,
        'holderCount': len(holders),
        'topHolderAddress': holders[0].get('owner', '') if holders else '',
        'topHolderPct': str(holders[0].get('percentage', 0)) if holders else '0',
        'token_symbol': symbol or '',
        'token_name': name or '',
        'updatedAt': int(time.time()),
    })

    return holder_hash


def main():
    parser = argparse.ArgumentParser(description='Generate top-holder fingerprints')
    parser.add_argument('--limit', type=int, default=0, help='Max tokens to process, 0=all (default: 0)')
    parser.add_argument('--mint', type=str, help='Process a single token mint address')
    parser.add_argument('--batch', type=int, default=100, help='AQL batch size (default: 100)')
    args = parser.parse_args()

    rdb = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    rdb.ping()

    session = requests.Session()

    # Single token mode
    if args.mint:
        log(f'Processing single token: {args.mint}')
        db = ArangoClient(hosts=ARANGO_HOST).db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASS)
        doc = db.collection('tokens').get(args.mint) or {}
        h = process_token(rdb, session, args.mint, symbol=doc.get('symbol', ''), name=doc.get('name', ''))
        if h:
            log(f'  topHolderHash: {h}')
            log(f'  Stored as token:{args.mint}')
        else:
            log(f'  No holders found')
        return

    # Batch mode — get top tokens by edge count from ArangoDB, with metadata
    db = ArangoClient(hosts=ARANGO_HOST).db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASS)
    total_tokens = db.collection('tokens').count()
    target = min(args.limit, total_tokens) if args.limit > 0 else total_tokens
    log(f'Tokens in ArangoDB: {total_tokens}  Fetching top {target} by edge count...')

    token_rows = list(db.aql.execute(
        '''
        FOR e IN edges
            FILTER e.token != null
            COLLECT token = e.token WITH COUNT INTO cnt
            SORT cnt DESC
            LIMIT @limit
            LET mint = SUBSTRING(token, 7)
            LET doc = DOCUMENT(token)
            RETURN { mint, symbol: doc.symbol, name: doc.name, edge_count: cnt }
        ''',
        bind_vars={'limit': target},
    ))
    # Build lookup: mint -> {symbol, name}
    token_meta = {r['mint']: r for r in token_rows}
    mints = [r['mint'] for r in token_rows]
    log(f'Found {len(mints)} tokens to process')

    processed = 0
    failed = 0
    t0 = time.time()

    for i, mint in enumerate(mints):
        try:
            meta = token_meta.get(mint, {})
            h = process_token(rdb, session, mint, symbol=meta.get('symbol', ''), name=meta.get('name', ''))
            if h:
                processed += 1
            else:
                failed += 1
        except Exception as e:
            log(f'  ERROR {mint[:16]}...: {e}')
            failed += 1

        if (processed + failed) % 50 == 0 and (processed + failed) > 0:
            elapsed = time.time() - t0
            rate = processed / elapsed if elapsed > 0 else 0
            log(f'  [{processed + failed}/{len(mints)}] processed={processed} failed={failed} ({rate:.1f}/sec)')

        time.sleep(0.1)  # pace Solscan API calls

    elapsed = time.time() - t0
    log(f'Done: {processed} processed, {failed} failed in {elapsed:.1f}s')
    log(f'Redis keys: token:* = {processed} hashes')


if __name__ == '__main__':
    main()
