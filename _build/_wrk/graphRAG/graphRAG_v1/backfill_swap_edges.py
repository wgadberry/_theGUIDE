#!/usr/bin/env python3
"""
backfill_swap_edges.py -- One-time ArangoDB backfill: reclassify spl_transfer → swap_in/swap_out

Reads transactions.raw_compressed from ArangoDB, re-derives swap edges using
the same logic as tx_detail_slim.py, and replaces misclassified spl_transfer
edges with properly typed swap_in/swap_out edges (including pool address).

The unique index on edges includes edge_type, so reclassification requires
DELETE old + INSERT new.

Usage:
    python backfill_swap_edges.py                  # dry-run (report only)
    python backfill_swap_edges.py --apply          # apply changes
    python backfill_swap_edges.py --apply --batch 500
"""

import argparse
import json
import sys
import time
import zlib
from graph_common import log, get_arango_db, ARANGO_HOST, ARANGO_DB, ARANGO_USER, ARANGO_PASS
from arango import ArangoClient

TAG = 'backfill-swap'


# ---------------------------------------------------------------------------
# Swap detection (mirrors tx_detail_slim.py lines 198-246)
# ---------------------------------------------------------------------------

def detect_swaps(slim):
    """Re-derive swap classification from stored slim JSON.

    Returns a list of edge reclassifications:
        [{'sig': str, 'from': str, 'to': str, 'token': str, 'amount': str,
          'old_type': 'spl_transfer', 'new_type': 'swap_in'|'swap_out',
          'pool': str|None, 'decimals': int, 'block_time': int}, ...]
    """
    signers = slim.get('signers', [])
    signer_set = set(signers)
    token_changes = slim.get('token_changes', [])
    edges = slim.get('edges', [])
    sig = slim.get('tx_hash', '')
    block_time = slim.get('block_time', 0)

    if not signers or not token_changes:
        return []

    # Group token changes by owner
    by_owner = {}
    for tc in token_changes:
        by_owner.setdefault(tc['owner'], []).append(tc)

    reclassifications = []

    for signer in signers:
        signer_tc = by_owner.get(signer, [])
        sent_mints = {tc['token'] for tc in signer_tc if tc['change_amount'] < 0}
        recv_mints = {tc['token'] for tc in signer_tc if tc['change_amount'] > 0}

        if not (sent_mints and recv_mints and sent_mints != recv_mints):
            continue

        # It's a swap — find pool for each sent mint
        for mint in sent_mints:
            pool = None
            for owner, tcs in by_owner.items():
                if owner == signer:
                    continue
                if any(tc['token'] == mint and tc['change_amount'] > 0 for tc in tcs):
                    pool = owner
                    break

            # Find matching spl_transfer edge: signer -> ? for this token
            for e in edges:
                if (e.get('transfer_type') == 'spl_transfer' and
                        e['from'] == signer and e['token'] == mint):
                    reclassifications.append({
                        'sig': sig,
                        'from': e['from'],
                        'to': e['to'],
                        'token': mint,
                        'amount': str(e['amount']),
                        'decimals': e.get('decimals', 0),
                        'block_time': block_time,
                        'old_type': 'spl_transfer',
                        'new_type': 'swap_in',
                        'pool': pool,
                    })

        for mint in recv_mints:
            pool = None
            for owner, tcs in by_owner.items():
                if owner == signer:
                    continue
                if any(tc['token'] == mint and tc['change_amount'] < 0 for tc in tcs):
                    pool = owner
                    break

            for e in edges:
                if (e.get('transfer_type') == 'spl_transfer' and
                        e['to'] == signer and e['token'] == mint):
                    reclassifications.append({
                        'sig': sig,
                        'from': e['from'],
                        'to': e['to'],
                        'token': mint,
                        'amount': str(e['amount']),
                        'decimals': e.get('decimals', 0),
                        'block_time': block_time,
                        'old_type': 'spl_transfer',
                        'new_type': 'swap_out',
                        'pool': pool,
                    })

    return reclassifications


# ---------------------------------------------------------------------------
# ArangoDB operations
# ---------------------------------------------------------------------------

def find_old_edge(db, sig, from_addr, to_addr, token_mint, edge_type='spl_transfer'):
    """Find the existing edge doc by unique index fields."""
    cursor = db.aql.execute(
        '''
        FOR e IN edges
            FILTER e.signature == @sig
               AND e._from == @from_addr
               AND e._to == @to_addr
               AND e.edge_type == @edge_type
               AND e.token == @token
            LIMIT 1
            RETURN e
        ''',
        bind_vars={
            'sig': sig,
            'from_addr': f'addresses/{from_addr}',
            'to_addr': f'addresses/{to_addr}',
            'edge_type': edge_type,
            'token': f'tokens/{token_mint}',
        }
    )
    docs = [doc for doc in cursor]
    return docs[0] if docs else None


def apply_reclassification(db, reclass):
    """Delete old spl_transfer edge, insert new swap_in/swap_out edge."""
    old_edge = find_old_edge(
        db, reclass['sig'], reclass['from'], reclass['to'], reclass['token'])

    if old_edge is None:
        return False  # edge already reclassified or missing

    # Build new edge doc (copy all fields, change edge_type + add pool)
    new_edge = {
        '_from': old_edge['_from'],
        '_to': old_edge['_to'],
        'edge_type': reclass['new_type'],
        'token': old_edge.get('token'),
        'amount': old_edge.get('amount'),
        'decimals': old_edge.get('decimals', 0),
        'signature': old_edge.get('signature'),
        'block_time': old_edge.get('block_time'),
        'program': old_edge.get('program'),
    }
    if reclass['pool']:
        new_edge['pool'] = f'addresses/{reclass["pool"]}'

    # Delete old, insert new
    db.collection('edges').delete(old_edge['_id'], silent=True)
    db.collection('edges').insert(new_edge, silent=True)
    return True


def apply_batch_aql(db, batch):
    """Batch reclassify: remove old spl_transfer edges, insert new swap edges.

    Processes each reclassification individually — ArangoDB doesn't support
    nested FOR-over-bind-array with REMOVE.
    """
    if not batch:
        return 0

    applied = 0
    new_edges = []

    for r in batch:
        # Delete the old spl_transfer edge
        cursor = db.aql.execute(
            '''
            FOR e IN edges
                FILTER e.signature == @sig
                   AND e._from == @from_addr
                   AND e._to == @to_addr
                   AND e.edge_type == 'spl_transfer'
                   AND e.token == @token
                LIMIT 1
                REMOVE e IN edges
                RETURN OLD._key
            ''',
            bind_vars={
                'sig': r['sig'],
                'from_addr': f'addresses/{r["from"]}',
                'to_addr': f'addresses/{r["to"]}',
                'token': f'tokens/{r["token"]}',
            }
        )
        deleted = [k for k in cursor]
        if not deleted:
            continue

        new_edge = {
            '_from': f'addresses/{r["from"]}',
            '_to': f'addresses/{r["to"]}',
            'edge_type': r['new_type'],
            'token': f'tokens/{r["token"]}',
            'amount': r['amount'],
            'decimals': r['decimals'],
            'signature': r['sig'],
            'block_time': r['block_time'],
        }
        if r['pool']:
            new_edge['pool'] = f'addresses/{r["pool"]}'
        new_edges.append(new_edge)
        applied += 1

    # Bulk insert all new swap edges
    if new_edges:
        db.collection('edges').insert_many(new_edges, silent=True, overwrite=False)

    return applied


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Backfill swap edges in ArangoDB')
    parser.add_argument('--apply', action='store_true', help='Apply changes (default: dry-run)')
    parser.add_argument('--batch', type=int, default=200, help='Transaction batch size (default: 200)')
    args = parser.parse_args()

    mode = 'APPLY' if args.apply else 'DRY-RUN'
    log(TAG, f'Mode: {mode}, batch_size={args.batch}')

    db = get_arango_db()

    # Count total transactions with raw_compressed
    cursor = db.aql.execute('RETURN LENGTH(FOR t IN transactions FILTER t.raw_compressed != null RETURN 1)')
    total = next(cursor, 0)
    log(TAG, f'Found {total} transactions with raw_compressed')

    # Stream through all transactions in batches
    offset = 0
    total_reclass = 0
    total_applied = 0
    total_skipped = 0
    swap_txs = 0

    while offset < total:
        cursor = db.aql.execute(
            '''
            FOR t IN transactions
                FILTER t.raw_compressed != null
                SORT t._key
                LIMIT @offset, @batch
                RETURN { _key: t._key, raw_compressed: t.raw_compressed }
            ''',
            bind_vars={'offset': offset, 'batch': args.batch}
        )

        batch_docs = [doc for doc in cursor]
        if not batch_docs:
            break

        batch_reclass = []

        for doc in batch_docs:
            try:
                compressed_hex = doc['raw_compressed']
                raw_bytes = zlib.decompress(bytes.fromhex(compressed_hex))
                slim = json.loads(raw_bytes)
            except Exception as e:
                log(TAG, f'  Skip {doc["_key"][:16]}... decompress error: {e}')
                total_skipped += 1
                continue

            reclassifications = detect_swaps(slim)
            if reclassifications:
                swap_txs += 1
                batch_reclass.extend(reclassifications)

        if batch_reclass:
            total_reclass += len(batch_reclass)
            if args.apply:
                applied = apply_batch_aql(db, batch_reclass)
                total_applied += applied
                log(TAG, f'  Batch {offset}-{offset+len(batch_docs)}: '
                    f'{len(batch_reclass)} reclassified, {applied} applied')
            else:
                for r in batch_reclass[:5]:  # show first 5 per batch
                    log(TAG, f'  {r["sig"][:16]}... {r["from"][:12]}→{r["to"][:12]} '
                        f'{r["old_type"]}→{r["new_type"]} pool={r["pool"][:12] if r["pool"] else "None"}')
                if len(batch_reclass) > 5:
                    log(TAG, f'  ... and {len(batch_reclass) - 5} more')

        offset += len(batch_docs)
        log(TAG, f'Progress: {offset}/{total} txs scanned, '
            f'{swap_txs} swap txs, {total_reclass} edges to reclassify')

    log(TAG, f'Done. {total} txs scanned, {total_skipped} skipped')
    log(TAG, f'{swap_txs} transactions contain swaps')
    log(TAG, f'{total_reclass} edges identified for reclassification')
    if args.apply:
        log(TAG, f'{total_applied} edges updated')
    else:
        log(TAG, f'Run with --apply to execute changes')


if __name__ == '__main__':
    main()
