"""
guide-load-mysql-ref.py -- Load reference data from MySQL into ArangoDB

Imports: tokens, programs, pools, templates, attributes (reference tables),
and enriches address types from tx_address.

Usage:
    python guide-load-mysql-ref.py
"""

import sys, json, hashlib, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import mysql.connector
from datetime import datetime
from graph_common import bootstrap_arango, log

TAG = 'load-ref'

with open('../guide-config.json') as f:
    _cfg = json.load(f)

DB_CONFIG = {
    'host': _cfg['DB_HOST'], 'port': _cfg['DB_PORT'],
    'user': _cfg['DB_USER'], 'password': _cfg['DB_PASSWORD'],
    'database': _cfg['DB_NAME'],
}

SERVICES_DB_CONFIG = {
    'host': '34.196.15.116', 'port': 3397,
    'user': 'root', 'password': 'rootpassword',
    'database': 't16o_db',
}


def main():
    db = bootstrap_arango()

    # Use services DB for enrichment data (templates, programs, pools, tokens)
    log(TAG, 'Connecting to MySQL services DB...')
    svc_conn = mysql.connector.connect(**SERVICES_DB_CONFIG, charset='utf8mb4')
    svc_cur = svc_conn.cursor(dictionary=True)

    # Also connect to local/config DB for address types
    log(TAG, 'Connecting to MySQL config DB...')
    cfg_conn = mysql.connector.connect(**DB_CONFIG, charset='utf8mb4')
    cfg_cur = cfg_conn.cursor(dictionary=True)

    # ------------------------------------------------------------------
    # 1. Tokens (tx_token -> tokens collection)
    # ------------------------------------------------------------------
    log(TAG, 'Loading tokens...')
    svc_cur.execute('''
        SELECT a.address as mint, t.token_symbol, t.token_name, t.decimals,
               t.token_type, t.supply
        FROM tx_token t
        JOIN tx_address a ON a.id = t.mint_address_id
        WHERE a.address IS NOT NULL
    ''')
    token_docs = []
    for r in svc_cur.fetchall():
        doc = {'_key': r['mint'], 'decimals': r['decimals'] or 0}
        if r['token_symbol']: doc['symbol'] = r['token_symbol']
        if r['token_name']: doc['name'] = r['token_name']
        if r['token_type']: doc['token_type'] = r['token_type']
        if r['supply']: doc['supply'] = str(r['supply'])
        token_docs.append(doc)

    if not db.has_collection('tokens'):
        db.create_collection('tokens')
    db.collection('tokens').import_bulk(token_docs, on_duplicate='update', halt_on_error=False)
    log(TAG, f'  Tokens: {len(token_docs)} loaded')

    # ------------------------------------------------------------------
    # 2. Programs (tx_program -> programs + addresses collections)
    # ------------------------------------------------------------------
    log(TAG, 'Loading programs...')
    svc_cur.execute('''
        SELECT a.address as program_addr, a.label as program_label,
               p.name, p.program_type, p.is_verified
        FROM tx_program p
        JOIN tx_address a ON a.id = p.program_address_id
        WHERE a.address IS NOT NULL
    ''')
    prog_rows = svc_cur.fetchall()

    addr_updates = []
    prog_docs = []
    for r in prog_rows:
        addr_updates.append({
            '_key': r['program_addr'],
            'type': 'program',
            'label': r['program_label'] or r['name'],
        })
        doc = {
            '_key': r['program_addr'],
            'name': r['name'],
            'program_type': r['program_type'],
            'is_verified': bool(r['is_verified']),
        }
        if r['program_label']: doc['label'] = r['program_label']
        prog_docs.append(doc)

    db.collection('addresses').import_bulk(addr_updates, on_duplicate='update', halt_on_error=False)

    if not db.has_collection('programs'):
        db.create_collection('programs')
    db.collection('programs').import_bulk(prog_docs, on_duplicate='update', halt_on_error=False)
    log(TAG, f'  Programs: {len(prog_docs)} loaded')

    # ------------------------------------------------------------------
    # 3. Pools (tx_pool -> pools + addresses collections)
    # ------------------------------------------------------------------
    log(TAG, 'Loading pools...')
    svc_cur.execute('''
        SELECT a.address as pool_addr, a.label as pool_label,
               p.dex, p.token_pair, p.pool_label as pool_name,
               t1a.address as token1_mint, t2a.address as token2_mint,
               prog_a.address as program_addr
        FROM tx_pool p
        JOIN tx_address a ON a.id = p.pool_address_id
        LEFT JOIN tx_token t1 ON t1.id = p.token1_id
        LEFT JOIN tx_address t1a ON t1a.id = t1.mint_address_id
        LEFT JOIN tx_token t2 ON t2.id = p.token2_id
        LEFT JOIN tx_address t2a ON t2a.id = t2.mint_address_id
        LEFT JOIN tx_program prog ON prog.id = p.program_id
        LEFT JOIN tx_address prog_a ON prog_a.id = prog.program_address_id
        WHERE a.address IS NOT NULL
    ''')
    pool_rows = svc_cur.fetchall()

    addr_updates = []
    pool_docs = []
    for r in pool_rows:
        addr_updates.append({
            '_key': r['pool_addr'],
            'type': 'pool',
            'label': r['pool_label'] or r['pool_name'],
        })
        doc = {'_key': r['pool_addr']}
        if r['dex']: doc['dex'] = r['dex']
        if r['token_pair']: doc['token_pair'] = r['token_pair']
        if r['pool_name']: doc['label'] = r['pool_name']
        if r['pool_label']: doc['label'] = r['pool_label']
        if r['token1_mint']: doc['token1'] = r['token1_mint']
        if r['token2_mint']: doc['token2'] = r['token2_mint']
        if r['program_addr']: doc['program'] = r['program_addr']
        pool_docs.append(doc)

    db.collection('addresses').import_bulk(addr_updates, on_duplicate='update', halt_on_error=False)

    if not db.has_collection('pools'):
        db.create_collection('pools')
    db.collection('pools').import_bulk(pool_docs, on_duplicate='update', halt_on_error=False)
    log(TAG, f'  Pools: {len(pool_docs)} loaded')

    # ------------------------------------------------------------------
    # 4. Pool edges (trades_token, runs_on)
    # ------------------------------------------------------------------
    log(TAG, 'Building pool edges...')
    if not db.has_collection('pool_edges'):
        db.create_collection('pool_edges', edge=True)

    pool_edges = []
    for pool in db.aql.execute('FOR p IN pools RETURN p'):
        pk = pool['_key']
        if pool.get('token1'):
            pool_edges.append({
                '_from': f'addresses/{pk}',
                '_to': f'addresses/{pool["token1"]}',
                'edge_type': 'trades_token',
                'position': 'token1',
            })
        if pool.get('token2'):
            pool_edges.append({
                '_from': f'addresses/{pk}',
                '_to': f'addresses/{pool["token2"]}',
                'edge_type': 'trades_token',
                'position': 'token2',
            })
        if pool.get('program'):
            pool_edges.append({
                '_from': f'addresses/{pk}',
                '_to': f'addresses/{pool["program"]}',
                'edge_type': 'runs_on',
            })

    # Ensure referenced addresses exist
    addr_keys = set()
    for e in pool_edges:
        addr_keys.add(e['_from'].split('/')[1])
        addr_keys.add(e['_to'].split('/')[1])

    existing = set(r['_key'] for r in db.aql.execute('FOR a IN addresses RETURN {_key: a._key}'))
    missing = addr_keys - existing
    if missing:
        db.collection('addresses').insert_many(
            [{'_key': k, 'type': 'unknown'} for k in missing],
            silent=True, overwrite=False)

    db.collection('pool_edges').insert_many(pool_edges, silent=True, overwrite=False)

    # Add to graph if not already
    graph = db.graph('t16o_graph')
    edge_defs = [ed['edge_collection'] for ed in graph.edge_definitions()]
    if 'pool_edges' not in edge_defs:
        graph.create_edge_definition(
            edge_collection='pool_edges',
            from_vertex_collections=['addresses'],
            to_vertex_collections=['addresses'],
        )
    log(TAG, f'  Pool edges: {len(pool_edges)} (trades_token + runs_on)')

    # ------------------------------------------------------------------
    # 5. Templates (tx_guide_template -> templates collection)
    # ------------------------------------------------------------------
    log(TAG, 'Loading templates...')
    svc_cur.execute('SELECT * FROM tx_guide_template WHERE use_template = 1')
    tmpl_rows = svc_cur.fetchall()

    tmpl_docs = []
    for r in tmpl_rows:
        emb = json.loads(r['embedding']) if isinstance(r['embedding'], str) else r['embedding']
        tmpl_docs.append({
            '_key': str(r['id']),
            'pattern_hash': r['pattern_hash'],
            'edge_type_code': r['edge_type_code'],
            'edge_category': r['edge_category'],
            'edge_direction': r['edge_direction'],
            'from_address_type': r['from_address_type'],
            'to_address_type': r['to_address_type'],
            'has_pool': bool(r['has_pool']),
            'has_tax': bool(r['has_tax']),
            'swap_direction': r['swap_direction'],
            'balance_signal': r['balance_signal'],
            'amount_magnitude': r['amount_magnitude'],
            'label_signal': r['label_signal'],
            'narrative': r['narrative'],
            'embedding': emb,
            'row_count': r['row_count'],
            'pct_of_total': float(r['pct_of_total']) if r['pct_of_total'] else 0,
            'cumulative_pct': float(r['cumulative_pct']) if r['cumulative_pct'] else 0,
        })

    if not db.has_collection('templates'):
        db.create_collection('templates')
    db.collection('templates').import_bulk(tmpl_docs, on_duplicate='replace', halt_on_error=False)
    log(TAG, f'  Templates: {len(tmpl_docs)} loaded')

    # ------------------------------------------------------------------
    # 6. Attributes (small reference tables -> single collection)
    # ------------------------------------------------------------------
    log(TAG, 'Loading attributes...')
    if not db.has_collection('attributes'):
        db.create_collection('attributes')

    ref_tables = [
        'tx_guide_type', 'tx_guide_source', 'tx_activity_type_map',
        'tx_state_phase', 'tx_boundary', 'tx_feature_config', 'config',
    ]
    for table in ref_tables:
        svc_cur.execute(f'SELECT * FROM {table}')
        rows = svc_cur.fetchall()
        clean_rows = []
        for r in rows:
            clean = {}
            for k, v in r.items():
                if hasattr(v, 'isoformat'): clean[k] = v.isoformat()
                elif isinstance(v, bytes): clean[k] = v.decode('utf-8', errors='replace')
                elif isinstance(v, set): clean[k] = list(v)
                else: clean[k] = v
            clean_rows.append(clean)
        db.collection('attributes').insert(
            {'_key': table, 'source': 'mysql', 'row_count': len(clean_rows), 'data': clean_rows},
            overwrite=True, silent=True)
        log(TAG, f'  {table}: {len(clean_rows)} rows')

    # ------------------------------------------------------------------
    # 7. Enrich address types from tx_address
    # ------------------------------------------------------------------
    log(TAG, 'Enriching address types...')
    svc_cur.execute('SELECT address, address_type FROM tx_address WHERE address_type != "unknown"')
    addr_rows = svc_cur.fetchall()
    updates = [{'_key': r['address'], 'type': r['address_type']} for r in addr_rows]

    db.aql.execute('''
        FOR doc IN @docs
            LET existing = DOCUMENT(CONCAT("addresses/", doc._key))
            FILTER existing != null AND existing.type == "unknown"
            UPDATE { _key: doc._key, type: doc.type } IN addresses
    ''', bind_vars={'docs': updates})
    log(TAG, f'  Address types: {len(updates)} candidates')

    svc_cur.close()
    svc_conn.close()
    cfg_cur.close()
    cfg_conn.close()

    # Summary
    log(TAG, '=== Summary ===')
    for name in ['addresses', 'tokens', 'programs', 'pools', 'templates',
                 'attributes', 'edges', 'pool_edges', 'transactions']:
        if db.has_collection(name):
            log(TAG, f'  {name:15s} {db.collection(name).count():>10,}')


if __name__ == '__main__':
    main()
