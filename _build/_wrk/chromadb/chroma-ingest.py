#!/usr/bin/env python3
"""
ChromaDB Ingestion Script — Embeds tx_guide edges as natural language sentences.

Usage:
    python chroma-ingest.py --full          # Ingest all tx_guide rows
    python chroma-ingest.py --incremental   # Ingest only new rows since last run
    python chroma-ingest.py --reset         # Delete collection and re-ingest

Connects to MySQL via guide-config.json (same as all workers).
Connects to ChromaDB at CHROMADB_HOST:CHROMADB_PORT from config (default localhost:8000).
"""

import argparse
import os
import sys
import hashlib
import time
from datetime import datetime, timezone

# Add parent paths for t16o_exchange imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import mysql.connector
import chromadb

try:
    from t16o_exchange.guide.common.config import get_db_config, load_config
    _cfg = load_config()
except ImportError:
    _cfg = {}
    def get_db_config(**kwargs):
        return {
            'host': os.environ.get('DB_HOST', '34.196.15.116'),
            'port': int(os.environ.get('DB_PORT', '3397')),
            'user': os.environ.get('DB_USER', 'root'),
            'password': os.environ.get('DB_PASSWORD', 'rootpassword'),
            'database': os.environ.get('DB_NAME', 't16o_db'),
            'autocommit': True,
        }

# ── Config ──
CHROMADB_HOST = _cfg.get('CHROMADB_HOST', os.environ.get('CHROMADB_HOST', 'localhost'))
CHROMADB_PORT = int(_cfg.get('CHROMADB_PORT', os.environ.get('CHROMADB_PORT', '8000')))
COLLECTION_NAME = 'tx_guide_edges'
BATCH_SIZE = 5000  # Rows per ChromaDB upsert batch
MYSQL_BATCH = 5000  # Rows per MySQL fetch
EMBED_BATCH = 256   # SentenceTransformer encode batch size

# ── SQL ──
INGEST_QUERY = """
SELECT
    g.id AS guide_id,
    t.signature AS tx_signature,
    g.tx_id,
    g.block_time,
    fa.address AS from_address,
    fa.label AS from_label,
    fa.address_type AS from_address_type,
    ta.address AS to_address,
    ta.label AS to_label,
    ta.address_type AS to_address_type,
    tk.token_symbol,
    tk.token_name,
    mint_addr.address AS mint_address,
    gt.type_code AS edge_type,
    gt.type_name AS edge_type_name,
    gt.category AS edge_category,
    g.amount AS amount_raw,
    g.decimals,
    p.dex,
    p.pool_label,
    g.swap_direction,
    pa.address AS pool_address,
    g.tax_bps,
    g.tax_amount
FROM tx_guide g
JOIN tx t ON t.id = g.tx_id
JOIN tx_address fa ON fa.id = g.from_address_id
JOIN tx_address ta ON ta.id = g.to_address_id
JOIN tx_guide_type gt ON gt.id = g.edge_type_id
LEFT JOIN tx_token tk ON tk.id = g.token_id
LEFT JOIN tx_address mint_addr ON mint_addr.id = tk.mint_address_id
LEFT JOIN tx_pool p ON p.id = g.pool_id
LEFT JOIN tx_address pa ON pa.id = p.pool_address_id
WHERE g.id > %s
ORDER BY g.id
LIMIT %s
"""

# ── Sentence Generation ──
ACTION_VERBS = {
    'swap_out': 'swapped',
    'swap_in': 'received',
    'spl_transfer': 'transferred',
    'sol_transfer': 'sent SOL to',
    'wallet_funded': 'was funded by',
    'fee': 'paid fee to',
    'priority_fee': 'paid priority fee to',
    'protocol_fee': 'paid protocol fee to',
    'create_ata': 'created token account for',
    'close_ata': 'closed token account at',
    'lend_deposit': 'deposited into',
    'lend_withdraw': 'withdrew from',
    'borrow': 'borrowed from',
    'repay': 'repaid to',
    'liquidation': 'was liquidated by',
    'stake': 'staked with',
    'unstake': 'unstaked from',
    'stake_reward': 'received stake reward from',
    'add_liquidity': 'added liquidity to',
    'remove_liquidity': 'removed liquidity from',
    'lp_reward': 'received LP reward from',
    'farm_deposit': 'deposited into farm',
    'farm_withdraw': 'withdrew from farm',
    'bridge_out': 'bridged out to',
    'bridge_in': 'bridged in from',
    'mint': 'minted',
    'burn': 'burned',
    'airdrop': 'airdropped to',
    'nft_transfer': 'transferred NFT to',
    'nft_sale': 'sold NFT to',
    'nft_mint': 'minted NFT',
    'unknown': 'interacted with',
}


def block_time_to_utc(block_time):
    """Convert unix timestamp to ISO-ish UTC string."""
    if not block_time:
        return ''
    return datetime.fromtimestamp(int(block_time), tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def short_addr(addr):
    if not addr:
        return 'unknown'
    return addr[:5] + '...' + addr[-4:]


def resolve_label(address, label, address_type):
    """Build a human-readable identifier for an address."""
    type_prefix = (address_type or 'wallet').capitalize()
    if type_prefix in ('Wallet', 'Unknown'):
        type_prefix = 'Wallet'
    if label and label not in ('wallet', 'unknown', 'ata', ''):
        return f"{type_prefix} {label}"
    return f"{type_prefix} {short_addr(address)}"


def format_amount(raw_amount, decimals):
    if raw_amount is None or decimals is None:
        return ''
    human = raw_amount / (10 ** decimals)
    if human >= 1_000_000:
        return f"{human / 1_000_000:,.2f}M"
    if human >= 1_000:
        return f"{human:,.0f}"
    if human >= 1:
        return f"{human:,.2f}"
    if human == 0:
        return '0'
    return f"{human:.6f}"


def build_sentence(row):
    """Transform a resolved tx_guide row into a natural language sentence."""
    from_label = resolve_label(row['from_address'], row['from_label'], row['from_address_type'])
    to_label = resolve_label(row['to_address'], row['to_label'], row['to_address_type'])
    edge_type = row['edge_type'] or 'unknown'
    verb = ACTION_VERBS.get(edge_type, 'interacted with')
    token = row['token_symbol'] or 'SOL'
    amount_str = format_amount(row['amount_raw'], row['decimals'])

    # Build the core sentence
    parts = [from_label, verb]

    if amount_str:
        parts.append(amount_str)
        parts.append(token)

    # Direction for swaps
    if row['swap_direction']:
        parts.append(row['swap_direction'])

    # Target
    if edge_type not in ('mint', 'burn', 'nft_mint'):
        parts.append('to' if 'to' not in verb else '')
        parts.append(to_label)

    # DEX/Pool context
    if row['dex']:
        parts.append(f"via {row['dex']}")
    if row['pool_label']:
        parts.append(f"pool {row['pool_label']}")

    # Tax info
    if row['tax_bps'] and row['tax_bps'] >= 100:
        parts.append(f"({row['tax_bps'] / 100:.1f}% tax)")

    # Timestamp
    utc_str = block_time_to_utc(row['block_time'])
    if utc_str:
        parts.append(f"at {utc_str}")

    return ' '.join(p for p in parts if p)


def build_metadata(row):
    """Build slim ChromaDB metadata — only filterable fields + display essentials.
    Full details live in the document text (sentence). Addresses shortened to 10 chars."""
    def s(v):
        return str(v) if v is not None else ''

    def i(v):
        return int(v) if v is not None else 0

    amount_human = 0.0
    if row['amount_raw'] is not None and row['decimals'] is not None:
        amount_human = row['amount_raw'] / (10 ** row['decimals'])

    return {
        # Filterable fields (used in WHERE clauses)
        'token_symbol': s(row['token_symbol']),
        'token_name': s(row.get('token_name', '')),
        'edge_type': s(row['edge_type']),
        'edge_category': s(row.get('edge_category', '')),
        'dex': s(row['dex']),
        'block_time': i(row['block_time']),
        'from_address': s(row['from_address']),       # full — needed for address filter
        'to_address': s(row['to_address']),            # full — needed for address filter
        # Display essentials (minimum needed to render a result + click-to-load)
        'guide_id': i(row['guide_id']),
        'tx_signature': s(row['tx_signature']),        # full — needed for click-to-load
        'mint_address': s(row.get('mint_address', '')),# full — needed for click-to-load
        'amount_human': round(amount_human, 6),
        'block_time_utc': block_time_to_utc(row['block_time']),
    }


CHROMA_PERSIST_PATH = os.environ.get('CHROMA_PERSIST_PATH', '')

def get_chromadb_client():
    """Connect to ChromaDB — persistent local if path set, else HTTP."""
    if CHROMA_PERSIST_PATH:
        print(f"[ingest] Using PersistentClient at {CHROMA_PERSIST_PATH}")
        client = chromadb.PersistentClient(path=CHROMA_PERSIST_PATH)
    else:
        print(f"[ingest] Connecting to ChromaDB at {CHROMADB_HOST}:{CHROMADB_PORT}")
        client = chromadb.HttpClient(host=CHROMADB_HOST, port=CHROMADB_PORT)
        client.heartbeat()
    print("[ingest] ChromaDB connected")
    return client


def get_or_create_collection(client, reset=False):
    """Get or create the tx_guide_edges collection."""
    if reset:
        try:
            client.delete_collection(COLLECTION_NAME)
            print(f"[ingest] Deleted existing collection '{COLLECTION_NAME}'")
        except Exception:
            pass

    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={
            'hnsw:space': 'cosine',
            'hnsw:sync_threshold': 50000,
            'hnsw:batch_size': 5000,
            'hnsw:M': 16,
            'hnsw:construction_ef': 64,
        }
    )
    print(f"[ingest] Collection '{COLLECTION_NAME}': {collection.count()} documents")
    return collection


def get_max_ingested_id(collection):
    """Find the highest guide_id already in ChromaDB."""
    if collection.count() == 0:
        return 0
    # ChromaDB doesn't support MAX() — peek at a small sample to find the hint
    # For reliability, we store max_id in collection metadata
    meta = collection.metadata or {}
    return int(meta.get('max_guide_id', 0))


def update_max_id(client, collection_name, max_id):
    """Update the collection metadata with the latest max guide_id."""
    col = client.get_collection(collection_name)
    existing_meta = col.metadata or {}
    existing_meta['max_guide_id'] = str(max_id)
    # Only set non-immutable keys (hnsw:space can't be changed after creation)
    safe_meta = {k: v for k, v in existing_meta.items() if not k.startswith('hnsw:')}
    safe_meta['max_guide_id'] = str(max_id)
    col.modify(metadata=safe_meta)


def export_to_file(output_path, limit=None):
    """Export resolved tx_guide rows to JSONL file."""
    import json as json_mod

    db_config = get_db_config()
    db_config['use_pure'] = True
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    total = 0
    max_id = 0
    t_start = time.time()

    # Modify query limit if specified
    query = INGEST_QUERY
    if limit:
        # Replace the LIMIT %s with a fixed limit for the outer cap
        pass  # We'll use the batch loop with early exit

    with open(output_path, 'w') as f:
        while True:
            if limit and total >= limit:
                break
            batch_limit = min(BATCH_SIZE, (limit - total) if limit else BATCH_SIZE)
            cursor.execute(INGEST_QUERY, (max_id, batch_limit))
            rows = cursor.fetchall()
            if not rows:
                break

            for row in rows:
                # Convert Decimal types to float/int for JSON serialization
                clean = {}
                for k, v in row.items():
                    if hasattr(v, 'as_integer_ratio'):  # Decimal
                        clean[k] = int(v) if v == int(v) else float(v)
                    else:
                        clean[k] = v
                f.write(json_mod.dumps(clean, default=str) + '\n')
                if clean['guide_id'] > max_id:
                    max_id = clean['guide_id']
                total += 1

            elapsed = time.time() - t_start
            rate = total / elapsed if elapsed > 0 else 0
            print(f"[export] {total} rows exported, max_id={max_id}, {rate:.0f} rows/sec")

    cursor.close()
    conn.close()
    elapsed = time.time() - t_start
    print(f"[export] Done. {total} rows exported to {output_path} in {elapsed:.1f}s")


def build_summaries(rows):
    """Generate token, wallet, pool, relationship, and analytical summary documents from edge rows."""
    from collections import defaultdict

    # Accumulators
    tokens = defaultdict(lambda: {'holders': set(), 'swaps': 0, 'edges': 0, 'dexes': set(),
                                   'volume': 0.0, 'first_time': None, 'last_time': None,
                                   'mint': '', 'name': '', 'burns': 0, 'mints': 0,
                                   'tax_edges': 0, 'max_tax_bps': 0})
    wallets = defaultdict(lambda: {'tokens': set(), 'swaps': 0, 'edges': 0, 'dexes': set(),
                                    'buys': 0, 'sells': 0, 'transfers_in': 0, 'transfers_out': 0,
                                    'first_time': None, 'last_time': None, 'funded_by': set(),
                                    'pools': set(), 'label': ''})
    pools = defaultdict(lambda: {'swaps': 0, 'wallets': set(), 'tokens': set(), 'volume': 0.0,
                                  'dex': '', 'label': '', 'first_time': None, 'last_time': None})
    funding = defaultdict(set)  # funder → set of funded wallets
    # Analytical
    largest_swap = None
    all_swap_amounts = []
    dex_volumes = defaultdict(float)

    for r in rows:
        edge_type = r.get('edge_type', '')
        token_sym = r.get('token_symbol', '') or 'SOL'
        from_addr = r.get('from_address', '')
        to_addr = r.get('to_address', '')
        from_type = r.get('from_address_type', '')
        to_type = r.get('to_address_type', '')
        dex = r.get('dex', '') or ''
        pool_label = r.get('pool_label', '') or ''
        amount_raw = r.get('amount_raw') or 0
        decimals = r.get('decimals') or 0
        amount_human = amount_raw / (10 ** decimals) if decimals else 0
        bt = r.get('block_time') or 0
        bt_utc = block_time_to_utc(bt)

        # ── Token stats ──
        t = tokens[token_sym]
        t['edges'] += 1
        t['mint'] = r.get('mint_address', '') or t['mint']
        t['name'] = r.get('token_name', '') or t['name']
        if edge_type in ('swap_in', 'swap_out'):
            t['swaps'] += 1
            t['volume'] += amount_human
            if dex: t['dexes'].add(dex)
        if edge_type == 'swap_in': t['holders'].add(from_addr)
        if edge_type == 'swap_out': t['holders'].add(to_addr)
        if edge_type == 'spl_transfer': t['holders'].add(to_addr)
        if edge_type == 'burn': t['burns'] += 1
        if edge_type == 'mint': t['mints'] += 1
        if r.get('tax_bps', 0) and r['tax_bps'] >= 100:
            t['tax_edges'] += 1
            t['max_tax_bps'] = max(t['max_tax_bps'], r['tax_bps'])
        if bt and (not t['first_time'] or bt < t['first_time']): t['first_time'] = bt
        if bt and (not t['last_time'] or bt > t['last_time']): t['last_time'] = bt

        # ── Wallet stats ──
        for addr, addr_type in [(from_addr, from_type), (to_addr, to_type)]:
            if addr_type not in ('wallet', 'bot', 'dex_wallet', ''): continue
            w = wallets[addr]
            w['edges'] += 1
            w['tokens'].add(token_sym)
            if dex: w['dexes'].add(dex)
            if pool_label: w['pools'].add(pool_label)
            if bt and (not w['first_time'] or bt < w['first_time']): w['first_time'] = bt
            if bt and (not w['last_time'] or bt > w['last_time']): w['last_time'] = bt
        if edge_type == 'swap_out':
            wallets[to_addr]['swaps'] += 1; wallets[to_addr]['buys'] += 1
        elif edge_type == 'swap_in':
            wallets[from_addr]['swaps'] += 1; wallets[from_addr]['sells'] += 1
        elif edge_type == 'spl_transfer':
            wallets[from_addr]['transfers_out'] += 1; wallets[to_addr]['transfers_in'] += 1

        # ── Pool stats ──
        if pool_label:
            p = pools[pool_label]
            if edge_type in ('swap_in', 'swap_out'):
                p['swaps'] += 1; p['volume'] += amount_human
            p['tokens'].add(token_sym)
            p['dex'] = dex or p['dex']
            p['label'] = pool_label
            if from_type == 'wallet': p['wallets'].add(from_addr)
            if to_type == 'wallet': p['wallets'].add(to_addr)
            if bt and (not p['first_time'] or bt < p['first_time']): p['first_time'] = bt
            if bt and (not p['last_time'] or bt > p['last_time']): p['last_time'] = bt

        # ── Funding relationships ──
        if edge_type in ('wallet_funded', 'funded_by'):
            funding[from_addr].add(to_addr)

        # ── Analytical ──
        if edge_type in ('swap_in', 'swap_out') and amount_human > 0:
            all_swap_amounts.append(amount_human)
            if dex: dex_volumes[dex] += amount_human
            if largest_swap is None or amount_human > largest_swap['amount']:
                largest_swap = {'amount': amount_human, 'token': token_sym, 'from': from_addr,
                                'to': to_addr, 'dex': dex, 'time': bt_utc}

    # ── Build summary documents ──
    summaries = []

    # Token summaries
    for sym, t in tokens.items():
        if t['edges'] < 2: continue
        tax_info = f" Token has {t['max_tax_bps']/100:.1f}% tax." if t['tax_edges'] else ""
        doc = (f"{sym} ({t['name'] or sym}) has {len(t['holders'])} unique holders, "
               f"{t['swaps']} swaps, {t['edges']} total edges, "
               f"total swap volume {format_amount(int(t['volume']), 0)}. "
               f"Traded on {', '.join(t['dexes']) or 'unknown DEX'}. "
               f"Mint address: {short_addr(t['mint'])}."
               f"{' Burns: ' + str(t['burns']) + '.' if t['burns'] else ''}"
               f"{' Mints: ' + str(t['mints']) + '.' if t['mints'] else ''}"
               f"{tax_info}"
               f" First seen {block_time_to_utc(t['first_time'])}, last seen {block_time_to_utc(t['last_time'])}.")
        meta = {'doc_type': 'token_summary', 'token_symbol': sym, 'mint_address': t['mint'],
                'holder_count': len(t['holders']), 'swap_count': t['swaps'], 'edge_count': t['edges'],
                'dex': ', '.join(t['dexes']), 'block_time': t['last_time'] or 0,
                'block_time_utc': block_time_to_utc(t['last_time'])}
        summaries.append(('token_' + hashlib.md5(sym.encode()).hexdigest()[:12], doc, meta))

    # Wallet summaries
    for addr, w in wallets.items():
        if w['edges'] < 2: continue
        doc = (f"Wallet {short_addr(addr)} traded {len(w['tokens'])} tokens ({', '.join(list(w['tokens'])[:5])}), "
               f"made {w['swaps']} swaps ({w['buys']} buys, {w['sells']} sells), "
               f"{w['transfers_in']} transfers in, {w['transfers_out']} transfers out, "
               f"{w['edges']} total edges. "
               f"Used {', '.join(list(w['dexes'])[:3]) or 'no DEX'}. "
               f"Active from {block_time_to_utc(w['first_time'])} to {block_time_to_utc(w['last_time'])}.")
        meta = {'doc_type': 'wallet_summary', 'from_address': addr, 'to_address': addr,
                'token_symbol': ', '.join(list(w['tokens'])[:5]), 'swap_count': w['swaps'],
                'edge_count': w['edges'], 'dex': ', '.join(list(w['dexes'])[:3]),
                'block_time': w['last_time'] or 0,
                'block_time_utc': block_time_to_utc(w['last_time'])}
        summaries.append(('wallet_' + hashlib.md5(addr.encode()).hexdigest()[:12], doc, meta))

    # Pool summaries
    for label, p in pools.items():
        if p['swaps'] < 2: continue
        doc = (f"Pool {label} on {p['dex'] or 'unknown DEX'} processed {p['swaps']} swaps "
               f"across {len(p['wallets'])} wallets, "
               f"tokens: {', '.join(list(p['tokens'])[:5])}, "
               f"total volume {format_amount(int(p['volume']), 0)}. "
               f"Active from {block_time_to_utc(p['first_time'])} to {block_time_to_utc(p['last_time'])}.")
        meta = {'doc_type': 'pool_summary', 'pool_label': label, 'dex': p['dex'],
                'token_symbol': ', '.join(list(p['tokens'])[:3]), 'swap_count': p['swaps'],
                'wallet_count': len(p['wallets']),
                'block_time': p['last_time'] or 0,
                'block_time_utc': block_time_to_utc(p['last_time'])}
        summaries.append(('pool_' + hashlib.md5(label.encode()).hexdigest()[:12], doc, meta))

    # Funding relationship summaries
    for funder, funded_set in funding.items():
        if not funded_set: continue
        doc = (f"Wallet {short_addr(funder)} funded {len(funded_set)} wallets: "
               f"{', '.join(short_addr(a) for a in list(funded_set)[:10])}.")
        meta = {'doc_type': 'funding_summary', 'from_address': funder,
                'to_address': ', '.join(list(funded_set)[:3]),
                'wallet_count': len(funded_set), 'block_time': 0, 'block_time_utc': ''}
        summaries.append(('funding_' + hashlib.md5(funder.encode()).hexdigest()[:12], doc, meta))

    # Analytical summaries
    if all_swap_amounts:
        avg_swap = sum(all_swap_amounts) / len(all_swap_amounts)
        top_dexes = sorted(dex_volumes.items(), key=lambda x: -x[1])[:5]
        doc = (f"Across all transactions: {len(all_swap_amounts)} total swaps, "
               f"average swap size {format_amount(int(avg_swap), 0)}, "
               f"top DEXes by volume: {', '.join(f'{d} ({format_amount(int(v), 0)})' for d, v in top_dexes)}. "
               f"Largest single swap: {format_amount(int(largest_swap['amount']), 0)} {largest_swap['token']} "
               f"on {largest_swap['dex']} at {largest_swap['time']}.")
        meta = {'doc_type': 'analytics_summary', 'swap_count': len(all_swap_amounts),
                'dex': top_dexes[0][0] if top_dexes else '', 'block_time': 0, 'block_time_utc': ''}
        summaries.append(('analytics_global', doc, meta))

    return summaries


def ingest_from_file(file_path, reset=False):
    """Ingest edges + summaries from a JSONL file into ChromaDB."""
    import json as json_mod
    from sentence_transformers import SentenceTransformer

    print("[ingest] Loading SentenceTransformer model...")
    model = SentenceTransformer('all-MiniLM-L6-v2')
    print("[ingest] Model loaded")

    client = get_chromadb_client()
    collection = get_or_create_collection(client, reset=reset)

    # Phase 1: Read all rows
    print(f"[ingest] Reading {file_path}...")
    raw_rows = []
    all_ids = []
    all_docs = []
    all_metas = []
    max_id = 0

    with open(file_path, 'r') as f:
        for line in f:
            row = json_mod.loads(line)
            raw_rows.append(row)
            all_ids.append(str(row['guide_id']))
            all_docs.append(build_sentence(row))
            all_metas.append(build_metadata(row))
            if row['guide_id'] > max_id:
                max_id = row['guide_id']

    print(f"[ingest] {len(all_ids)} edge rows loaded")

    # Phase 2: Generate summary documents
    print("[ingest] Computing summaries...")
    t_sum = time.time()
    summaries = build_summaries(raw_rows)
    for sid, sdoc, smeta in summaries:
        all_ids.append('summary_' + sid)
        all_docs.append(sdoc)
        # Ensure all metadata values are valid ChromaDB types
        clean_meta = {}
        for k, v in smeta.items():
            if isinstance(v, set): v = ', '.join(str(x) for x in v)
            if v is None: v = ''
            clean_meta[k] = v
        all_metas.append(clean_meta)
    print(f"[ingest] {len(summaries)} summaries generated in {time.time() - t_sum:.1f}s "
          f"(token: {sum(1 for s in summaries if s[2].get('doc_type')=='token_summary')}, "
          f"wallet: {sum(1 for s in summaries if s[2].get('doc_type')=='wallet_summary')}, "
          f"pool: {sum(1 for s in summaries if s[2].get('doc_type')=='pool_summary')}, "
          f"funding: {sum(1 for s in summaries if s[2].get('doc_type')=='funding_summary')}, "
          f"analytics: {sum(1 for s in summaries if s[2].get('doc_type')=='analytics_summary')})")

    total_docs = len(all_ids)
    print(f"[ingest] Total documents to embed: {total_docs} ({len(raw_rows)} edges + {len(summaries)} summaries)")

    # Phase 3: Pre-compute ALL embeddings
    t_embed = time.time()
    embeddings = model.encode(
        all_docs,
        batch_size=EMBED_BATCH,
        show_progress_bar=True,
        normalize_embeddings=True
    ).tolist()
    embed_time = time.time() - t_embed
    print(f"[ingest] Embeddings computed in {embed_time:.1f}s ({total_docs / embed_time:.0f} docs/sec)")

    # Phase 4: Bulk insert
    t_insert = time.time()
    for i in range(0, total_docs, BATCH_SIZE):
        end = min(i + BATCH_SIZE, total_docs)
        collection.upsert(
            ids=all_ids[i:end],
            documents=all_docs[i:end],
            embeddings=embeddings[i:end],
            metadatas=all_metas[i:end]
        )
        print(f"[ingest] Inserted {end}/{total_docs}")

    insert_time = time.time() - t_insert
    print(f"[ingest] Insert completed in {insert_time:.1f}s")
    print(f"[ingest] Collection now has {collection.count()} documents")

    if max_id > 0:
        update_max_id(client, COLLECTION_NAME, max_id)

    total_time = embed_time + insert_time
    print(f"[ingest] Done. {total_docs} documents in {total_time:.1f}s "
          f"(embed: {embed_time:.1f}s, insert: {insert_time:.1f}s). "
          f"Collection: {collection.count()} documents.")


def ingest(mode='full'):
    """Main ingestion loop — reads from MySQL directly."""
    client = get_chromadb_client()
    reset = mode == 'reset'
    collection = get_or_create_collection(client, reset=reset)

    start_id = 0 if mode in ('full', 'reset') else get_max_ingested_id(collection)
    print(f"[ingest] Mode: {mode}, starting from guide_id > {start_id}")

    db_config = get_db_config()
    db_config['use_pure'] = True
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    total_ingested = 0
    max_id = start_id
    batch_num = 0
    t_start = time.time()

    while True:
        cursor.execute(INGEST_QUERY, (max_id, BATCH_SIZE))
        rows = cursor.fetchall()

        if not rows:
            break

        batch_num += 1
        ids = []
        documents = []
        metadatas = []

        for row in rows:
            doc_id = str(row['guide_id'])
            sentence = build_sentence(row)
            metadata = build_metadata(row)

            ids.append(doc_id)
            documents.append(sentence)
            metadatas.append(metadata)

            if row['guide_id'] > max_id:
                max_id = row['guide_id']

        collection.upsert(ids=ids, documents=documents, metadatas=metadatas)
        total_ingested += len(rows)

        elapsed = time.time() - t_start
        rate = total_ingested / elapsed if elapsed > 0 else 0
        print(f"[ingest] Batch {batch_num}: {len(rows)} rows, total {total_ingested}, "
              f"max_id={max_id}, {rate:.0f} rows/sec")

    if max_id > start_id:
        update_max_id(client, COLLECTION_NAME, max_id)

    cursor.close()
    conn.close()

    elapsed = time.time() - t_start
    print(f"[ingest] Done. {total_ingested} rows ingested in {elapsed:.1f}s. "
          f"Collection now has {collection.count()} documents.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ChromaDB tx_guide ingestion')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--full', action='store_true', help='Ingest all rows from MySQL')
    group.add_argument('--incremental', action='store_true', help='Ingest only new rows from MySQL')
    group.add_argument('--reset', action='store_true', help='Delete collection and re-ingest from MySQL')
    group.add_argument('--export', type=str, metavar='FILE', help='Export resolved rows to JSONL file')
    group.add_argument('--from-file', type=str, metavar='FILE', help='Ingest from JSONL file (no MySQL needed)')
    parser.add_argument('--limit', type=int, default=None, help='Max rows to export (with --export)')
    parser.add_argument('--clear', action='store_true', help='Clear collection before --from-file import')
    args = parser.parse_args()

    if args.export:
        export_to_file(args.export, limit=args.limit)
    elif args.from_file:
        ingest_from_file(args.from_file, reset=args.clear)
    else:
        mode = 'full' if args.full else ('reset' if args.reset else 'incremental')
        ingest(mode)
