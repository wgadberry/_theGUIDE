"""
graph_service.py -- ArangoDB graph operations microservice layer

Single source of truth for graph schema, collection bootstrap, and
upsert operations. All workers import this instead of writing inline
graph logic.

Collections:
    addresses   — every Solana account (wallet, pool, program, vault, etc.)
    edges       — ALL directed relationships (transfers, swaps, funding, burns, mints)
    tokens      — SPL token metadata (one doc per mint)

Dropped collections (legacy):
    pool_edges, defi_activities, transactions — consolidated into edges

Edge schema (universal):
    _from, _to, edge_type, token, amount, decimals, signature, block_time,
    value_usd, pool, activity_type, flow, legs[]

Usage:
    from graph_service import GraphService
    gs = GraphService()                    # connects to ArangoDB
    gs.upsert_addresses(addr_docs)
    gs.upsert_tokens(token_docs)
    gs.upsert_edges(edge_docs)
    gs.upsert_swap_edges(defi_docs, owner)
    gs.upsert_funded_by(funder, funded, sig, block_time)
"""

import re
import time
from datetime import datetime
from arango import ArangoClient


# ---------------------------------------------------------------------------
# Config defaults (overridable via constructor)
# ---------------------------------------------------------------------------

ARANGO_HOST = 'http://localhost:8529'
ARANGO_DB   = 't16o'
ARANGO_USER = 'root'
ARANGO_PASS = 'localdev'


def _log(msg):
    print(f'[{datetime.now().strftime("%H:%M:%S")}][graph-svc] {msg}', flush=True)


# ---------------------------------------------------------------------------
# Address type classification
# ---------------------------------------------------------------------------

TYPE_RANK = {
    'unknown': 0, 'wallet': 1, 'ata': 2, 'mint': 3, 'program': 4,
    'market': 5, 'dex_wallet': 5, 'lp_token': 6, 'pool': 7,
    'vault': 8, 'bot': 9,
}


def classify_address_type(meta):
    """Derive address type from Solscan metadata dict.

    Args:
        meta: dict with account_label, account_tags, account_type

    Returns:
        str: classified type or 'unknown'
    """
    tags = [t.lower() for t in (meta.get('account_tags') or [])]
    label = (meta.get('account_label') or '').lower()
    acct = meta.get('account_type', '')

    if 'vault' in label and not re.search(r'\([^)]*vault[^)]*\)', label):
        return 'vault'
    if 'pool' in tags:       return 'pool'
    if 'market' in tags:     return 'market'
    if any('vault' in t for t in tags): return 'vault'
    if 'lptoken' in tags:    return 'lp_token'
    if 'dex_wallet' in tags: return 'dex_wallet'
    if 'arbitrage_bot' in tags: return 'bot'
    if acct == 'token_account': return 'ata'
    if acct == 'program':    return 'program'
    if acct == 'mint':       return 'mint'
    if acct == 'address':    return 'wallet'
    return 'unknown'


def should_upgrade_type(current, new):
    """Only upgrade to a more specific type, never downgrade."""
    return TYPE_RANK.get(new, 0) > TYPE_RANK.get(current, 0)


# ---------------------------------------------------------------------------
# Edge type mapping
# ---------------------------------------------------------------------------

ACTIVITY_TO_EDGE_TYPE = {
    'ACTIVITY_SPL_TRANSFER':           'spl_transfer',
    'ACTIVITY_TOKEN_SWAP':             'swap_in',
    'ACTIVITY_AGG_TOKEN_SWAP':         'swap_in',
    'ACTIVITY_TOKEN_ADD_LIQUIDITY':    'add_liquidity',
    'ACTIVITY_TOKEN_ADD_LIQ':          'add_liquidity',
    'ACTIVITY_TOKEN_REMOVE_LIQUIDITY': 'remove_liquidity',
    'ACTIVITY_TOKEN_REMOVE_LIQ':       'remove_liquidity',
    'ACTIVITY_SPL_BURN':               'burn',
    'ACTIVITY_SPL_MINT':               'mint',
    'ACTIVITY_SPL_INIT_MINT':          'mint',
    'ACTIVITY_SPL_CREATE_ACCOUNT':     'create_ata',
    'ACTIVITY_SYSTEM_TRANSFER':        'sol_transfer',
}


# ---------------------------------------------------------------------------
# Address doc builder
# ---------------------------------------------------------------------------

def build_address_doc(address, block_time=0, addr_type='unknown', enriched=0, meta=None):
    """Build a standardized address document.

    Args:
        address:    Solana pubkey
        block_time: first/last seen timestamp
        addr_type:  address type (wallet, pool, etc.)
        enriched:   0 or 1
        meta:       optional Solscan metadata dict for full enrichment
    """
    doc = {
        '_key': address,
        'type': addr_type,
        'first_seen': block_time or 0,
        'last_seen': block_time or 0,
        'enriched': enriched,
    }
    if meta:
        doc['type'] = classify_address_type(meta)
        doc['label'] = meta.get('account_label', '')
        doc['tags'] = meta.get('account_tags', [])
        doc['active_age'] = meta.get('active_age')
        doc['enriched'] = 1
        doc['enriched_at'] = int(time.time())
        funded = meta.get('funded_by') or {}
        if funded and funded.get('funded_by'):
            doc['funded_by'] = funded['funded_by']
            doc['funded_by_tx'] = funded.get('tx_hash', '')
            doc['funded_by_time'] = funded.get('block_time')
        else:
            doc['funded_by'] = 'none'
    return doc


# ---------------------------------------------------------------------------
# Token doc builder
# ---------------------------------------------------------------------------

def build_token_doc(mint, symbol='', name='', decimals=0, icon=''):
    """Build a standardized token document."""
    return {
        '_key': mint,
        'symbol': symbol,
        'name': name,
        'decimals': decimals,
        'icon': icon,
    }


# ---------------------------------------------------------------------------
# Transaction doc builder
# ---------------------------------------------------------------------------

def build_transaction_doc(raw, origin=''):
    """Build a transaction document from any Solscan record (transfer or defi).

    The transactions collection is a processing ledger — one doc per trans_id,
    storing the full context of what happened. Works for both transfer and defi
    activity records by capturing all available fields.

    Args:
        raw: dict from Solscan /account/transfer or flattened defi activity
        origin: holder address that triggered this processing
    """
    is_defi = bool(raw.get('token_in'))

    doc = {
        '_key':               raw.get('trans_id', ''),
        'block_time':         raw.get('block_time', 0),
        'block_id':           raw.get('block_id'),
        'activity_type':      raw.get('activity_type', ''),
        'from_address':       raw.get('from_address', ''),
        'to_address':         raw.get('to_address', '') or (raw.get('pool_address', '') if is_defi else ''),
        'from_token_account': raw.get('from_token_account', ''),
        'to_token_account':   raw.get('to_token_account', ''),
        'token_address':      raw.get('token_address', '') or (raw.get('token_in', '') if is_defi else ''),
        'token_decimals':     raw.get('token_decimals'),
        'amount':             raw.get('amount') or raw.get('amount_in'),
        'flow':               raw.get('flow', '') or ('out' if is_defi else ''),
        'value_usd':          raw.get('value') or raw.get('value_usd'),
        'origin':             origin,
        'processed_at':       int(time.time()),
    }
    # DeFi-specific fields (present on swap activities, absent on transfers)
    if is_defi:
        doc['token_in'] = raw['token_in']
        doc['token_out'] = raw.get('token_out', '')
        doc['amount_in'] = raw.get('amount_in')
        doc['amount_out'] = raw.get('amount_out')
        doc['pool_address'] = raw.get('pool_address', '')
        doc['sources'] = raw.get('sources', [])
        doc['platform'] = raw.get('platform', [])
        doc['legs'] = raw.get('legs', [])
        doc['leg_count'] = raw.get('leg_count', 0)
    return doc


# ---------------------------------------------------------------------------
# Edge doc builders
# ---------------------------------------------------------------------------

def build_transfer_edge(from_addr, to_addr, transfer):
    """Build an edge doc from a Solscan account/transfer record."""
    activity = transfer.get('activity_type', '')
    edge_type = ACTIVITY_TO_EDGE_TYPE.get(activity, 'spl_transfer')
    if edge_type == 'swap_in' and transfer.get('flow') == 'out':
        edge_type = 'swap_out'

    token_addr = transfer.get('token_address', '')
    return {
        '_from': f'addresses/{from_addr}',
        '_to': f'addresses/{to_addr}',
        'edge_type': edge_type,
        'token': f'tokens/{token_addr}' if token_addr else None,
        'amount': str(transfer.get('amount', '0')),
        'decimals': transfer.get('token_decimals', 0),
        'signature': transfer.get('trans_id', ''),
        'block_time': transfer.get('block_time', 0),
        'activity_type': activity,
        'flow': transfer.get('flow', ''),
        'value_usd': transfer.get('value'),
    }


def build_funded_by_edge(funder, funded, signature='', block_time=0):
    """Build a funded_by edge."""
    return {
        '_from': f'addresses/{funder}',
        '_to': f'addresses/{funded}',
        'edge_type': 'funded_by',
        'token': None,
        'amount': '0',
        'decimals': 0,
        'signature': signature,
        'block_time': block_time,
    }


def build_swap_edges(defi_doc, owner):
    """Build swap_in/swap_out edge docs + pool address docs from a defi activity doc.

    Args:
        defi_doc: dict with pool_address, token_in, token_out, amount_in, amount_out, legs[]
        owner:    holder wallet address

    Returns:
        (addr_docs, edge_docs)
    """
    addr_docs = []
    edge_docs = []

    activity_type = defi_doc.get('activity_type', '')
    if 'SWAP' not in activity_type:
        return addr_docs, edge_docs

    tid = defi_doc.get('trans_id', '')
    bt = defi_doc.get('block_time', 0)
    from_addr = defi_doc.get('from_address', owner)
    token_in = defi_doc.get('token_in', '')
    token_out = defi_doc.get('token_out', '')
    amount_in = defi_doc.get('amount_in')
    amount_out = defi_doc.get('amount_out')
    value = defi_doc.get('value_usd')
    legs = defi_doc.get('legs') or []

    if not token_in or not token_out or not tid:
        return addr_docs, edge_docs

    pool = defi_doc.get('pool_address', '')
    if not pool and legs:
        pool = legs[0].get('pool_address', '')
    if not pool:
        return addr_docs, edge_docs

    addr_docs.append(build_address_doc(pool, bt, 'pool'))

    edge_docs.append({
        '_from': f'addresses/{from_addr}', '_to': f'addresses/{pool}',
        'edge_type': 'swap_in',
        'token': f'tokens/{token_in}',
        'amount': str(amount_in or '0'),
        'signature': tid, 'block_time': bt,
        'activity_type': activity_type, 'value_usd': value,
        'pool': f'addresses/{pool}',
        'legs': legs,
    })

    edge_docs.append({
        '_from': f'addresses/{pool}', '_to': f'addresses/{from_addr}',
        'edge_type': 'swap_out',
        'token': f'tokens/{token_out}',
        'amount': str(amount_out or '0'),
        'signature': tid, 'block_time': bt,
        'activity_type': activity_type, 'value_usd': value,
        'pool': f'addresses/{pool}',
    })

    # Multi-hop legs
    for leg in legs[1:]:
        lp = leg.get('pool_address', '')
        lt_in = leg.get('token_in', '')
        lt_out = leg.get('token_out', '')
        if lp and lt_in and lt_out:
            addr_docs.append(build_address_doc(lp, bt, 'pool'))
            edge_docs.append({
                '_from': f'addresses/{from_addr}', '_to': f'addresses/{lp}',
                'edge_type': 'swap_in', 'token': f'tokens/{lt_in}',
                'amount': str(leg.get('amount_in', '0')),
                'signature': tid, 'block_time': bt,
                'activity_type': 'swap_hop', 'leg_index': leg['leg_index'],
            })
            edge_docs.append({
                '_from': f'addresses/{lp}', '_to': f'addresses/{from_addr}',
                'edge_type': 'swap_out', 'token': f'tokens/{lt_out}',
                'amount': str(leg.get('amount_out', '0')),
                'signature': tid, 'block_time': bt,
                'activity_type': 'swap_hop', 'leg_index': leg['leg_index'],
            })

    return addr_docs, edge_docs


def build_defi_doc(activity, owner):
    """Build a flattened defi activity doc with router legs.

    This is stored as fields on the swap edge (legs array), NOT in a
    separate collection.
    """
    routers = activity.get('routers') or {}
    child_routers = routers.get('child_routers') or []

    legs = []
    if routers.get('token1') and routers.get('token2'):
        legs.append({
            'token_in': routers['token1'], 'decimals_in': routers.get('token1_decimals'),
            'amount_in': routers.get('amount1'),
            'token_out': routers['token2'], 'decimals_out': routers.get('token2_decimals'),
            'amount_out': routers.get('amount2'),
            'pool_address': routers.get('pool_address', ''),
            'program_address': routers.get('program_address', ''),
            'leg_index': 0,
        })
    for i, leg in enumerate(child_routers, start=1):
        legs.append({
            'token_in': leg.get('token1'), 'decimals_in': leg.get('token1_decimals'),
            'amount_in': leg.get('amount1'),
            'token_out': leg.get('token2'), 'decimals_out': leg.get('token2_decimals'),
            'amount_out': leg.get('amount2'),
            'pool_address': leg.get('pool_address', ''),
            'program_address': leg.get('program_address', ''),
            'leg_index': i,
        })

    return {
        'trans_id': activity.get('trans_id', ''),
        'block_id': activity.get('block_id'),
        'block_time': activity.get('block_time'),
        'activity_type': activity.get('activity_type'),
        'from_address': activity.get('from_address', owner),
        'owner': owner,
        'sources': activity.get('sources', []),
        'platform': activity.get('platform', []),
        'value_usd': activity.get('value', 0),
        'token_in': routers.get('token1', ''),
        'token_out': routers.get('token2', ''),
        'amount_in': routers.get('amount1'),
        'amount_out': routers.get('amount2'),
        'pool_address': routers.get('pool_address', ''),
        'legs': legs,
        'leg_count': len(legs),
    }


# ---------------------------------------------------------------------------
# GraphService class
# ---------------------------------------------------------------------------

class GraphService:
    """Encapsulates all ArangoDB graph operations.

    Usage:
        gs = GraphService()
        gs.upsert_addresses([...])
        gs.upsert_tokens([...])
        gs.upsert_edges([...])
    """

    def __init__(self, host=ARANGO_HOST, db_name=ARANGO_DB,
                 user=ARANGO_USER, password=ARANGO_PASS):
        client = ArangoClient(hosts=host)
        self.db = client.db(db_name, username=user, password=password)
        self._bootstrapped = False

    def bootstrap(self):
        """Create collections + indexes if they don't exist. Idempotent."""
        if self._bootstrapped:
            return

        # Document collections
        for name in ('addresses', 'tokens', 'transactions'):
            if not self.db.has_collection(name):
                self.db.create_collection(name)
                _log(f'Created collection: {name}')

        # Edge collection
        if not self.db.has_collection('edges'):
            self.db.create_collection('edges', edge=True)
            _log('Created edge collection: edges')

        # Named graph
        if not self.db.has_graph('t16o_graph'):
            self.db.create_graph('t16o_graph', edge_definitions=[{
                'edge_collection': 'edges',
                'from_vertex_collections': ['addresses'],
                'to_vertex_collections': ['addresses'],
            }])

        # Indexes — edges (persistent)
        edges = self.db.collection('edges')
        _ensure_indexes(edges, [
            ['signature'],
            ['block_time'],
            ['_from'],           # needed for sweep AQL inner scan
            ['_to'],             # needed for sweep AQL inner scan
            ['token', 'block_time'],
            ['edge_type'],
            ['token', '_from'],
            ['token', '_to'],
            ['edge_type', '_to'],
        ])

        # Vertex-centric indexes — accelerate graph traversals
        # These let TRAVERSAL queries filter edges by attribute WITHOUT
        # scanning all edges from a vertex.
        _ensure_indexes(edges, [
            ['_from', 'edge_type'],       # outbound traversal filtered by type
            ['_to', 'edge_type'],         # inbound traversal filtered by type
            ['_from', 'token'],           # outbound traversal filtered by token
            ['_to', 'token'],             # inbound traversal filtered by token
            ['_from', 'edge_type', 'token'],  # outbound: type + token (bubble map query)
            ['_to', 'edge_type', 'token'],    # inbound: type + token
        ])

        # Unique constraint
        try:
            edges.add_index({
                'type': 'persistent',
                'fields': ['signature', '_from', '_to', 'edge_type', 'token'],
                'unique': True,
            })
        except Exception:
            pass

        # Indexes — addresses
        addrs = self.db.collection('addresses')
        _ensure_indexes(addrs, [
            ['type'],
            ['label'],
            ['enriched', 'type'],
            ['enriched_at'],
        ])

        # Indexes — tokens
        tokens = self.db.collection('tokens')
        _ensure_indexes(tokens, [
            ['symbol'],
            ['name'],
        ])

        # Indexes — transactions
        txs = self.db.collection('transactions')
        _ensure_indexes(txs, [
            ['block_time'],
            ['from_address'],
            ['to_address'],
            ['activity_type'],
            ['token_address'],
            ['origin'],
        ])

        # Drop legacy collections
        for legacy in ('pool_edges', 'defi_activities', 'transactions'):
            if self.db.has_collection(legacy):
                _log(f'Legacy collection exists: {legacy} (not dropping — manual cleanup)')

        self._bootstrapped = True
        _log('Bootstrap complete')

    def upsert_addresses(self, addr_docs):
        """Batch upsert address documents. Merges with existing docs."""
        if not addr_docs:
            return
        # Dedupe within batch
        seen = {}
        for doc in addr_docs:
            key = doc['_key']
            if key not in seen:
                seen[key] = doc
            else:
                ex = seen[key]
                if doc.get('type', 'unknown') != 'unknown':
                    ex['type'] = doc['type']
                fs = doc.get('first_seen', 0) or 0
                ls = doc.get('last_seen', 0) or 0
                if fs:
                    ex['first_seen'] = min(ex.get('first_seen', 0) or 0, fs)
                if ls:
                    ex['last_seen'] = max(ex.get('last_seen', 0) or 0, ls)
                # Merge enrichment fields
                for k in ('label', 'tags', 'funded_by', 'funded_by_tx',
                           'funded_by_time', 'active_age', 'enriched', 'enriched_at'):
                    if doc.get(k) is not None:
                        ex[k] = doc[k]

        self.db.collection('addresses').insert_many(
            list(seen.values()), overwrite_mode='update', merge=True, silent=True)

    def upsert_tokens(self, token_docs):
        """Batch upsert token documents with COALESCE — non-empty incoming values
        win, but existing non-empty values are preserved when incoming is empty.
        """
        if not token_docs:
            return
        # Dedupe within batch — non-empty values win
        seen = {}
        for doc in token_docs:
            key = doc['_key']
            if key not in seen:
                seen[key] = dict(doc)
            else:
                for k, v in doc.items():
                    if v is not None and v != '':
                        seen[key][k] = v

        # AQL UPSERT with COALESCE — preserves existing non-empty values
        self.db.aql.execute('''
            FOR doc IN @docs
                UPSERT { _key: doc._key }
                INSERT doc
                UPDATE {
                    symbol:   doc.symbol   != "" ? doc.symbol   : OLD.symbol,
                    name:     doc.name     != "" ? doc.name     : OLD.name,
                    icon:     doc.icon     != "" ? doc.icon     : OLD.icon,
                    decimals: doc.decimals != null ? doc.decimals : OLD.decimals
                } IN tokens
        ''', bind_vars={'docs': list(seen.values())})

    def upsert_edges(self, edge_docs):
        """Batch insert edges. Unique index handles dedup — silent skip on conflict."""
        if not edge_docs:
            return
        self.db.collection('edges').insert_many(edge_docs, silent=True, overwrite=False)

    def upsert_transactions(self, tx_docs):
        """Batch upsert transaction docs. _key = trans_id, update on duplicate."""
        if not tx_docs:
            return
        self.db.collection('transactions').insert_many(
            tx_docs, overwrite_mode='update', merge=True, silent=True)

    def upsert_funded_by(self, funder, funded, signature='', block_time=0):
        """Create a funded_by edge + ensure funder address exists."""
        self.upsert_addresses([build_address_doc(funder, block_time, 'wallet')])
        self.upsert_edges([build_funded_by_edge(funder, funded, signature, block_time)])

    def upsert_swap_edges(self, defi_docs, owner):
        """Create swap_in/swap_out edges from defi activity docs.

        Args:
            defi_docs: list of dicts from build_defi_doc()
            owner:     holder wallet address
        """
        all_addrs = []
        all_edges = []
        for d in defi_docs:
            addrs, edges = build_swap_edges(d, owner)
            all_addrs.extend(addrs)
            all_edges.extend(edges)
        if all_addrs:
            self.upsert_addresses(all_addrs)
        if all_edges:
            self.upsert_edges(all_edges)

    def upsert_graph(self, addr_docs, token_docs, edge_docs):
        """Combined upsert — addresses + tokens + edges in parallel."""
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [
                pool.submit(self.upsert_addresses, addr_docs),
                pool.submit(self.upsert_tokens, token_docs),
                pool.submit(self.upsert_edges, edge_docs),
            ]
            for f in futures:
                f.result()

    # ------------------------------------------------------------------
    # Graph traversal queries (use vertex-centric indexes)
    # ------------------------------------------------------------------

    def token_graph(self, mint, limit=500):
        """Get aggregated edges + nodes for a token — replaces the viewer's main AQL.

        Uses graph traversal pattern: start from edges filtered by token,
        aggregate by from/to/type, resolve address docs.
        """
        return self.query('''
            LET token_ref = CONCAT("tokens/", @mint)
            LET raw_edges = (
                FOR e IN edges
                    FILTER e.token == token_ref
                    FILTER e.edge_type NOT IN ["funded_by"]
                    COLLECT from_addr = e._from, to_addr = e._to,
                            edge_type = e.edge_type, token = e.token
                    WITH COUNT INTO tx_count
                    SORT tx_count DESC
                    LIMIT @limit
                    LET fd = DOCUMENT(from_addr)
                    LET td = DOCUMENT(to_addr)
                    LET tk = token ? DOCUMENT(token) : null
                    RETURN {
                        source: fd._key, target: td._key,
                        type: edge_type, tx_count: tx_count,
                        source_type: fd.type, target_type: td.type,
                        source_label: fd.label, target_label: td.label,
                        token: token,
                        token_symbol: tk.symbol || tk.token_symbol || "",
                        token_name: tk.name || tk.token_name || "",
                        token_decimals: tk.decimals
                    }
            )
            LET all_addrs = UNION_DISTINCT(
                (FOR e IN raw_edges RETURN e.source),
                (FOR e IN raw_edges RETURN e.target)
            )
            LET nodes = (
                FOR addr IN all_addrs
                    LET doc = DOCUMENT(CONCAT("addresses/", addr))
                    LET out_c = SUM(FOR e IN raw_edges FILTER e.source == addr RETURN e.tx_count)
                    LET in_c  = SUM(FOR e IN raw_edges FILTER e.target == addr RETURN e.tx_count)
                    RETURN {
                        address: addr,
                        address_type: doc.type || "unknown",
                        label: doc.label,
                        interactions: { sent: out_c, received: in_c }
                    }
            )
            LET token_doc = DOCUMENT(CONCAT("tokens/", @mint))
            RETURN {
                token: {
                    mint: @mint,
                    symbol: token_doc.symbol || token_doc.token_symbol,
                    name: token_doc.name || token_doc.token_name,
                    decimals: token_doc.decimals
                },
                nodes: nodes,
                edges: raw_edges
            }
        ''', bind_vars={'mint': mint, 'limit': limit})

    def holder_edges(self, mint, holder_addrs, limit=500):
        """Get edges between holder addresses for a token.

        Uses vertex-centric index on [token, _from].
        """
        holder_refs = [f'addresses/{a}' for a in holder_addrs]
        return self.query('''
            LET holder_refs = @holderRefs
            LET token_ref = CONCAT("tokens/", @mint)
            FOR e IN edges
                FILTER e.token == token_ref
                FILTER e._from IN holder_refs AND e._to IN holder_refs
                FILTER e._from != e._to
                COLLECT from_addr = SUBSTRING(e._from, 10),
                        to_addr = SUBSTRING(e._to, 10)
                WITH COUNT INTO tx_count
                SORT tx_count DESC
                LIMIT @limit
                RETURN { source: from_addr, target: to_addr, tx_count: tx_count }
        ''', bind_vars={'mint': mint, 'holderRefs': holder_refs, 'limit': limit})

    def holder_aggregates(self, mint, holder_addrs, limit=50):
        """Aggregate counterparties (pools, programs, exchanges) by holder connections.

        Returns one object per pool/exchange with:
          - address, type, label
          - holder_count: how many unique holders interact through it
          - edge_count: total edges through it
          - holders[]: nested list of { address, tx_count } sorted by activity

        This is the core query for the aggregated bubble map — pools as parent
        nodes sized by holder connection count, not individual wallet noise.
        """
        holder_refs = [f'addresses/{a}' for a in holder_addrs]
        return self.query('''
            LET token_ref = CONCAT("tokens/", @mint)
            LET holder_refs = @holderRefs

            FOR e IN edges
                FILTER e.token == token_ref
                FILTER e.edge_type != "funded_by"
                FILTER e._from IN holder_refs OR e._to IN holder_refs

                LET holder = e._from IN holder_refs
                    ? SUBSTRING(e._from, 10)
                    : SUBSTRING(e._to, 10)
                LET counterparty = e._from IN holder_refs
                    ? SUBSTRING(e._to, 10)
                    : SUBSTRING(e._from, 10)
                LET cp_doc = DOCUMENT(CONCAT("addresses/", counterparty))

                FILTER cp_doc.type NOT IN ["wallet", "unknown", null]

                COLLECT pool = counterparty,
                        pool_type = cp_doc.type,
                        pool_label = cp_doc.label
                INTO holder_edges

                LET unique_holders = UNIQUE(holder_edges[*].holder)
                LET edge_count = LENGTH(holder_edges)

                SORT edge_count DESC
                LIMIT @limit

                RETURN {
                    address: pool,
                    type: pool_type,
                    label: pool_label,
                    holder_count: LENGTH(unique_holders),
                    edge_count: edge_count,
                    holders: (
                        FOR h IN holder_edges
                            COLLECT holder_addr = h.holder WITH COUNT INTO tx_count
                            SORT tx_count DESC
                            RETURN { address: holder_addr, tx_count: tx_count }
                    )
                }
        ''', bind_vars={'mint': mint, 'holderRefs': holder_refs, 'limit': limit})

    def holder_direct_edges(self, mint, holder_addrs, limit=500):
        """Get direct edges between holders only (no counterparties).

        Complements holder_aggregates — shows holder-to-holder transfers
        that don't route through a pool/program.
        """
        holder_refs = [f'addresses/{a}' for a in holder_addrs]
        return self.query('''
            LET holder_refs = @holderRefs
            LET token_ref = CONCAT("tokens/", @mint)
            FOR e IN edges
                FILTER e.token == token_ref
                FILTER e._from IN holder_refs AND e._to IN holder_refs
                FILTER e._from != e._to
                FILTER e.edge_type != "funded_by"
                COLLECT from_addr = SUBSTRING(e._from, 10),
                        to_addr = SUBSTRING(e._to, 10)
                WITH COUNT INTO tx_count
                SORT tx_count DESC
                LIMIT @limit
                RETURN { source: from_addr, target: to_addr, tx_count: tx_count }
        ''', bind_vars={'mint': mint, 'holderRefs': holder_refs, 'limit': limit})

    def holder_funding(self, holder_addrs):
        """Get funded_by data for holder addresses.

        Uses vertex-centric index on [edge_type, _to].
        """
        holder_refs = [f'addresses/{a}' for a in holder_addrs]
        results = self.query('''
            LET holder_set = @holders
            LET holder_refs = @holderRefs
            LET funding_info = (
                FOR addr IN holder_set
                    LET doc = DOCUMENT(CONCAT("addresses/", addr))
                    FILTER doc != null AND doc.funded_by != null AND doc.funded_by != "none"
                    RETURN {
                        address: addr,
                        funded_by: doc.funded_by,
                        funded_by_tx: doc.funded_by_tx,
                        funded_by_time: doc.funded_by_time
                    }
            )
            LET funded_by_edges = (
                FOR e IN edges
                    FILTER e.edge_type == "funded_by"
                    FILTER e._to IN holder_refs
                    RETURN {
                        funder: SUBSTRING(e._from, 10),
                        funded: SUBSTRING(e._to, 10),
                        signature: e.signature,
                        block_time: e.block_time
                    }
            )
            RETURN { funding_info, funded_by_edges }
        ''', bind_vars={'holders': holder_addrs, 'holderRefs': holder_refs})
        return results[0] if results else {'funding_info': [], 'funded_by_edges': []}

    def address_neighbors(self, address, depth=1, edge_types=None, limit=100):
        """Graph traversal: find neighbors of an address up to N hops.

        This uses ArangoDB's native TRAVERSAL which leverages the edge index
        directly — much faster than manual FROM/TO filtering.
        """
        edge_filter = ''
        if edge_types:
            types_str = ', '.join(f'"{t}"' for t in edge_types)
            edge_filter = f'FILTER e.edge_type IN [{types_str}]'

        return self.query(f'''
            FOR v, e, p IN 1..@depth ANY CONCAT("addresses/", @addr)
                GRAPH "t16o_graph"
                {edge_filter}
                LIMIT @limit
                RETURN DISTINCT {{
                    vertex: {{ address: v._key, type: v.type, label: v.label }},
                    edge: {{ type: e.edge_type, token: e.token, amount: e.amount,
                             block_time: e.block_time, signature: e.signature }}
                }}
        ''', bind_vars={'addr': address, 'depth': depth, 'limit': limit})

    def funding_chain(self, address, depth=5):
        """Traverse the funding chain: who funded this address, who funded them, etc.

        Uses directed traversal on funded_by edges only.
        """
        return self.query('''
            FOR v, e, p IN 1..@depth OUTBOUND CONCAT("addresses/", @addr)
                GRAPH "t16o_graph"
                FILTER e.edge_type == "funded_by"
                RETURN {
                    depth: LENGTH(p.edges),
                    funder: v._key,
                    funder_type: v.type,
                    funder_label: v.label,
                    tx: e.signature,
                    block_time: e.block_time
                }
        ''', bind_vars={'addr': address, 'depth': depth})

    def common_counterparties(self, address1, address2, token=None):
        """Find addresses that both address1 and address2 transacted with.

        Useful for detecting shared infrastructure (same pool, same funder).
        """
        token_filter = 'FILTER e1.token == @token AND e2.token == @token' if token else ''
        bind = {'addr1': f'addresses/{address1}', 'addr2': f'addresses/{address2}'}
        if token:
            bind['token'] = f'tokens/{token}'

        return self.query(f'''
            LET neighbors1 = (
                FOR v, e1 IN 1..1 ANY @addr1 GRAPH "t16o_graph"
                    {token_filter.replace('e2.token == @token AND ', '') if token else ''}
                    RETURN DISTINCT v._key
            )
            LET neighbors2 = (
                FOR v, e2 IN 1..1 ANY @addr2 GRAPH "t16o_graph"
                    {token_filter.replace('e1.token == @token AND ', '') if token else ''}
                    RETURN DISTINCT v._key
            )
            LET common = INTERSECTION(neighbors1, neighbors2)
            FOR addr IN common
                LET doc = DOCUMENT(CONCAT("addresses/", addr))
                RETURN {{ address: addr, type: doc.type, label: doc.label }}
        ''', bind_vars=bind)

    def count(self, collection):
        """Get document count for a collection."""
        return self.db.collection(collection).count()

    def query(self, aql, bind_vars=None, **kwargs):
        """Execute an AQL query and return results as list."""
        return list(self.db.aql.execute(aql, bind_vars=bind_vars, **kwargs))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_indexes(collection, field_lists):
    """Ensure persistent indexes exist on a collection."""
    existing = set()
    for idx in collection.indexes():
        if idx.get('fields'):
            existing.add(tuple(idx['fields']))

    for fields in field_lists:
        if tuple(fields) not in existing:
            try:
                collection.add_persistent_index(fields=fields)
            except Exception:
                pass
