#!/usr/bin/env python3
"""
token_api.py — GET /api/token/{mint} handler
T16O GraphRAG gateway read path

Cache-first, map-driven response assembly.
Each section of the response is stored independently in Redis.
The handler resolves a named profile or ad-hoc params into a
request spec, fetches only the needed sections, and assembles
exactly the payload the client asked for.

─── URL shapes ───────────────────────────────────────────────
  GET /api/token/{mint}                        → profile=full (default)
  GET /api/token/{mint}?profile=fast           → nodes+edges+holders, no signals
  GET /api/token/{mint}?profile=funder         → nodes+edges+shared_funder+pool_chain
  GET /api/token/{mint}?profile=activity       → nodes+edges+bidirectional+fan_out
  GET /api/token/{mint}?profile=pools          → nodes+edges+shared_pool+pool_chain
  GET /api/token/{mint}?profile=signals_only   → nodes+all signals, no edges/holders
  GET /api/token/{mint}?signals=fan_out,shared_funder&sections=nodes,edges  → ad-hoc

─── Redis key layout ─────────────────────────────────────────
  graph:{mint}:meta          STRING  ~250B
  graph:{mint}:nodes         STRING  ~60KB
  graph:{mint}:edges         STRING  ~100KB
  graph:{mint}:holders       STRING  ~12KB
  graph:{mint}:swapstats     STRING  ~120B
  graph:{mint}:signals       HASH    one field per signal type (no wallet lists)
  graph:{mint}:fanout:{addr} STRING  wallet list for that distributor
  graph:{mint}:complete      STRING  sentinel — set last, checked first

─── Framework adapters ───────────────────────────────────────
  FastAPI / Flask examples at bottom of file.

─── Cache invalidation ───────────────────────────────────────
  Call cache_invalidate(rdb, mint) from get-top-holders.py
  at the end of load_token() after a pipeline completes.
"""

import os
import time
import logging
from collections import defaultdict

import ujson
import redis as redis_lib
from arango import ArangoClient

log = logging.getLogger('token_api')


# =============================================================================
# Config
# =============================================================================

REDIS_HOST  = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT  = int(os.getenv('REDIS_PORT', 6379))
ARANGO_HOST = os.getenv('ARANGO_HOST', 'localhost')
ARANGO_PORT = int(os.getenv('ARANGO_PORT', 8529))
ARANGO_USER = os.getenv('ARANGO_USER', 'root')
ARANGO_PASS = os.getenv('ARANGO_PASS', '')
ARANGO_DB   = os.getenv('ARANGO_DB',   't16o_guide')

CACHE_TTL      = int(os.getenv('CACHE_TTL',     300))  # 5 min — signals/edges/holders
CACHE_NODE_TTL = int(os.getenv('CACHE_NODE_TTL', 600)) # 10 min — nodes change slowly
CACHE_ENABLED  = os.getenv("CACHE_ENABLED", "0") == "1"   # disabled until cache invalidation is solid
EDGE_MIN_TX    = int(os.getenv('EDGE_MIN_TX',    10))
TOP_HOLDERS    = int(os.getenv('TOP_HOLDERS',    250))

# Fan-out thresholds — mirror graph.js constants
FAN_MIN_TARGETS = 15
FAN_MIN_TX      = 300
FAN_MIN_RATIO   = 5


# =============================================================================
# Declarative maps
# =============================================================================

# ── Section registry ──────────────────────────────────────────────────────────
# Each entry: param_key → (redis_suffix, is_required, response_key, ttl)
#
# is_required=True  → a Redis miss forces a full cold-path ArangoDB rebuild.
# is_required=False → section is simply omitted from the response if not cached
#                     (caller explicitly asked for it but we don't force a rebuild).
SECTIONS = {
    'meta':      ('meta',      True,  'meta',      CACHE_TTL),
    'nodes':     ('nodes',     True,  'nodes',     CACHE_NODE_TTL),
    'edges':     ('edges',     False, 'edges',     CACHE_TTL),
    'holders':   ('holders',   False, 'holders',   CACHE_TTL),
    'swapstats': ('swapstats', False, 'swapStats', CACHE_TTL),
}

# ── Signal type registry ──────────────────────────────────────────────────────
# param_name → (redis_hash_field, has_separate_wallet_keys)
#
# has_separate_wallet_keys=True means wallet lists are stored per-distributor
# at graph:{mint}:fanout:{addr} to keep the signals hash lean.
SIGNAL_TYPES = {
    'shared_funder': ('shared_funder',         False),
    'shared_pool':   ('shared_pool',            False),
    'bidirectional': ('bidirectional_transfer', False),
    'fan_out':       ('fan_out',                True),
    'pool_chain':    ('pool_chain',             False),
    'shared_fee_payer': ('shared_fee_payer',     False),
}

ALL_SIGNALS = list(SIGNAL_TYPES.keys())

# ── Request profiles ──────────────────────────────────────────────────────────
# Named presets. Client sends ?profile=xxx for common cases,
# or ?sections=...&signals=... for ad-hoc assembly.
PROFILES = {
    # Phase-1 render: layout immediately visible, no signal processing
    'fast': {
        'sections': ['meta', 'nodes', 'edges', 'holders'],
        'signals':  [],
    },
    # Full payload — everything
    'full': {
        'sections': ['meta', 'nodes', 'edges', 'holders', 'swapstats'],
        'signals':  ALL_SIGNALS,
    },
    # Graph layout + all signals, skip holder/swap overhead
    'graph_only': {
        'sections': ['meta', 'nodes', 'edges'],
        'signals':  ALL_SIGNALS,
    },
    # Signal refresh — client already has nodes/edges in holderData._raw
    'signals_only': {
        'sections': ['meta', 'nodes'],
        'signals':  ALL_SIGNALS,
    },
    # Ownership / control signals
    'funder': {
        'sections': ['meta', 'nodes', 'edges'],
        'signals':  ['shared_funder', 'pool_chain'],
    },
    # Activity / behaviour signals
    'activity': {
        'sections': ['meta', 'nodes', 'edges'],
        'signals':  ['bidirectional', 'fan_out'],
    },
    # Pool-focused signals
    'pools': {
        'sections': ['meta', 'nodes', 'edges'],
        'signals':  ['shared_pool', 'pool_chain'],
    },
    # Coordination signals (excludes structural pool/funder relationships)
    'coordination': {
        'sections': ['meta', 'nodes', 'edges'],
        'signals':  ['shared_funder', 'bidirectional', 'fan_out'],
    },
}

DEFAULT_PROFILE = 'full'


# =============================================================================
# Connections
# =============================================================================

def get_redis() -> redis_lib.Redis:
    return redis_lib.Redis(
        host=REDIS_HOST, port=REDIS_PORT,
        decode_responses=True, socket_timeout=2,
    )


def get_db():
    client = ArangoClient(hosts=f'http://{ARANGO_HOST}:{ARANGO_PORT}')
    return client.db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASS)


# =============================================================================
# Redis key helpers
# =============================================================================

def _k(mint: str, suffix: str) -> str:
    return f'graph:{mint}:{suffix}'


def _k_complete(mint: str) -> str:
    """Write-fence sentinel. Set last during cache_set_decomposed."""
    return f'graph:{mint}:complete'


def _k_fanout(mint: str, addr: str) -> str:
    """Per-distributor wallet list for fan_out signals."""
    return f'graph:{mint}:fanout:{addr}'


# =============================================================================
# Request spec resolution
# =============================================================================

def resolve_spec(profile: str = None,
                 sections: str = None,
                 signals: str = None) -> dict:
    """
    Build a normalised request spec from a named profile and/or explicit params.
    Explicit params always win over the profile default.

    Returns:
        {
            'sections': ['meta', 'nodes', ...],   # subset of SECTIONS keys
            'signals':  ['shared_funder', ...],   # subset of SIGNAL_TYPES keys
        }
    """
    base = profile if profile in PROFILES else DEFAULT_PROFILE
    spec = {
        'sections': list(PROFILES[base]['sections']),
        'signals':  list(PROFILES[base]['signals']),
    }

    if sections:
        parsed = [s.strip() for s in sections.split(',')]
        spec['sections'] = [s for s in parsed if s in SECTIONS]

    if signals:
        parsed = [s.strip() for s in signals.split(',')]
        spec['signals'] = [s for s in parsed if s in SIGNAL_TYPES]

    # meta and nodes are always required regardless of what was asked
    for key in ('meta', 'nodes'):
        if key not in spec['sections']:
            spec['sections'].insert(0, key)

    return spec


# =============================================================================
# Cache — decomposed write
# =============================================================================

def cache_set_decomposed(rdb: redis_lib.Redis, mint: str, data: dict) -> None:
    """
    Write every section and signal type to its own Redis key.

    Key strategy:
      - Each SECTION → independent STRING key with its own TTL
      - All signal types → fields in a single HASH key (lean — no wallet lists)
      - fan_out wallet lists → one STRING key per distributor address
      - complete sentinel → set LAST as a write fence

    Any reader checks the complete sentinel before reading other keys.
    A missing sentinel means a partial write — treat as a full cache miss.
    """
    pipe = rdb.pipeline(transaction=False)

    # ── Sections ──────────────────────────────────────────────
    pipe.set(_k(mint, 'meta'),      ujson.dumps(data.get('meta',      {})),  ex=CACHE_TTL)
    pipe.set(_k(mint, 'nodes'),     ujson.dumps(data.get('nodes',     [])),  ex=CACHE_NODE_TTL)
    pipe.set(_k(mint, 'edges'),     ujson.dumps(data.get('edges',     [])),  ex=CACHE_TTL)
    pipe.set(_k(mint, 'holders'),   ujson.dumps(data.get('holders',   [])),  ex=CACHE_TTL)
    pipe.set(_k(mint, 'swapstats'), ujson.dumps(data.get('swapStats', [])),  ex=CACHE_TTL)

    # ── Signals hash ──────────────────────────────────────────
    all_signals = data.get('signals', {})
    sig_map: dict = {}

    for param_name, (hash_field, has_wallet_keys) in SIGNAL_TYPES.items():
        # data['signals'] is keyed by hash_field (the canonical form)
        sig_list = all_signals.get(hash_field) or all_signals.get(param_name) or []

        if not sig_list:
            sig_map[hash_field] = ujson.dumps([])
            continue

        if has_wallet_keys:
            # Write per-distributor wallet lists to separate keys
            for entry in sig_list:
                addr    = entry.get('signal_value', '')
                wallets = entry.get('wallets', [])
                if addr and wallets:
                    pipe.set(_k_fanout(mint, addr), ujson.dumps(wallets), ex=CACHE_TTL)
            # Strip wallets from hash field — keeps HGETALL lean
            trimmed = [{k: v for k, v in e.items() if k != 'wallets'} for e in sig_list]
            sig_map[hash_field] = ujson.dumps(trimmed)
        else:
            sig_map[hash_field] = ujson.dumps(sig_list)

    pipe.delete(_k(mint, 'signals'))
    pipe.hset(_k(mint, 'signals'), mapping=sig_map)
    pipe.expire(_k(mint, 'signals'), CACHE_TTL)

    # ── Write fence (set last) ────────────────────────────────
    pipe.set(_k_complete(mint), '1', ex=CACHE_TTL)

    pipe.execute()
    log.debug(f'[cache] wrote decomposed keys for {mint[:16]}...')


# =============================================================================
# Cache — decomposed read
# =============================================================================

def cache_get_decomposed(rdb: redis_lib.Redis, mint: str,
                          spec: dict) -> dict:
    """
    Fetch exactly the sections and signals described by spec from Redis.

    Returns None (full cache miss) when:
      - The complete sentinel is absent (partial write or expired)
      - Any required section key is missing

    On success returns a fully assembled response dict containing
    only the sections and signals that were requested.
    """
    # Write-fence check — avoids reading partial cache state
    if not rdb.exists(_k_complete(mint)):
        return None

    needed_sections = spec['sections']
    needed_signals  = spec['signals']

    # ── Fetch sections in one pipeline ───────────────────────
    pipe = rdb.pipeline(transaction=False)
    for s in needed_sections:
        pipe.get(_k(mint, SECTIONS[s][0]))
    section_raws = pipe.execute()

    # Required section missing → full miss
    for s, raw in zip(needed_sections, section_raws):
        if raw is None and SECTIONS[s][1]:
            log.debug(f'[cache] required section "{s}" missing for {mint[:16]}')
            return None

    # Assemble sections into response
    response: dict = {}
    for s, raw in zip(needed_sections, section_raws):
        resp_key = SECTIONS[s][2]
        if raw is not None:
            response[resp_key] = ujson.loads(raw)
        else:
            response[resp_key] = {} if resp_key == 'meta' else []

    # ── Fetch signals from hash ───────────────────────────────
    if not needed_signals:
        return response

    hash_fields     = [SIGNAL_TYPES[p][0] for p in needed_signals]
    has_wallet_keys = [SIGNAL_TYPES[p][1] for p in needed_signals]

    sig_raws = rdb.hmget(_k(mint, 'signals'), hash_fields)

    # Collect all fan_out wallet key lookups needed
    fanout_lookups: list = []  # (sig_index, entry_index, addr)
    for i, (raw, hw) in enumerate(zip(sig_raws, has_wallet_keys)):
        if raw and hw:
            for j, entry in enumerate(ujson.loads(raw)):
                addr = entry.get('signal_value', '')
                if addr:
                    fanout_lookups.append((i, j, addr))

    # Batch-fetch wallet lists in one pipeline
    wallet_map: dict = {}
    if fanout_lookups:
        wp = rdb.pipeline(transaction=False)
        for _, _, addr in fanout_lookups:
            wp.get(_k_fanout(mint, addr))
        for (_, _, addr), wl_raw in zip(fanout_lookups, wp.execute()):
            wallet_map[addr] = ujson.loads(wl_raw) if wl_raw else []

    # Build signals dict keyed by hash_field (matches client expectation)
    signals: dict = {}
    for param_name, hash_field, raw, hw in zip(
        needed_signals, hash_fields, sig_raws, has_wallet_keys
    ):
        if raw is None:
            signals[hash_field] = []
            continue
        parsed = ujson.loads(raw)
        if hw:
            for entry in parsed:
                addr = entry.get('signal_value', '')
                entry['wallets'] = wallet_map.get(addr, [])
        signals[hash_field] = parsed

    response['signals'] = signals
    return response


# =============================================================================
# Cache invalidation
# =============================================================================

def cache_invalidate(rdb: redis_lib.Redis, mint: str) -> None:
    """
    Wipe all cached keys for a mint so the next request hits ArangoDB.

    Call from get-top-holders.py after a pipeline completes:

        from token_api import cache_invalidate, get_redis
        try:
            cache_invalidate(get_redis(), mint)
            log(TAG, f'Cache invalidated for {mint[:16]}...')
        except Exception as e:
            log(TAG, f'Cache invalidate failed (non-fatal): {e}')
    """
    pipe = rdb.pipeline(transaction=False)

    # Delete sentinel first so any in-flight reader sees a miss immediately
    pipe.delete(_k_complete(mint))

    for suffix in ('meta', 'nodes', 'edges', 'holders', 'swapstats', 'signals'):
        pipe.delete(_k(mint, suffix))

    # SCAN for fan-out wallet keys (avoids blocking on large keyspaces)
    cursor = 0
    pattern = _k_fanout(mint, '*')
    while True:
        cursor, keys = rdb.scan(cursor, match=pattern, count=100)
        if keys:
            pipe.delete(*keys)
        if cursor == 0:
            break

    pipe.execute()
    log.info(f'[cache] invalidated {mint[:16]}...')


# =============================================================================
# Response assembly from in-memory data (cold path)
# =============================================================================

def assemble_response(data: dict, spec: dict) -> dict:
    """
    Build a response dict from a fully-loaded in-memory data dict,
    filtered to only the sections and signals described by spec.

    Used on the cold path after a fresh ArangoDB query so we don't
    return more than the client asked for even on a cache miss.
    """
    response: dict = {}

    # Map spec section keys → data keys and defaults
    data_keys = {
        'meta':      ('meta',      {}),
        'nodes':     ('nodes',     []),
        'edges':     ('edges',     []),
        'holders':   ('holders',   []),
        'swapstats': ('swapStats', []),
    }

    for s in spec['sections']:
        data_key, default = data_keys[s]
        response[SECTIONS[s][2]] = data.get(data_key, default)

    if spec['signals']:
        all_signals = data.get('signals', {})
        signals_out: dict = {}
        for param_name in spec['signals']:
            hash_field = SIGNAL_TYPES[param_name][0]
            signals_out[hash_field] = all_signals.get(hash_field) or []
        response['signals'] = signals_out

    return response


# =============================================================================
# ArangoDB queries
# =============================================================================

def query_token_meta(db, mint: str) -> dict:
    cursor = db.aql.execute(
        'FOR t IN token FILTER t._key == @mint LIMIT 1 RETURN t',
        bind_vars={'mint': mint},
    )
    docs = list(cursor)
    if not docs:
        return {}
    t = docs[0]
    return {
        'mint':       t.get('token_address', mint),
        'symbol':     t.get('token_symbol'),
        'name':       t.get('token_name'),
        'decimals':   t.get('decimals'),
        'icon':       t.get('token_icon'),
        'first_seen': t.get('first_seen'),
        'last_seen':  t.get('last_seen'),
    }


def query_holders(db, mint: str, top_n: int = TOP_HOLDERS) -> list:
    cursor = db.aql.execute(
        '''
        FOR h IN holder
            FILTER h.mint == @mint
            SORT h.rank ASC
            LIMIT @top_n
            RETURN {
                owner:      h.owner,
                amount:     h.amount,
                decimals:   h.decimals,
                percentage: h.percentage,
                rank:       h.rank
            }
        ''',
        bind_vars={'mint': mint, 'top_n': top_n},
    )
    return list(cursor)


def query_nodes_and_edges(db, mint: str, edge_min_tx: int = EDGE_MIN_TX) -> tuple:
    """
    Returns (nodes, edges).
    Nodes include holder snapshot data joined from the holder collection.
    Edges are aggregated directed owner pairs with tx_count >= edge_min_tx.
    """
    node_cursor = db.aql.execute(
        '''
        LET holder_addrs = (FOR h IN holder FILTER h.mint == @mint RETURN h.owner)

        // Find all addresses on qualifying edges (holder on at least one side)
        LET edge_addrs = (
            FOR t IN transfer
                FILTER t.token_address == @mint
                FILTER t.source_owner IN holder_addrs OR t.destination_owner IN holder_addrs
                FILTER t.source_owner != t.destination_owner
                COLLECT
                    source = t.source_owner,
                    target = t.destination_owner
                AGGREGATE tx_count = COUNT(1)
                FILTER tx_count >= @min_tx
                RETURN DISTINCT UNION_DISTINCT([source], [target])
        )
        LET all_addrs = UNION_DISTINCT(holder_addrs, FLATTEN(edge_addrs))

        FOR addr IN all_addrs
            LET a = DOCUMENT(CONCAT("address/", addr))
            LET h = FIRST(
                FOR hh IN holder
                    FILTER hh.mint == @mint
                    FILTER hh.owner == addr
                    LIMIT 1
                    RETURN hh
            )
            LET tx_count = LENGTH(
                FOR t IN transfer
                    FILTER t.token_address == @mint
                    FILTER t.source_owner == addr
                       OR  t.destination_owner == addr
                    RETURN 1
            )
            RETURN {
                address:        addr,
                tx_count:       tx_count,
                type:           a.type           OR "unknown",
                label:          a.label,
                funded_by:      a.funded_by,
                funded_by_tx:   a.funded_by_tx,
                funded_by_time: a.funded_by_time,
                tags:           a.tags           OR [],
                holder_rank:    h.rank,
                holder_pct:     h.percentage,
                holder_amount:  h.amount,
                fee_payer:      a.fee_payer,
                fee_payer_tx_count: a.fee_payer_tx_count
            }
        ''',
        bind_vars={'mint': mint, 'min_tx': edge_min_tx},
        stream=True,
    )
    nodes = list(node_cursor)

    edge_cursor = db.aql.execute(
        '''
        LET holder_addrs = (FOR h IN holder FILTER h.mint == @mint RETURN h.owner)
        FOR t IN transfer
            FILTER t.token_address == @mint
            FILTER t.source_owner IN holder_addrs OR t.destination_owner IN holder_addrs
            FILTER t.source_owner != t.destination_owner
            COLLECT
                source = t.source_owner,
                target = t.destination_owner
            AGGREGATE
                tx_count     = COUNT(1),
                total_amount = SUM(TO_NUMBER(t.amount_str)),
                first_tx     = MIN(t.block_time),
                last_tx      = MAX(t.block_time)
            FILTER tx_count >= @min_tx
            LET sa = DOCUMENT(CONCAT("address/", source))
            LET ta = DOCUMENT(CONCAT("address/", target))
            RETURN {
                source:           source,
                target:           target,
                tx_count:         tx_count,
                total_amount:     total_amount,
                first_tx:         first_tx,
                last_tx:          last_tx,
                edge_type:        "spl_transfer",
                source_type:      sa.type  OR "unknown",
                target_type:      ta.type  OR "unknown",
                source_label:     sa.label OR "",
                target_label:     ta.label OR "",
                source_funded_by: sa.funded_by,
                target_funded_by: ta.funded_by
            }
        ''',
        bind_vars={'mint': mint, 'min_tx': edge_min_tx},
        stream=True,
    )
    edges = list(edge_cursor)

    return nodes, edges


def query_swap_stats(db, mint: str) -> list:
    cursor = db.aql.execute(
        '''
        FOR a IN activity
            FILTER a.token_in == @mint OR a.token_out == @mint
            COLLECT
                direction = (a.token_out == @mint ? "buy" : "sell")
            AGGREGATE
                count          = COUNT(1),
                unique_wallets = COUNT_DISTINCT(a.account)
            RETURN { direction, count, unique_wallets }
        ''',
        bind_vars={'mint': mint},
    )
    return list(cursor)


# =============================================================================
# Signal computation
# =============================================================================

def compute_signals(nodes: list, edges: list) -> dict:
    """
    Derive all five signal types from node/edge data.
    Mirrors the detection logic in graph.js so the client receives
    pre-computed signals rather than deriving them on every render.

    Returns dict keyed by hash_field (not param_name), e.g.:
        { 'bidirectional_transfer': [...], 'fan_out': [...], ... }
    """
    addr_set     = {n['address'] for n in nodes}
    node_by_addr = {n['address']: n for n in nodes}
    pool_addrs   = {n['address'] for n in nodes if n.get('type') == 'pool'}

    # ── shared_funder ─────────────────────────────────────────
    funder_groups: dict = defaultdict(list)
    for n in nodes:
        fb = n.get('funded_by')
        if fb and fb not in ('none', ''):
            funder_groups[fb].append(n['address'])

    shared_funder = [
        {'signal_type': 'shared_funder', 'signal_value': funder,
         'wallets': wallets, 'strength': 1}
        for funder, wallets in funder_groups.items()
        if len(wallets) >= 2
    ]

    # ── shared_pool ───────────────────────────────────────────
    pool_traders: dict = defaultdict(list)
    for e in edges:
        if e['target'] in pool_addrs and e['source'] in addr_set:
            pool_traders[e['target']].append(e['source'])

    shared_pool = []
    for pool_addr, traders in pool_traders.items():
        uniq = list(dict.fromkeys(traders))
        if len(uniq) >= 2:
            pn = node_by_addr.get(pool_addr, {})
            shared_pool.append({
                'signal_type':  'shared_pool',
                'signal_value': pool_addr,
                'pool_name':    pn.get('label') or '',
                'wallets':      uniq,
                'strength':     0.18,
            })

    # ── bidirectional_transfer ────────────────────────────────
    forward: dict = {(e['source'], e['target']): e['tx_count'] for e in edges}
    bidir_seen: set = set()
    bidirectional_transfer = []

    for e in edges:
        rev = (e['target'], e['source'])
        fs  = frozenset([e['source'], e['target']])
        if rev in forward and fs not in bidir_seen:
            bidir_seen.add(fs)
            combined = e['tx_count'] + forward[rev]
            bidirectional_transfer.append({
                'signal_type':  'bidirectional_transfer',
                'wallet_a':     e['source'],
                'wallet_b':     e['target'],
                'signal_value': combined,
                'strength':     min(1.0, combined / 10),
            })

    # ── fan_out ───────────────────────────────────────────────
    out_deg: dict = defaultdict(set)
    out_tx:  dict = defaultdict(int)
    in_deg:  dict = defaultdict(set)
    for e in edges:
        out_deg[e['source']].add(e['target'])
        out_tx[e['source']] += e['tx_count']
        in_deg[e['target']].add(e['source'])

    fan_out = []
    for n in nodes:
        addr = n['address']
        od   = len(out_deg.get(addr, set()))
        id_  = len(in_deg.get(addr, set()))
        tx   = out_tx.get(addr, 0)
        if od >= FAN_MIN_TARGETS and tx >= FAN_MIN_TX and od / (id_ + 1) >= FAN_MIN_RATIO:
            fan_out.append({
                'signal_type':  'fan_out',
                'signal_value': addr,
                'wallet_count': od,
                'out_tx':       tx,
                'in_deg':       id_,
                'wallets':      list(out_deg[addr]),
                'strength':     1,
            })

    # ── pool_chain ────────────────────────────────────────────
    pool_funder_pools: dict = defaultdict(list)
    for n in nodes:
        if n.get('type') == 'pool':
            fb = n.get('funded_by')
            if fb and fb not in ('none', '') and fb in addr_set:
                pool_funder_pools[fb].append({
                    'address': n['address'],
                    'label':   n.get('label') or '',
                })

    pool_chain = [
        {'signal_type':  'pool_chain', 'signal_value': fa,
         'pools': ps, 'pool_count': len(ps), 'strength': 1}
        for fa, ps in pool_funder_pools.items() if ps
    ]

    # ── shared_fee_payer ─────────────────────────────────────
    fee_payer_groups = defaultdict(list)
    for n in nodes:
        fp = n.get("fee_payer")
        if fp and fp not in ("none", ""):
            fee_payer_groups[fp].append(n["address"])

    shared_fee_payer = [
        {"signal_type": "shared_fee_payer", "signal_value": fp,
         "wallets": wallets, "strength": 1}
        for fp, wallets in fee_payer_groups.items()
        if 2 <= len(wallets) <= 5  # >5 is likely a bundler/MEV bot, not coordination
    ]

    # Keyed by hash_field — matches SIGNAL_TYPES registration
    return {
        'shared_funder':          shared_funder,
        'shared_pool':            shared_pool,
        'bidirectional_transfer': bidirectional_transfer,
        'fan_out':                fan_out,
        'pool_chain':             pool_chain,
        "shared_fee_payer":       shared_fee_payer,
    }


# =============================================================================
# Cold-path builder
# =============================================================================

def build_token_response(db, mint: str) -> dict:
    """
    Full ArangoDB query. Computes all signals.
    Returns the complete data dict. Caller passes it to
    cache_set_decomposed() then assemble_response().
    """
    t0 = time.perf_counter()

    meta         = query_token_meta(db, mint)
    holders      = query_holders(db, mint)
    nodes, edges = query_nodes_and_edges(db, mint)
    swap_stats   = query_swap_stats(db, mint)
    signals      = compute_signals(nodes, edges)

    # Inject signal-referenced addresses not already in the node set:
    #   1. Funder/fee_payer addresses (signal_value in shared_funder/shared_fee_payer)
    #   2. Member wallets listed in signal wallets[] arrays
    node_addrs = {n["address"] for n in nodes}
    missing_addrs = set()
    for sig_key in ("shared_funder", "shared_fee_payer"):
        for s in signals.get(sig_key, []):
            # The funder itself
            fa = s.get("signal_value", "")
            if fa and fa != "none" and fa not in node_addrs:
                missing_addrs.add(fa)
            # All wallets in the group
            for w in s.get("wallets", []):
                if w and w not in node_addrs:
                    missing_addrs.add(w)

    if missing_addrs:
        col = db.collection("address")
        docs = col.get_many(list(missing_addrs))
        for doc in docs:
            if not doc:
                continue
            nodes.append({
                "address":        doc["_key"],
                "tx_count":       0,
                "type":           doc.get("type", "wallet"),
                "label":          doc.get("label"),
                "funded_by":      doc.get("funded_by"),
                "funded_by_tx":   doc.get("funded_by_tx"),
                "funded_by_time": doc.get("funded_by_time"),
                "tags":           doc.get("tags", []),
                "holder_rank":    None,
                "holder_pct":     None,
                "holder_amount":  None,
                "fee_payer":      doc.get("fee_payer"),
                "fee_payer_tx_count": doc.get("fee_payer_tx_count"),
                "is_signal_node": True,
            })
            node_addrs.add(doc["_key"])
        # Stub nodes for addresses not yet in the address collection
        found = {doc["_key"] for doc in docs if doc}
        for addr in missing_addrs - found:
            nodes.append({
                "address": addr, "tx_count": 0, "type": "wallet",
                "label": None, "funded_by": None, "tags": [],
                "holder_rank": None, "holder_pct": None, "holder_amount": None,
                "fee_payer": None, "fee_payer_tx_count": None,
                "is_signal_node": True,
            })
        log.info(f"[token_api] injected {len(missing_addrs)} signal nodes for {mint[:16]}...")

    elapsed = round((time.perf_counter() - t0) * 1000)
    log.info(
        f'[token_api] cold build {mint[:16]}... '
        f'nodes={len(nodes)} edges={len(edges)} '
        f'holders={len(holders)} elapsed={elapsed}ms'
    )

    return {
        'meta': {
            'token':      meta,
            'mode':       'mint',
            'edgeMinTx':  EDGE_MIN_TX,
            'elapsed_ms': elapsed,
        },
        'nodes':     nodes,
        'edges':     edges,
        'signals':   signals,
        'holders':   holders,
        'swapStats': swap_stats,
    }


# =============================================================================
# Main handler
# =============================================================================

def handle_get_token(
    mint:     str,
    profile:  str = None,
    sections: str = None,
    signals:  str = None,
) -> tuple:
    """
    Core handler for GET /api/token/{mint}.

    Parameters (all optional — defaults to profile=full):
        profile   — named preset from PROFILES
        sections  — comma-separated section keys (overrides profile)
        signals   — comma-separated signal param names (overrides profile)

    Returns (response_dict, http_status_code).

    Cache flow:
        1. Resolve spec from params
        2. Check complete sentinel in Redis
        3. HIT  — fetch only the needed sections/signals, assemble, return
        4. MISS — query ArangoDB, write all sections, assemble spec, return
    """
    if not mint or len(mint) < 32:
        return {'error': 'invalid mint address'}, 400

    spec = resolve_spec(profile, sections, signals)
    log.debug(f'[token_api] {mint[:16]} spec={spec}')

    # ── Cache read ────────────────────────────────────────────
    rdb = None
    try:
        rdb = get_redis()
        cached = cache_get_decomposed(rdb, mint, spec) if CACHE_ENABLED else None
        if cached is not None:
            cached['_cache']   = 'hit'
            cached['_profile'] = profile or ('custom' if (sections or signals) else DEFAULT_PROFILE)
            return cached, 200
    except redis_lib.RedisError as e:
        log.warning(f'[cache] Redis read error {mint[:16]}: {e} — falling through to ArangoDB')

    # ── Cold path ─────────────────────────────────────────────
    try:
        data = build_token_response(get_db(), mint)
    except Exception as e:
        log.error(f'[token_api] ArangoDB query failed {mint[:16]}: {e}')
        return {'error': f'query failed: {str(e)}'}, 500

    # Populate cache — best-effort, never fails the request
    if rdb is not None and CACHE_ENABLED:
        try:
            cache_set_decomposed(rdb, mint, data) if data.get("nodes") else None
        except redis_lib.RedisError as e:
            log.warning(f'[cache] Redis write error {mint[:16]}: {e}')

    # Assemble only what the spec asked for
    response = assemble_response(data, spec)
    response['_cache']   = 'miss'
    response['_profile'] = profile or ('custom' if (sections or signals) else DEFAULT_PROFILE)
    return response, 200


# =============================================================================
# Framework adapters
# =============================================================================

# ┌─ FastAPI ───────────────────────────────────────────────────────────────────
# │
# │  from fastapi import APIRouter, Query
# │  from fastapi.responses import ORJSONResponse
# │  from token_api import handle_get_token
# │
# │  router = APIRouter()
# │
# │  @router.get('/api/token/{mint}')
# │  async def get_token(
# │      mint:     str,
# │      profile:  str = Query(None),
# │      sections: str = Query(None),
# │      signals:  str = Query(None),
# │  ):
# │      data, status = handle_get_token(mint, profile, sections, signals)
# │      return ORJSONResponse(content=data, status_code=status)
# │
# └─────────────────────────────────────────────────────────────────────────────

# ┌─ Flask ─────────────────────────────────────────────────────────────────────
# │
# │  from flask import Blueprint, request, jsonify
# │  from token_api import handle_get_token
# │
# │  bp = Blueprint('token', __name__)
# │
# │  @bp.route('/api/token/<mint>')
# │  def get_token(mint):
# │      data, status = handle_get_token(
# │          mint,
# │          profile  = request.args.get('profile'),
# │          sections = request.args.get('sections'),
# │          signals  = request.args.get('signals'),
# │      )
# │      return jsonify(data), status
# │
# └─────────────────────────────────────────────────────────────────────────────
