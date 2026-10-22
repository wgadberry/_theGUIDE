"""
decode_v2 - Pre-resolved decode path for guide-decoder.py

Parses Solscan JSON once in Python, resolves all string addresses to integer
IDs via a thread-local cache, INSERTs new lookup rows directly, populates
temp tables, and calls sp_tx_parse_decode_v2 which does pure INSERTs.

Usage from guide-decoder.py:
    from decode_v2 import process_decoded_v2, AddressCache
"""

import json
from datetime import datetime

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FEATURE_ALL_ADDRESSES = 2   # collect ATAs, vaults, pools
FEATURE_SWAP_ROUTING  = 4   # all swap hops vs top-level only

_CHUNK_SIZE = 500           # max IN-list size for batch SELECTs

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log(tag, msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}][{tag}] {msg}", flush=True)


def _safe_str(val):
    """Return None if val is None or the literal string 'null', else str(val)."""
    if val is None:
        return None
    s = str(val)
    if s == 'null':
        return None
    return s


def _chunks(lst, size):
    """Yield successive chunks of *size* from list *lst*."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


# ---------------------------------------------------------------------------
# AddressCache
# ---------------------------------------------------------------------------

class AddressCache:
    """Thread-local cache for address/token/program/pool ID resolution.

    Intended to be instantiated once per WorkerThread and reused across
    successive calls to process_decoded_v2.
    """

    def __init__(self):
        self.addr = {}      # address_str  -> tx_address.id
        self.token = {}     # mint_address_id (int) -> tx_token.id
        self.program = {}   # program_address_id (int) -> tx_program.id
        self.pool = {}      # pool_address_id (int) -> tx_pool.id

    def clear(self):
        """Call on DB reconnect to avoid stale IDs."""
        self.addr.clear()
        self.token.clear()
        self.program.clear()
        self.pool.clear()


# ---------------------------------------------------------------------------
# Extraction  (single pass)
# ---------------------------------------------------------------------------

def _collect_addr(addresses, addr_str, addr_type):
    """Add addr_str to the addresses dict with the given type.

    If the address already exists with a different type, keep the more
    specific one (wallet > ata > unknown; mint/program/pool always win).
    """
    val = _safe_str(addr_str)
    if val is None:
        return
    # Priority: pool > mint > program > vault > wallet > ata > unknown
    _PRIORITY = {
        'pool': 7, 'mint': 6, 'program': 5, 'vault': 4,
        'wallet': 3, 'ata': 2, 'unknown': 1,
    }
    existing = addresses.get(val)
    if existing is None or _PRIORITY.get(addr_type, 0) > _PRIORITY.get(existing, 0):
        addresses[val] = addr_type


def extract_all(data, features=31):
    """Single-pass extraction of all data from the Solscan response.

    Args:
        data: The parsed Solscan response dict (must have 'data' key).
        features: Feature bitmask.

    Returns:
        dict with keys: addresses, tx_rows, transfer_rows, swap_rows,
        activity_rows.
    """
    all_addresses = {}   # address_str -> type_str
    tx_rows = []
    transfer_rows = []
    swap_rows = []
    activity_rows = []

    collect_extended = bool(features & FEATURE_ALL_ADDRESSES)
    collect_all_hops = bool(features & FEATURE_SWAP_ROUTING)

    txs = data.get('data') or []

    for tx in txs:
        tx_hash = _safe_str(tx.get('tx_hash'))
        if tx_hash is None:
            continue

        block_id = tx.get('block_id')
        block_time = tx.get('block_time')
        fee = tx.get('fee')
        priority_fee = tx.get('priority_fee')

        # --- one_line_summary ---
        ols = tx.get('one_line_summary') or {}
        ols_data = ols.get('data') or {}
        ols_activity_type = _safe_str(ols.get('activity_type'))
        ols_program_id = _safe_str(ols.get('program_id'))
        ols_account = _safe_str(ols_data.get('account'))
        ols_token_1 = _safe_str(ols_data.get('token_1'))
        ols_token_2 = _safe_str(ols_data.get('token_2'))
        ols_amount_1 = ols_data.get('amount_1')
        ols_amount_2 = ols_data.get('amount_2')
        ols_decimal_1 = ols_data.get('token_decimal_1')
        ols_decimal_2 = ols_data.get('token_decimal_2')
        ols_fee_amount = ols_data.get('fee_ammount')
        ols_fee_token = _safe_str(ols_data.get('fee_token'))

        # Collect addresses from one_line_summary
        _collect_addr(all_addresses, ols_account, 'wallet')
        _collect_addr(all_addresses, ols_token_1, 'mint')
        _collect_addr(all_addresses, ols_token_2, 'mint')
        _collect_addr(all_addresses, ols_fee_token, 'mint')
        _collect_addr(all_addresses, ols_program_id, 'program')

        # Determine fallback signer from first transfer
        transfers = tx.get('transfers') or []
        fallback_signer = None
        if not ols_account and transfers:
            fallback_signer = _safe_str((transfers[0].get('source_owner')))

        signer = ols_account or fallback_signer

        tx_rows.append({
            'tx_hash': tx_hash,
            'block_id': block_id,
            'block_time': block_time,
            'fee': fee,
            'priority_fee': priority_fee,
            'activity_type': ols_activity_type,
            'signer': signer,               # address str, resolved later
            'agg_program': ols_program_id,   # address str, resolved later
            'token_1': ols_token_1,          # mint address str
            'token_2': ols_token_2,          # mint address str
            'amount_1': ols_amount_1,
            'amount_2': ols_amount_2,
            'decimal_1': ols_decimal_1,
            'decimal_2': ols_decimal_2,
            'fee_amount': ols_fee_amount,
            'fee_token': ols_fee_token,      # mint address str
        })

        # --- transfers ---
        for xfer in transfers:
            src = _safe_str(xfer.get('source'))
            src_owner = _safe_str(xfer.get('source_owner'))
            dst = _safe_str(xfer.get('destination'))
            dst_owner = _safe_str(xfer.get('destination_owner'))
            token_addr = _safe_str(xfer.get('token_address'))
            program_id = _safe_str(xfer.get('program_id'))
            outer_program_id = _safe_str(xfer.get('outer_program_id'))

            base_val = xfer.get('base_value') or {}
            base_token = _safe_str(base_val.get('token_address'))

            # Always collect these
            _collect_addr(all_addresses, src_owner, 'wallet')
            _collect_addr(all_addresses, dst_owner, 'wallet')
            _collect_addr(all_addresses, token_addr, 'mint')
            _collect_addr(all_addresses, program_id, 'program')
            _collect_addr(all_addresses, outer_program_id, 'program')
            if base_token:
                _collect_addr(all_addresses, base_token, 'mint')

            # Extended addresses (ATAs)
            if collect_extended:
                _collect_addr(all_addresses, src, 'ata')
                _collect_addr(all_addresses, dst, 'ata')

            transfer_rows.append({
                'tx_hash': tx_hash,
                'ins_index': xfer.get('ins_index'),
                'outer_ins_index': xfer.get('outer_ins_index'),
                'transfer_type': _safe_str(xfer.get('transfer_type')),
                'source': src,
                'source_owner': src_owner,
                'destination': dst,
                'destination_owner': dst_owner,
                'token_address': token_addr,
                'decimals': xfer.get('decimals'),
                'amount': xfer.get('amount'),
                'program_id': program_id,
                'outer_program_id': outer_program_id,
                'base_token_address': base_token,
                'base_decimals': base_val.get('decimals'),
                'base_amount': base_val.get('amount'),
            })

        # --- activities ---
        activities = tx.get('activities') or []
        for act in activities:
            ins_index = act.get('ins_index')
            outer_ins_index = act.get('outer_ins_index')

            # If not collecting all hops, skip inner instructions
            if not collect_all_hops and ins_index != outer_ins_index:
                continue

            act_data = act.get('data') or {}
            act_type = _safe_str(act.get('activity_type'))
            act_name = _safe_str(act.get('name'))
            act_program = _safe_str(act.get('program_id'))
            act_outer_program = _safe_str(act.get('outer_program_id'))
            act_account = _safe_str(act_data.get('account'))
            act_source = _safe_str(act_data.get('source'))
            act_new_account = _safe_str(act_data.get('new_account'))
            act_init_account = _safe_str(act_data.get('init_account'))
            act_amm_id = _safe_str(act_data.get('amm_id'))
            act_owner_1 = _safe_str(act_data.get('owner_1'))
            act_owner_2 = _safe_str(act_data.get('owner_2'))
            act_token_1 = _safe_str(act_data.get('token_1'))
            act_token_2 = _safe_str(act_data.get('token_2'))
            act_amount_1 = act_data.get('amount_1')
            act_amount_2 = act_data.get('amount_2')
            act_decimal_1 = act_data.get('token_decimal_1')
            act_decimal_2 = act_data.get('token_decimal_2')
            act_fee_amount = act_data.get('fee_ammount')
            act_fee_token = _safe_str(act_data.get('fee_token'))
            act_ta_1_1 = _safe_str(act_data.get('token_account_1_1'))
            act_ta_1_2 = _safe_str(act_data.get('token_account_1_2'))
            act_ta_2_1 = _safe_str(act_data.get('token_account_2_1'))
            act_ta_2_2 = _safe_str(act_data.get('token_account_2_2'))

            # Always collect core addresses
            _collect_addr(all_addresses, act_program, 'program')
            _collect_addr(all_addresses, act_outer_program, 'program')
            _collect_addr(all_addresses, act_account, 'wallet')
            _collect_addr(all_addresses, act_source, 'wallet')
            _collect_addr(all_addresses, act_owner_1, 'wallet')
            _collect_addr(all_addresses, act_token_1, 'mint')
            _collect_addr(all_addresses, act_token_2, 'mint')
            _collect_addr(all_addresses, act_fee_token, 'mint')

            # Extended addresses
            if collect_extended:
                _collect_addr(all_addresses, act_new_account, 'ata')
                _collect_addr(all_addresses, act_init_account, 'ata')
                _collect_addr(all_addresses, act_amm_id, 'pool')
                _collect_addr(all_addresses, act_owner_2, 'wallet')
                _collect_addr(all_addresses, act_ta_1_1, 'vault')
                _collect_addr(all_addresses, act_ta_1_2, 'vault')
                _collect_addr(all_addresses, act_ta_2_1, 'vault')
                _collect_addr(all_addresses, act_ta_2_2, 'vault')

            # Build activity row (used for both activity_rows and swap_rows)
            row = {
                'tx_hash': tx_hash,
                'ins_index': ins_index,
                'outer_ins_index': outer_ins_index,
                'activity_type': act_type,
                'name': act_name,
                'program_id': act_program,
                'outer_program_id': act_outer_program,
                'account': act_account,
                'source': act_source,
                'new_account': act_new_account,
                'init_account': act_init_account,
                'amm_id': act_amm_id,
                'owner_1': act_owner_1,
                'owner_2': act_owner_2,
                'token_1': act_token_1,
                'token_2': act_token_2,
                'amount_1': act_amount_1,
                'amount_2': act_amount_2,
                'decimal_1': act_decimal_1,
                'decimal_2': act_decimal_2,
                'fee_amount': act_fee_amount,
                'fee_token': act_fee_token,
                'token_account_1_1': act_ta_1_1,
                'token_account_1_2': act_ta_1_2,
                'token_account_2_1': act_ta_2_1,
                'token_account_2_2': act_ta_2_2,
            }

            # Swap rows are a subset of activities that are swap-typed
            if act_type and 'SWAP' in act_type:
                swap_rows.append(row)

            activity_rows.append(row)

    return {
        'addresses': all_addresses,
        'tx_rows': tx_rows,
        'transfer_rows': transfer_rows,
        'swap_rows': swap_rows,
        'activity_rows': activity_rows,
    }


# ---------------------------------------------------------------------------
# Address resolution
# ---------------------------------------------------------------------------

def resolve_addresses(cursor, conn, addresses, cache):
    """Resolve string addresses to integer IDs, inserting new rows as needed.

    Args:
        cursor: MySQL cursor.
        conn: MySQL connection.
        addresses: dict {address_str: type_str} from extract_all().
        cache: AddressCache instance.

    After this call, every key in *addresses* will have an entry in
    cache.addr.  Mint addresses will also be in cache.token, programs in
    cache.program, pools in cache.pool.
    """
    if not addresses:
        return

    # 1. Find addresses missing from cache
    missing = [a for a in addresses if a not in cache.addr]
    if not missing:
        # All addresses cached, but we still need to ensure token/program/pool
        _ensure_secondary_lookups(cursor, conn, addresses, cache)
        return

    # 2. Batch SELECT existing tx_address rows
    for chunk in _chunks(missing, _CHUNK_SIZE):
        ph = ','.join(['%s'] * len(chunk))
        cursor.execute(
            f"SELECT id, address FROM tx_address WHERE address IN ({ph})",
            chunk)
        for row in cursor.fetchall():
            cache.addr[row[1]] = row[0]

    # 3. Determine truly new addresses (not in DB at all)
    still_missing = [a for a in missing if a not in cache.addr]

    # 4. INSERT IGNORE new addresses
    if still_missing:
        insert_rows = [(a, addresses[a]) for a in still_missing]
        cursor.executemany(
            "INSERT IGNORE INTO tx_address (address, address_type) VALUES (%s, %s)",
            insert_rows)
        conn.commit()

        # 5. Re-SELECT to get auto-generated IDs
        for chunk in _chunks(still_missing, _CHUNK_SIZE):
            ph = ','.join(['%s'] * len(chunk))
            cursor.execute(
                f"SELECT id, address FROM tx_address WHERE address IN ({ph})",
                chunk)
            for row in cursor.fetchall():
                cache.addr[row[1]] = row[0]

    # 6. Insert into secondary lookup tables and populate cache
    _ensure_secondary_lookups(cursor, conn, addresses, cache)


def _ensure_secondary_lookups(cursor, conn, addresses, cache):
    """INSERT IGNORE into tx_token/tx_program/tx_pool and cache their IDs."""

    # Gather address IDs grouped by type
    mint_addr_ids = []
    program_addr_ids = []
    pool_addr_ids = []

    for addr_str, addr_type in addresses.items():
        addr_id = cache.addr.get(addr_str)
        if addr_id is None:
            continue
        if addr_type == 'mint' and addr_id not in cache.token:
            mint_addr_ids.append(addr_id)
        elif addr_type == 'program' and addr_id not in cache.program:
            program_addr_ids.append(addr_id)
        elif addr_type == 'pool' and addr_id not in cache.pool:
            pool_addr_ids.append(addr_id)

    did_insert = False

    # tx_token
    if mint_addr_ids:
        cursor.executemany(
            "INSERT IGNORE INTO tx_token (mint_address_id) VALUES (%s)",
            [(aid,) for aid in mint_addr_ids])
        did_insert = True

    # tx_program
    if program_addr_ids:
        cursor.executemany(
            "INSERT IGNORE INTO tx_program (program_address_id) VALUES (%s)",
            [(aid,) for aid in program_addr_ids])
        did_insert = True

    # tx_pool
    if pool_addr_ids:
        cursor.executemany(
            "INSERT IGNORE INTO tx_pool (pool_address_id) VALUES (%s)",
            [(aid,) for aid in pool_addr_ids])
        did_insert = True

    if did_insert:
        conn.commit()

    # Batch-select IDs into cache
    _cache_tokens(cursor, mint_addr_ids, cache)
    _cache_programs(cursor, program_addr_ids, cache)
    _cache_pools(cursor, pool_addr_ids, cache)


def _cache_tokens(cursor, addr_ids, cache):
    """SELECT tx_token IDs for mint_address_ids not yet cached."""
    uncached = [aid for aid in addr_ids if aid not in cache.token]
    if not uncached:
        return
    for chunk in _chunks(uncached, _CHUNK_SIZE):
        ph = ','.join(['%s'] * len(chunk))
        cursor.execute(
            f"SELECT id, mint_address_id FROM tx_token WHERE mint_address_id IN ({ph})",
            chunk)
        for row in cursor.fetchall():
            cache.token[row[1]] = row[0]


def _cache_programs(cursor, addr_ids, cache):
    """SELECT tx_program IDs for program_address_ids not yet cached."""
    uncached = [aid for aid in addr_ids if aid not in cache.program]
    if not uncached:
        return
    for chunk in _chunks(uncached, _CHUNK_SIZE):
        ph = ','.join(['%s'] * len(chunk))
        cursor.execute(
            f"SELECT id, program_address_id FROM tx_program WHERE program_address_id IN ({ph})",
            chunk)
        for row in cursor.fetchall():
            cache.program[row[1]] = row[0]


def _cache_pools(cursor, addr_ids, cache):
    """SELECT tx_pool IDs for pool_address_ids not yet cached."""
    uncached = [aid for aid in addr_ids if aid not in cache.pool]
    if not uncached:
        return
    for chunk in _chunks(uncached, _CHUNK_SIZE):
        ph = ','.join(['%s'] * len(chunk))
        cursor.execute(
            f"SELECT id, pool_address_id FROM tx_pool WHERE pool_address_id IN ({ph})",
            chunk)
        for row in cursor.fetchall():
            cache.pool[row[1]] = row[0]


# ---------------------------------------------------------------------------
# ID resolution on extracted rows
# ---------------------------------------------------------------------------

def _resolve_addr(addr_str, cache):
    """Resolve an address string to tx_address.id via cache."""
    if addr_str is None:
        return None
    return cache.addr.get(addr_str)


def _resolve_token(addr_str, cache):
    """Resolve a mint address string to tx_token.id via cache (two-hop)."""
    if addr_str is None:
        return None
    addr_id = cache.addr.get(addr_str)
    if addr_id is None:
        return None
    return cache.token.get(addr_id)


def _resolve_program(addr_str, cache):
    """Resolve a program address string to tx_program.id via cache (two-hop)."""
    if addr_str is None:
        return None
    addr_id = cache.addr.get(addr_str)
    if addr_id is None:
        return None
    return cache.program.get(addr_id)


def _resolve_pool(addr_str, cache):
    """Resolve a pool address string to tx_pool.id via cache (two-hop)."""
    if addr_str is None:
        return None
    addr_id = cache.addr.get(addr_str)
    if addr_id is None:
        return None
    return cache.pool.get(addr_id)


def resolve_ids(extracted, cache):
    """Walk all extracted rows and replace string addresses with integer IDs.

    Modifies rows in-place.  Unresolvable addresses become None.
    """
    # tx_rows
    for row in extracted['tx_rows']:
        row['signer_id'] = _resolve_addr(row.pop('signer'), cache)
        row['agg_program_id'] = _resolve_program(row.pop('agg_program'), cache)
        row['token_1_id'] = _resolve_token(row.pop('token_1'), cache)
        row['token_2_id'] = _resolve_token(row.pop('token_2'), cache)
        row['fee_token_id'] = _resolve_token(row.pop('fee_token'), cache)

    # transfer_rows
    for row in extracted['transfer_rows']:
        row['source_id'] = _resolve_addr(row.pop('source'), cache)
        row['source_owner_id'] = _resolve_addr(row.pop('source_owner'), cache)
        row['destination_id'] = _resolve_addr(row.pop('destination'), cache)
        row['destination_owner_id'] = _resolve_addr(row.pop('destination_owner'), cache)
        row['token_id'] = _resolve_token(row.pop('token_address'), cache)
        row['base_token_id'] = _resolve_token(row.pop('base_token_address'), cache)
        row['program_id_resolved'] = _resolve_program(row.pop('program_id'), cache)
        row['outer_program_id_resolved'] = _resolve_program(row.pop('outer_program_id'), cache)

    # activity_rows and swap_rows share the same row dicts for swaps,
    # so resolve activity_rows (superset) and swap_rows get resolved too.
    _seen_activity_ids = set()
    for row in extracted['activity_rows']:
        row_id = id(row)
        if row_id in _seen_activity_ids:
            continue
        _seen_activity_ids.add(row_id)

        row['program_id_resolved'] = _resolve_program(row.pop('program_id'), cache)
        row['outer_program_id_resolved'] = _resolve_program(row.pop('outer_program_id'), cache)
        row['account_id'] = _resolve_addr(row.pop('account'), cache)
        row['source_id'] = _resolve_addr(row.pop('source'), cache)
        row['new_account_id'] = _resolve_addr(row.pop('new_account'), cache)
        row['init_account_id'] = _resolve_addr(row.pop('init_account'), cache)
        row['amm_id_resolved'] = _resolve_pool(row.pop('amm_id'), cache)
        row['owner_1_id'] = _resolve_addr(row.pop('owner_1'), cache)
        row['owner_2_id'] = _resolve_addr(row.pop('owner_2'), cache)
        row['token_1_id'] = _resolve_token(row.pop('token_1'), cache)
        row['token_2_id'] = _resolve_token(row.pop('token_2'), cache)
        row['fee_token_id'] = _resolve_token(row.pop('fee_token'), cache)
        row['ta_1_1_id'] = _resolve_addr(row.pop('token_account_1_1'), cache)
        row['ta_1_2_id'] = _resolve_addr(row.pop('token_account_1_2'), cache)
        row['ta_2_1_id'] = _resolve_addr(row.pop('token_account_2_1'), cache)
        row['ta_2_2_id'] = _resolve_addr(row.pop('token_account_2_2'), cache)


# ---------------------------------------------------------------------------
# Temp table population
# ---------------------------------------------------------------------------

_DDL_TMP_TXS = """
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_decode_txs (
    idx             INT,
    tx_hash         VARCHAR(88) NOT NULL,
    block_id        BIGINT UNSIGNED,
    block_time      BIGINT UNSIGNED,
    fee             BIGINT UNSIGNED,
    priority_fee    BIGINT UNSIGNED,
    activity_type   VARCHAR(60),
    signer_id       INT UNSIGNED,
    agg_program_id  BIGINT UNSIGNED,
    token_1_id      BIGINT,
    token_2_id      BIGINT,
    amount_1        DECIMAL(38,0),
    amount_2        DECIMAL(38,0),
    decimal_1       TINYINT UNSIGNED,
    decimal_2       TINYINT UNSIGNED,
    fee_amount      DECIMAL(38,0),
    fee_token_id    BIGINT,
    PRIMARY KEY (tx_hash)
) ENGINE=MEMORY
"""

_DDL_TMP_TRANSFERS = """
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_decode_transfers (
    tx_hash             VARCHAR(88) NOT NULL,
    ins_index           INT,
    outer_ins_index     INT,
    transfer_type       VARCHAR(30),
    source_id           INT UNSIGNED,
    source_owner_id     INT UNSIGNED,
    destination_id      INT UNSIGNED,
    destination_owner_id INT UNSIGNED,
    token_id            BIGINT,
    decimals            TINYINT UNSIGNED,
    amount              DECIMAL(38,0),
    program_id          BIGINT UNSIGNED,
    outer_program_id    BIGINT UNSIGNED,
    base_token_id       BIGINT,
    base_decimals       TINYINT UNSIGNED,
    base_amount         DECIMAL(38,0)
) ENGINE=MEMORY
"""

_DDL_TMP_SWAPS = """
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_decode_swaps (
    tx_hash             VARCHAR(88) NOT NULL,
    ins_index           INT,
    outer_ins_index     INT,
    activity_type       VARCHAR(60),
    name                VARCHAR(60),
    program_id          BIGINT UNSIGNED,
    outer_program_id    BIGINT UNSIGNED,
    account_id          INT UNSIGNED,
    source_id           INT UNSIGNED,
    new_account_id      INT UNSIGNED,
    init_account_id     INT UNSIGNED,
    amm_id              BIGINT UNSIGNED,
    owner_1_id          INT UNSIGNED,
    owner_2_id          INT UNSIGNED,
    token_1_id          BIGINT,
    token_2_id          BIGINT,
    amount_1            DECIMAL(38,0),
    amount_2            DECIMAL(38,0),
    decimal_1           TINYINT UNSIGNED,
    decimal_2           TINYINT UNSIGNED,
    fee_amount          DECIMAL(38,0),
    fee_token_id        BIGINT,
    ta_1_1_id           INT UNSIGNED,
    ta_1_2_id           INT UNSIGNED,
    ta_2_1_id           INT UNSIGNED,
    ta_2_2_id           INT UNSIGNED
) ENGINE=MEMORY
"""

_DDL_TMP_ACTIVITIES = """
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_decode_activities (
    tx_hash             VARCHAR(88) NOT NULL,
    ins_index           INT,
    outer_ins_index     INT,
    activity_type       VARCHAR(60),
    name                VARCHAR(60),
    program_id          BIGINT UNSIGNED,
    outer_program_id    BIGINT UNSIGNED,
    account_id          INT UNSIGNED,
    source_id           INT UNSIGNED,
    new_account_id      INT UNSIGNED,
    init_account_id     INT UNSIGNED,
    amm_id              BIGINT UNSIGNED,
    owner_1_id          INT UNSIGNED,
    owner_2_id          INT UNSIGNED,
    token_1_id          BIGINT,
    token_2_id          BIGINT,
    amount_1            DECIMAL(38,0),
    amount_2            DECIMAL(38,0),
    decimal_1           TINYINT UNSIGNED,
    decimal_2           TINYINT UNSIGNED,
    fee_amount          DECIMAL(38,0),
    fee_token_id        BIGINT,
    ta_1_1_id           INT UNSIGNED,
    ta_1_2_id           INT UNSIGNED,
    ta_2_1_id           INT UNSIGNED,
    ta_2_2_id           INT UNSIGNED
) ENGINE=MEMORY
"""


def populate_temp_tables(cursor, extracted):
    """Create temp tables and INSERT extracted rows with pre-resolved IDs.

    Args:
        cursor: MySQL cursor.
        extracted: The dict returned by extract_all() after resolve_ids().
    """
    # Create (or truncate if they exist from a prior call on same connection)
    for ddl in (_DDL_TMP_TXS, _DDL_TMP_TRANSFERS, _DDL_TMP_SWAPS, _DDL_TMP_ACTIVITIES):
        cursor.execute(ddl)

    # Truncate in case temp tables survived from a previous call
    cursor.execute("TRUNCATE TABLE tmp_decode_txs")
    cursor.execute("TRUNCATE TABLE tmp_decode_transfers")
    cursor.execute("TRUNCATE TABLE tmp_decode_swaps")
    cursor.execute("TRUNCATE TABLE tmp_decode_activities")

    # --- tmp_decode_txs ---
    tx_rows = extracted['tx_rows']
    if tx_rows:
        cursor.executemany(
            "INSERT INTO tmp_decode_txs "
            "(idx, tx_hash, block_id, block_time, fee, priority_fee, activity_type, "
            " signer_id, agg_program_id, token_1_id, token_2_id, "
            " amount_1, amount_2, decimal_1, decimal_2, fee_amount, fee_token_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            [
                (i + 1, r['tx_hash'], r['block_id'], r['block_time'], r['fee'],
                 r['priority_fee'], r['activity_type'], r['signer_id'],
                 r['agg_program_id'], r['token_1_id'], r['token_2_id'],
                 r['amount_1'], r['amount_2'], r['decimal_1'], r['decimal_2'],
                 r['fee_amount'], r['fee_token_id'])
                for i, r in enumerate(tx_rows)
            ])

    # --- tmp_decode_transfers ---
    xfer_rows = extracted['transfer_rows']
    if xfer_rows:
        cursor.executemany(
            "INSERT INTO tmp_decode_transfers "
            "(tx_hash, ins_index, outer_ins_index, transfer_type, "
            " source_id, source_owner_id, destination_id, destination_owner_id, "
            " token_id, decimals, amount, program_id, outer_program_id, "
            " base_token_id, base_decimals, base_amount) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            [
                (r['tx_hash'], r['ins_index'], r['outer_ins_index'],
                 r['transfer_type'], r['source_id'], r['source_owner_id'],
                 r['destination_id'], r['destination_owner_id'],
                 r['token_id'], r['decimals'], r['amount'],
                 r['program_id_resolved'], r['outer_program_id_resolved'],
                 r['base_token_id'], r['base_decimals'], r['base_amount'])
                for r in xfer_rows
            ])

    # --- tmp_decode_swaps ---
    swap_rows = extracted['swap_rows']
    if swap_rows:
        cursor.executemany(
            "INSERT INTO tmp_decode_swaps "
            "(tx_hash, ins_index, outer_ins_index, activity_type, name, "
            " program_id, outer_program_id, account_id, source_id, "
            " new_account_id, init_account_id, amm_id, "
            " owner_1_id, owner_2_id, token_1_id, token_2_id, "
            " amount_1, amount_2, decimal_1, decimal_2, "
            " fee_amount, fee_token_id, "
            " ta_1_1_id, ta_1_2_id, ta_2_1_id, ta_2_2_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            [_activity_row_tuple(r) for r in swap_rows])

    # --- tmp_decode_activities ---
    act_rows = extracted['activity_rows']
    if act_rows:
        cursor.executemany(
            "INSERT INTO tmp_decode_activities "
            "(tx_hash, ins_index, outer_ins_index, activity_type, name, "
            " program_id, outer_program_id, account_id, source_id, "
            " new_account_id, init_account_id, amm_id, "
            " owner_1_id, owner_2_id, token_1_id, token_2_id, "
            " amount_1, amount_2, decimal_1, decimal_2, "
            " fee_amount, fee_token_id, "
            " ta_1_1_id, ta_1_2_id, ta_2_1_id, ta_2_2_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            [_activity_row_tuple(r) for r in act_rows])


def _activity_row_tuple(r):
    """Convert an activity/swap row dict to a parameter tuple for INSERT."""
    return (
        r['tx_hash'], r['ins_index'], r['outer_ins_index'],
        r['activity_type'], r['name'],
        r['program_id_resolved'], r['outer_program_id_resolved'],
        r['account_id'], r['source_id'],
        r['new_account_id'], r['init_account_id'],
        r['amm_id_resolved'],
        r['owner_1_id'], r['owner_2_id'],
        r['token_1_id'], r['token_2_id'],
        r['amount_1'], r['amount_2'],
        r['decimal_1'], r['decimal_2'],
        r['fee_amount'], r['fee_token_id'],
        r['ta_1_1_id'], r['ta_1_2_id'],
        r['ta_2_1_id'], r['ta_2_2_id'],
    )


# ---------------------------------------------------------------------------
# SP call
# ---------------------------------------------------------------------------

def call_sp(cursor, conn, request_log_id, tx_origin):
    """Call sp_tx_parse_decode_v2 and return result counts.

    Assumes temp tables have been populated by populate_temp_tables().

    Returns:
        dict with keys: tx_count, transfer_count, swap_count,
        activity_count, skipped_count.
    """
    cursor.execute("SET @tx=0, @xfer=0, @swap=0, @act=0, @skip=0")
    cursor.execute(
        "CALL sp_tx_parse_decode_v2(%s, %s, @tx, @xfer, @swap, @act, @skip)",
        (request_log_id, tx_origin))
    # Drain any result sets from the SP
    try:
        while cursor.nextset():
            pass
    except Exception:
        pass
    cursor.execute("SELECT @tx, @xfer, @swap, @act, @skip")
    row = cursor.fetchone()
    conn.commit()

    return {
        'tx_count':       row[0] or 0 if row else 0,
        'transfer_count': row[1] or 0 if row else 0,
        'swap_count':     row[2] or 0 if row else 0,
        'activity_count': row[3] or 0 if row else 0,
        'skipped_count':  row[4] or 0 if row else 0,
    }


# ---------------------------------------------------------------------------
# Pool reclassification
# ---------------------------------------------------------------------------

def _reclassify_pool_addresses(cursor, conn, extracted, cache):
    """Reclassify owner_2 addresses as pools when they appear as amm_id
    in the same activity.

    Some Solscan activities list the pool account as owner_2 AND as amm_id.
    We initially classify owner_2 as 'wallet'.  If the same address appears
    as amm_id in any activity, reclassify it to 'pool' and ensure a
    tx_pool row exists.
    """
    # Collect amm_id address strings from the original extraction
    # (before resolve_ids was called).  Since resolve_ids pops string keys,
    # we use the resolved amm_id and reverse-lookup.  Instead, gather from
    # the addresses dict which already has them typed.
    pool_addr_strs = {a for a, t in extracted['addresses'].items() if t == 'pool'}
    if not pool_addr_strs:
        return

    # Find any of these that were initially typed as 'wallet' in tx_address
    # and need reclassification.
    addr_ids_to_reclass = []
    for addr_str in pool_addr_strs:
        addr_id = cache.addr.get(addr_str)
        if addr_id is not None:
            addr_ids_to_reclass.append(addr_id)

    if not addr_ids_to_reclass:
        return

    # UPDATE tx_address SET address_type='pool' WHERE id IN (...) AND address_type != 'pool'
    for chunk in _chunks(addr_ids_to_reclass, _CHUNK_SIZE):
        ph = ','.join(['%s'] * len(chunk))
        cursor.execute(
            f"UPDATE IGNORE tx_address SET address_type='pool' "
            f"WHERE id IN ({ph}) AND address_type != 'pool'",
            chunk)

    # INSERT IGNORE into tx_pool for any newly-reclassified
    new_pool_ids = [aid for aid in addr_ids_to_reclass if aid not in cache.pool]
    if new_pool_ids:
        cursor.executemany(
            "INSERT IGNORE INTO tx_pool (pool_address_id) VALUES (%s)",
            [(aid,) for aid in new_pool_ids])
        conn.commit()

        # Cache the new pool IDs
        _cache_pools(cursor, new_pool_ids, cache)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def process_decoded_v2(cursor, conn, decoded_data, request_log_id, tx_origin,
                       features=31, cache=None, tag='V2'):
    """Extract, resolve, and insert decoded transaction data.

    This is the main entry point called from guide-decoder.py.

    Args:
        cursor: MySQL cursor.
        conn: MySQL connection.
        decoded_data: Solscan API response dict (already parsed JSON).
        request_log_id: FK to tx_request_log.
        tx_origin: Origin code (1=user, 2=sync, 3=prime).
        features: Feature bitmask (default 31 = all).
        cache: AddressCache instance (thread-local, reused across calls).
            If None, a temporary one is created (no cross-call caching).
        tag: Log tag prefix.

    Returns:
        dict with keys: tx_count, transfer_count, swap_count,
        activity_count, skipped_count, error (None on success).
    """
    if cache is None:
        cache = AddressCache()

    result = {
        'tx_count': 0, 'transfer_count': 0, 'swap_count': 0,
        'activity_count': 0, 'skipped_count': 0, 'error': None,
    }

    try:
        # Phase 1: Extract all data in a single pass
        extracted = extract_all(decoded_data, features)

        addr_count = len(extracted['addresses'])
        tx_count = len(extracted['tx_rows'])
        xfer_count = len(extracted['transfer_rows'])
        swap_count = len(extracted['swap_rows'])
        act_count = len(extracted['activity_rows'])

        if tx_count == 0:
            _log(tag, "No transactions extracted from response")
            return result

        _log(tag, f"Extracted: {tx_count} txs, {xfer_count} xfers, "
             f"{swap_count} swaps, {act_count} acts, {addr_count} addrs")

        # Phase 2: Resolve all string addresses to integer IDs
        resolve_addresses(cursor, conn, extracted['addresses'], cache)

        # Phase 2b: Reclassify pool addresses that were initially typed as wallet
        _reclassify_pool_addresses(cursor, conn, extracted, cache)

        # Phase 3: Replace string addresses with integer IDs in all rows
        resolve_ids(extracted, cache)

        # Phase 4: Populate temp tables
        populate_temp_tables(cursor, extracted)

        # Phase 5: Call the v2 stored procedure
        sp_result = call_sp(cursor, conn, request_log_id, tx_origin)

        result.update(sp_result)

        _log(tag, f"SP result: tx={sp_result['tx_count']} xfer={sp_result['transfer_count']} "
             f"swap={sp_result['swap_count']} act={sp_result['activity_count']} "
             f"skip={sp_result['skipped_count']}")

    except Exception as e:
        result['error'] = f"decode_v2 error: {e}"
        _log(tag, f"ERROR: {e}")
        # Re-raise DB errors so the caller can reconnect
        import mysql.connector
        if isinstance(e, mysql.connector.Error):
            raise

    return result
