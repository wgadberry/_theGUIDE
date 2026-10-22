"""
detail_v2 - Pre-resolved detail path for guide-detailer.py

Parses Solscan detail JSON once in Python, resolves all string addresses to
integer IDs via a thread-local cache, INSERTs new lookup rows directly,
populates temp tables, and calls sp_tx_parse_detail_v2 which does pure INSERTs.

Usage from guide-detailer.py:
    from detail_v2 import process_detail_v2
    from decode_v2 import AddressCache
"""

from decode_v2 import AddressCache, _safe_str, _chunks, _log, \
    resolve_addresses, _collect_addr

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CHUNK_SIZE = 500  # max rows per executemany batch


# ---------------------------------------------------------------------------
# Extraction (single pass)
# ---------------------------------------------------------------------------

def extract_detail(data):
    """Single-pass extraction of all data from Solscan detail response.

    Args:
        data: The parsed Solscan response dict (must have 'data' key).

    Returns:
        dict with keys: addresses, tx_rows, sol_rows, token_rows.
    """
    all_addresses = {}  # address_str -> type_str
    tx_rows = []
    sol_rows = []
    token_rows = []

    txs = data.get('data') or []

    for tx in txs:
        tx_hash = _safe_str(tx.get('tx_hash'))
        if tx_hash is None:
            continue

        block_id = tx.get('block_id')
        block_time = tx.get('block_time')

        # --- signer (fee_payer) ---
        signers = tx.get('signer') or []
        fee_payer = _safe_str(signers[0]) if signers else None
        if fee_payer:
            _collect_addr(all_addresses, fee_payer, 'wallet')

        tx_rows.append({
            'tx_hash': tx_hash,
            'block_id': block_id,
            'block_time': block_time,
            'fee_payer': fee_payer,
        })

        # --- sol_bal_change ---
        for sbc in (tx.get('sol_bal_change') or []):
            address = _safe_str(sbc.get('address'))
            if address is None:
                continue
            _collect_addr(all_addresses, address, 'wallet')

            sol_rows.append({
                'tx_hash': tx_hash,
                'block_time': block_time,
                'address': address,
                'pre_balance': _safe_str(sbc.get('pre_balance')),
                'post_balance': _safe_str(sbc.get('post_balance')),
                'change_amount': _safe_str(sbc.get('change_amount')),
            })

        # --- token_bal_change ---
        for tbc in (tx.get('token_bal_change') or []):
            address = _safe_str(tbc.get('address'))
            owner = _safe_str(tbc.get('owner'))
            token_address = _safe_str(tbc.get('token_address'))

            if address is None:
                continue

            _collect_addr(all_addresses, address, 'ata')
            _collect_addr(all_addresses, owner, 'wallet')
            _collect_addr(all_addresses, token_address, 'mint')

            token_rows.append({
                'tx_hash': tx_hash,
                'block_time': block_time,
                'address': address,
                'owner': owner,
                'token_address': token_address,
                'decimals': tbc.get('decimals'),
                'pre_balance': _safe_str(tbc.get('pre_balance')),
                'post_balance': _safe_str(tbc.get('post_balance')),
                'change_amount': _safe_str(tbc.get('change_amount')),
                'change_type': _safe_str(tbc.get('change_type')),
            })

    return {
        'addresses': all_addresses,
        'tx_rows': tx_rows,
        'sol_rows': sol_rows,
        'token_rows': token_rows,
    }


# ---------------------------------------------------------------------------
# ID resolution helpers (two-hop for tokens)
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


# ---------------------------------------------------------------------------
# ID resolution on extracted rows
# ---------------------------------------------------------------------------

def resolve_ids(extracted, cache):
    """Walk all extracted rows and replace string addresses with integer IDs.

    Modifies rows in-place. Unresolvable addresses become None.
    """
    # tx_rows: fee_payer -> fee_payer_id
    for row in extracted['tx_rows']:
        row['fee_payer_id'] = _resolve_addr(row.pop('fee_payer'), cache)

    # sol_rows: address -> address_id
    for row in extracted['sol_rows']:
        row['address_id'] = _resolve_addr(row.pop('address'), cache)

    # token_rows: address -> address_id, owner -> owner_id, token_address -> token_id
    for row in extracted['token_rows']:
        row['address_id'] = _resolve_addr(row.pop('address'), cache)
        row['owner_id'] = _resolve_addr(row.pop('owner'), cache)
        row['token_id'] = _resolve_token(row.pop('token_address'), cache)


# ---------------------------------------------------------------------------
# Temp table DDL
# ---------------------------------------------------------------------------

_DDL_TMP_TXS = """
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_detail_txs (
    tx_hash      VARCHAR(88) NOT NULL PRIMARY KEY,
    block_id     BIGINT UNSIGNED,
    block_time   BIGINT UNSIGNED,
    fee_payer_id INT UNSIGNED
) ENGINE=MEMORY
"""

_DDL_TMP_SOL = """
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_detail_sol (
    tx_hash        VARCHAR(88) NOT NULL,
    block_time     BIGINT UNSIGNED,
    address_id     INT UNSIGNED,
    pre_balance    BIGINT UNSIGNED,
    post_balance   BIGINT UNSIGNED,
    change_amount  BIGINT SIGNED
) ENGINE=MEMORY
"""

_DDL_TMP_TOKEN = """
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_detail_token (
    tx_hash        VARCHAR(88) NOT NULL,
    block_time     BIGINT UNSIGNED,
    address_id     INT UNSIGNED,
    owner_id       INT UNSIGNED,
    token_id       BIGINT,
    decimals       TINYINT UNSIGNED,
    pre_balance    DECIMAL(38,0),
    post_balance   DECIMAL(38,0),
    change_amount  DECIMAL(38,0),
    change_type    VARCHAR(10)
) ENGINE=MEMORY
"""


# ---------------------------------------------------------------------------
# Temp table population
# ---------------------------------------------------------------------------

def populate_temp_tables(cursor, extracted):
    """Create temp tables and INSERT extracted rows with pre-resolved IDs.

    Args:
        cursor: MySQL cursor.
        extracted: The dict returned by extract_detail() after resolve_ids().
    """
    # Create (or reuse if they exist from a prior call on same connection)
    for ddl in (_DDL_TMP_TXS, _DDL_TMP_SOL, _DDL_TMP_TOKEN):
        cursor.execute(ddl)

    # Truncate in case temp tables survived from a previous call
    cursor.execute("TRUNCATE TABLE tmp_detail_txs")
    cursor.execute("TRUNCATE TABLE tmp_detail_sol")
    cursor.execute("TRUNCATE TABLE tmp_detail_token")

    # --- tmp_detail_txs ---
    tx_rows = extracted['tx_rows']
    if tx_rows:
        for chunk in _chunks(tx_rows, _CHUNK_SIZE):
            cursor.executemany(
                "INSERT INTO tmp_detail_txs "
                "(tx_hash, block_id, block_time, fee_payer_id) "
                "VALUES (%s, %s, %s, %s)",
                [
                    (r['tx_hash'], r['block_id'], r['block_time'], r['fee_payer_id'])
                    for r in chunk
                ])

    # --- tmp_detail_sol ---
    # Balance values are passed as strings to avoid Python int overflow;
    # MySQL connector sends them as-is and MySQL casts to BIGINT/SIGNED.
    sol_rows = extracted['sol_rows']
    if sol_rows:
        for chunk in _chunks(sol_rows, _CHUNK_SIZE):
            cursor.executemany(
                "INSERT INTO tmp_detail_sol "
                "(tx_hash, block_time, address_id, pre_balance, post_balance, change_amount) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                [
                    (r['tx_hash'], r['block_time'], r['address_id'],
                     r['pre_balance'], r['post_balance'], r['change_amount'])
                    for r in chunk
                ])

    # --- tmp_detail_token ---
    # Token balance values can exceed BIGINT (meme tokens near 10^19).
    # Kept as strings; MySQL casts to DECIMAL(38,0).
    token_rows = extracted['token_rows']
    if token_rows:
        for chunk in _chunks(token_rows, _CHUNK_SIZE):
            cursor.executemany(
                "INSERT INTO tmp_detail_token "
                "(tx_hash, block_time, address_id, owner_id, token_id, "
                " decimals, pre_balance, post_balance, change_amount, change_type) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                [
                    (r['tx_hash'], r['block_time'], r['address_id'], r['owner_id'],
                     r['token_id'], r['decimals'],
                     r['pre_balance'], r['post_balance'], r['change_amount'],
                     r['change_type'])
                    for r in chunk
                ])


# ---------------------------------------------------------------------------
# SP call
# ---------------------------------------------------------------------------

def call_sp(cursor, conn, request_log_id):
    """Call sp_tx_parse_detail_v2 and return result counts.

    Assumes temp tables have been populated by populate_temp_tables().

    Returns:
        dict with keys: tx_count, sol_balance_count, token_balance_count,
        skipped_count.
    """
    cursor.execute("SET @tx=0, @sol=0, @tok=0, @skip=0")
    cursor.execute(
        "CALL sp_tx_parse_detail_v2(%s, @tx, @sol, @tok, @skip)",
        (request_log_id,))
    # Drain any result sets from the SP
    try:
        while cursor.nextset():
            pass
    except Exception:
        pass
    cursor.execute("SELECT @tx, @sol, @tok, @skip")
    row = cursor.fetchone()
    conn.commit()

    return {
        'tx_count':            row[0] or 0 if row else 0,
        'sol_balance_count':   row[1] or 0 if row else 0,
        'token_balance_count': row[2] or 0 if row else 0,
        'skipped_count':       row[3] or 0 if row else 0,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def process_detail_v2(cursor, conn, detail_data, request_log_id,
                      features=31, cache=None, tag='V2'):
    """Extract, resolve, and insert detail transaction data.

    This is the main entry point called from guide-detailer.py.

    Args:
        cursor: MySQL cursor.
        conn: MySQL connection.
        detail_data: Solscan detail API response dict (already parsed JSON).
        request_log_id: FK to tx_request_log.
        features: Feature bitmask (passed through; filtering done in SP).
        cache: AddressCache instance (thread-local, reused across calls).
            If None, a temporary one is created (no cross-call caching).
        tag: Log tag prefix.

    Returns:
        dict with keys: tx_count, sol_balance_count, token_balance_count,
        skipped_count, error (None on success).
    """
    if cache is None:
        cache = AddressCache()

    result = {
        'tx_count': 0, 'sol_balance_count': 0, 'token_balance_count': 0,
        'skipped_count': 0, 'error': None,
    }

    try:
        # Phase 1: Extract all data in a single pass
        extracted = extract_detail(detail_data)

        addr_count = len(extracted['addresses'])
        tx_count = len(extracted['tx_rows'])
        sol_count = len(extracted['sol_rows'])
        tok_count = len(extracted['token_rows'])

        if tx_count == 0:
            _log(tag, "No transactions extracted from detail response")
            return result

        _log(tag, f"Extracted: {tx_count} txs, {sol_count} sol_bal, "
             f"{tok_count} token_bal, {addr_count} addrs")

        # Phase 2: Resolve all string addresses to integer IDs
        resolve_addresses(cursor, conn, extracted['addresses'], cache)

        # Phase 3: Replace string addresses with integer IDs in all rows
        resolve_ids(extracted, cache)

        # Phase 4: Populate temp tables
        populate_temp_tables(cursor, extracted)

        # Phase 5: Call the v2 stored procedure
        sp_result = call_sp(cursor, conn, request_log_id)

        result.update(sp_result)

        _log(tag, f"SP result: tx={sp_result['tx_count']} "
             f"sol={sp_result['sol_balance_count']} "
             f"tok={sp_result['token_balance_count']} "
             f"skip={sp_result['skipped_count']}")

    except Exception as e:
        result['error'] = f"detail_v2 error: {e}"
        _log(tag, f"ERROR: {e}")
        # Re-raise DB errors so the caller can reconnect
        import mysql.connector
        if isinstance(e, mysql.connector.Error):
            raise

    return result
