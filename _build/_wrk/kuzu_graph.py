#!/usr/bin/env python3
"""
kuzu_graph.py — Kùzu embedded graph DB for T16O gateway.

Provides:
  - Schema creation (Address nodes, Transfer edges)
  - Bulk loading from MySQL tx_guide
  - Query helpers for holder maps, traversals, cycle detection

Usage:
  # Standalone load:
  python3 kuzu_graph.py --load

  # From gateway (import):
  from kuzu_graph import get_connection, query_holder_edges
"""
import os
import sys
import time
import json
import argparse

# Ensure t16o_exchange is importable (parent of _wrk)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

try:
    import kuzu
except ImportError:
    kuzu = None
    print("[KUZU] kuzu not installed — graph features disabled", flush=True)

KUZU_DB_PATH = os.environ.get('KUZU_DB_PATH', '/data/kuzu/t16o_graph')

# Module-level singleton
_db = None
_conn = None


def get_db():
    """Get or create the Kùzu database singleton."""
    global _db
    if _db is None:
        if kuzu is None:
            return None
        os.makedirs(os.path.dirname(KUZU_DB_PATH), exist_ok=True)
        _db = kuzu.Database(KUZU_DB_PATH)
        print(f"[KUZU] Database opened at {KUZU_DB_PATH}", flush=True)
    return _db


def get_connection():
    """Get or create a Kùzu connection singleton."""
    global _conn
    if _conn is None:
        db = get_db()
        if db is None:
            return None
        _conn = kuzu.Connection(db)
    return _conn


def create_schema(conn):
    """Create the graph schema (idempotent)."""
    print("[KUZU] Creating schema...", flush=True)

    # Node table
    conn.execute("""
        CREATE NODE TABLE IF NOT EXISTS Address (
            address STRING,
            type STRING,
            label STRING,
            funded_by STRING,
            PRIMARY KEY (address)
        )
    """)

    # Edge table with full tx_guide fields
    conn.execute("""
        CREATE REL TABLE IF NOT EXISTS TRANSFER (
            FROM Address TO Address,
            guide_id INT64,
            signature STRING,
            block_time INT64,
            edge_type STRING,
            edge_category STRING,
            edge_direction STRING,
            risk_weight INT16,
            swap_direction STRING,
            token_symbol STRING,
            token_mint STRING,
            decimals INT16,
            amount STRING,
            from_token_pre_balance STRING,
            from_token_post_balance STRING,
            to_token_pre_balance STRING,
            to_token_post_balance STRING,
            from_sol_pre_balance STRING,
            from_sol_post_balance STRING,
            to_sol_pre_balance STRING,
            to_sol_post_balance STRING,
            fee INT64,
            priority_fee INT64,
            tax_bps INT16,
            tax_amount STRING,
            pool_address STRING,
            pool_label STRING,
            dex STRING,
            token_pair STRING,
            program_name STRING,
            program_type STRING
        )
    """)
    print("[KUZU] Schema ready", flush=True)


def load_from_mysql(conn, batch_size=5000, token_mint=None):
    """Bulk load tx_guide data from MySQL into Kùzu.

    Args:
        token_mint: If set, only load edges for this token (by mint address).
                    Also loads SOL edges between the same addresses.
    """
    import mysql.connector
    from t16o_exchange.guide.common.config import load_config

    cfg = load_config()
    db_cfg = {
        'host': cfg.get('DB_HOST', '127.0.0.1'),
        'port': int(cfg.get('DB_PORT', 3306)),
        'user': cfg.get('DB_USER', 'root'),
        'password': cfg.get('DB_PASSWORD', ''),
        'database': cfg.get('DB_NAME', 't16o_db'),
    }

    t0 = time.time()
    mysql_conn = mysql.connector.connect(**db_cfg)
    cursor = mysql_conn.cursor(dictionary=True)

    # Resolve token_id if filtering
    token_filter = ""
    token_label = "ALL"
    if token_mint:
        cursor.execute(
            "SELECT t.id FROM tx_token t "
            "JOIN tx_address a ON a.id = t.mint_address_id "
            "WHERE a.address = %s", (token_mint,))
        row = cursor.fetchone()
        if not row:
            print(f"[KUZU] ERROR: token mint {token_mint} not found", flush=True)
            return
        token_id = row['id']
        token_filter = f" AND g.token_id = {token_id}"
        token_label = token_mint[:8] + "..."
        print(f"[KUZU] Filtering to token_id={token_id} ({token_label})", flush=True)

    # ── Load addresses ──
    print("[KUZU] Loading addresses...", flush=True)
    addr_where = f"WHERE g.token_id = {token_id}" if token_mint else ""
    cursor.execute(f"""
        SELECT DISTINCT a.address, a.address_type, a.label, fb.address AS funded_by
        FROM (
            SELECT from_address_id AS aid FROM tx_guide g {addr_where}
            UNION
            SELECT to_address_id FROM tx_guide g {addr_where}
        ) ids
        JOIN tx_address a ON a.id = ids.aid
        LEFT JOIN tx_address fb ON fb.id = a.funded_by_address_id
    """)
    addrs = cursor.fetchall()
    print(f"[KUZU]   Fetched {len(addrs)} addresses", flush=True)

    total = 0
    for row in addrs:
        try:
            conn.execute(
                "MERGE (a:Address {address: $addr}) "
                "SET a.type = $type, a.label = $label, a.funded_by = $funded_by",
                {
                    "addr": row['address'],
                    "type": row['address_type'],
                    "label": row.get('label'),
                    "funded_by": row.get('funded_by'),
                }
            )
            total += 1
        except Exception as e:
            if total < 3:
                print(f"[KUZU]   WARN addr: {e}", flush=True)

    print(f"[KUZU]   Loaded {total} address nodes ({time.time()-t0:.0f}s)", flush=True)

    # ── Load edges ──
    print("[KUZU] Loading tx_guide edges...", flush=True)
    cursor.execute(f"SELECT COUNT(*) as cnt FROM tx_guide g WHERE 1=1{token_filter}")
    total_rows = cursor.fetchone()['cnt']
    print(f"[KUZU]   Matching tx_guide rows: {total_rows:,}", flush=True)

    edge_sql = f"""
        SELECT g.id AS guide_id, t.signature, g.block_time,
            fa.address AS from_address, ta.address AS to_address,
            IFNULL(gt.type_code, 'UNKNOWN') AS edge_type,
            IFNULL(gt.category, 'other') AS edge_category,
            IFNULL(gt.direction, 'neutral') AS edge_direction,
            IFNULL(gt.risk_weight, 0) AS risk_weight,
            g.swap_direction,
            IFNULL(tok.token_symbol, 'SOL') AS token_symbol,
            IFNULL(tm.address, 'SOL') AS token_mint,
            IFNULL(g.decimals, IFNULL(tok.decimals, 9)) AS decimals,
            g.amount,
            g.from_token_pre_balance, g.from_token_post_balance,
            g.to_token_pre_balance, g.to_token_post_balance,
            g.from_sol_pre_balance, g.from_sol_post_balance,
            g.to_sol_pre_balance, g.to_sol_post_balance,
            IFNULL(g.fee, 0) AS fee, IFNULL(g.priority_fee, 0) AS priority_fee,
            g.tax_bps, g.tax_amount,
            pa.address AS pool_address, p.pool_label, p.dex, p.token_pair,
            prg.name AS program_name, prg.program_type
        FROM tx_guide g
        JOIN tx t ON t.id = g.tx_id
        JOIN tx_address fa ON fa.id = g.from_address_id
        JOIN tx_address ta ON ta.id = g.to_address_id
        LEFT JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        LEFT JOIN tx_token tok ON tok.id = g.token_id AND g.token_id > 0
        LEFT JOIN tx_address tm ON tm.id = tok.mint_address_id
        LEFT JOIN tx_pool p ON p.id = g.pool_id
        LEFT JOIN tx_address pa ON pa.id = p.pool_address_id
        LEFT JOIN tx_program prg ON prg.id = COALESCE(p.program_id, fa.program_id)
        WHERE g.id > %s{token_filter}
        ORDER BY g.id
        LIMIT %s
    """

    insert_stmt = (
        "MATCH (fr:Address {address: $from_addr}), (to:Address {address: $to_addr}) "
        "CREATE (fr)-[t:TRANSFER {"
        "guide_id: $guide_id, signature: $signature, block_time: $block_time, "
        "edge_type: $edge_type, edge_category: $edge_category, "
        "edge_direction: $edge_direction, risk_weight: $risk_weight, "
        "swap_direction: $swap_direction, token_symbol: $token_symbol, "
        "token_mint: $token_mint, decimals: $decimals, amount: $amount, "
        "from_token_pre_balance: $from_token_pre_balance, "
        "from_token_post_balance: $from_token_post_balance, "
        "to_token_pre_balance: $to_token_pre_balance, "
        "to_token_post_balance: $to_token_post_balance, "
        "from_sol_pre_balance: $from_sol_pre_balance, "
        "from_sol_post_balance: $from_sol_post_balance, "
        "to_sol_pre_balance: $to_sol_pre_balance, "
        "to_sol_post_balance: $to_sol_post_balance, "
        "fee: $fee, priority_fee: $priority_fee, "
        "tax_bps: $tax_bps, tax_amount: $tax_amount, "
        "pool_address: $pool_address, pool_label: $pool_label, "
        "dex: $dex, token_pair: $token_pair, "
        "program_name: $program_name, program_type: $program_type"
        "}]->(to)"
    )

    offset = 0
    total_loaded = 0
    errors = 0

    while True:
        cursor.execute(edge_sql, (offset, batch_size))
        rows = cursor.fetchall()
        if not rows:
            break

        for row in rows:
            try:
                params = {
                    "from_addr": row['from_address'],
                    "to_addr": row['to_address'],
                    "guide_id": int(row['guide_id']),
                    "signature": row['signature'],
                    "block_time": int(row['block_time']) if row['block_time'] else 0,
                    "edge_type": row['edge_type'],
                    "edge_category": row['edge_category'],
                    "edge_direction": row['edge_direction'],
                    "risk_weight": int(row['risk_weight']),
                    "swap_direction": row.get('swap_direction'),
                    "token_symbol": row['token_symbol'],
                    "token_mint": row['token_mint'],
                    "decimals": int(row['decimals']),
                    "amount": str(row['amount']) if row['amount'] is not None else None,
                    "from_token_pre_balance": str(row['from_token_pre_balance']) if row['from_token_pre_balance'] is not None else None,
                    "from_token_post_balance": str(row['from_token_post_balance']) if row['from_token_post_balance'] is not None else None,
                    "to_token_pre_balance": str(row['to_token_pre_balance']) if row['to_token_pre_balance'] is not None else None,
                    "to_token_post_balance": str(row['to_token_post_balance']) if row['to_token_post_balance'] is not None else None,
                    "from_sol_pre_balance": str(row['from_sol_pre_balance']) if row['from_sol_pre_balance'] is not None else None,
                    "from_sol_post_balance": str(row['from_sol_post_balance']) if row['from_sol_post_balance'] is not None else None,
                    "to_sol_pre_balance": str(row['to_sol_pre_balance']) if row['to_sol_pre_balance'] is not None else None,
                    "to_sol_post_balance": str(row['to_sol_post_balance']) if row['to_sol_post_balance'] is not None else None,
                    "fee": int(row['fee']),
                    "priority_fee": int(row['priority_fee']),
                    "tax_bps": int(row['tax_bps']) if row.get('tax_bps') is not None else None,
                    "tax_amount": str(row['tax_amount']) if row.get('tax_amount') is not None else None,
                    "pool_address": row.get('pool_address'),
                    "pool_label": row.get('pool_label'),
                    "dex": row.get('dex'),
                    "token_pair": row.get('token_pair'),
                    "program_name": row.get('program_name'),
                    "program_type": row.get('program_type'),
                }
                conn.execute(insert_stmt, params)
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"[KUZU]   WARN edge {row['guide_id']}: {e}", flush=True)

        total_loaded += len(rows)
        offset = int(rows[-1]['guide_id'])
        elapsed = time.time() - t0
        rate = total_loaded / max(1, elapsed)
        pct = total_loaded / max(1, total_rows) * 100
        print(f"[KUZU]   {total_loaded:>8,} / {total_rows:,} "
              f"({pct:.1f}%) — {rate:.0f}/s — {elapsed:.0f}s", flush=True)

        if len(rows) < batch_size:
            break

    cursor.close()
    mysql_conn.close()

    elapsed = time.time() - t0
    print(f"[KUZU] Complete: {total_loaded:,} edges in {elapsed:.1f}s", flush=True)
    if errors:
        print(f"[KUZU] Errors: {errors}", flush=True)

    # Report storage
    db_size = 0
    for root, dirs, files in os.walk(KUZU_DB_PATH):
        for f in files:
            db_size += os.path.getsize(os.path.join(root, f))
    print(f"[KUZU] Storage: {db_size / 1024 / 1024:.1f} MB", flush=True)


def verify(conn):
    """Print verification stats."""
    print("\n=== Kùzu Verification ===", flush=True)

    r = conn.execute("MATCH (a:Address) RETURN count(a) AS cnt")
    print(f"  Nodes: {r.get_next()[0]:,}")

    r = conn.execute("MATCH ()-[t:TRANSFER]->() RETURN count(t) AS cnt")
    print(f"  Edges: {r.get_next()[0]:,}")

    r = conn.execute("MATCH (a:Address) WHERE a.label IS NOT NULL RETURN count(a)")
    print(f"  Labeled: {r.get_next()[0]:,}")

    print("\n  By edge type:")
    r = conn.execute(
        "MATCH ()-[t:TRANSFER]->() "
        "RETURN t.edge_type, count(*) AS cnt ORDER BY cnt DESC"
    )
    while r.has_next():
        row = r.get_next()
        print(f"    {str(row[0]):18s} {row[1]:>8,}")

    print("\n  By token (top 10):")
    r = conn.execute(
        "MATCH ()-[t:TRANSFER]->() "
        "RETURN t.token_symbol, count(*) AS cnt ORDER BY cnt DESC LIMIT 10"
    )
    while r.has_next():
        row = r.get_next()
        print(f"    {str(row[0]):12s} {row[1]:>8,}")


# ── Query helpers for gateway ──────────────────────────────────────

def query_holder_edges(holder_addresses, token_mint=None):
    """Get all edges between a set of holder addresses."""
    conn = get_connection()
    if conn is None:
        return []

    cypher = (
        "MATCH (a:Address)-[t:TRANSFER]->(b:Address) "
        "WHERE a.address IN $addrs AND b.address IN $addrs "
    )
    params = {"addrs": holder_addresses}
    if token_mint:
        cypher += "AND t.token_mint = $mint "
        params["mint"] = token_mint

    cypher += (
        "RETURN a.address, b.address, t.edge_type, t.token_symbol, "
        "t.amount, t.block_time, t.signature"
    )

    edges = []
    r = conn.execute(cypher, params)
    while r.has_next():
        row = r.get_next()
        edges.append({
            'from': row[0], 'to': row[1],
            'edge_type': row[2], 'token_symbol': row[3],
            'amount': row[4], 'block_time': row[5],
            'signature': row[6],
        })
    return edges


def query_2hop_discovery(holder_addresses, limit=40):
    """Discover 2nd-degree addresses connected to holders."""
    conn = get_connection()
    if conn is None:
        return []

    r = conn.execute(
        "MATCH (h:Address)-[:TRANSFER]-(discovered:Address) "
        "WHERE h.address IN $addrs AND NOT discovered.address IN $addrs "
        "RETURN discovered.address, discovered.type, discovered.label, "
        "       count(*) AS connections "
        "ORDER BY connections DESC LIMIT $lim",
        {"addrs": holder_addresses, "lim": limit}
    )
    results = []
    while r.has_next():
        row = r.get_next()
        results.append({
            'address': row[0], 'type': row[1],
            'label': row[2], 'connections': row[3],
        })
    return results


def query_cycles(holder_addresses):
    """Detect triangular cycles (wash trading signal)."""
    conn = get_connection()
    if conn is None:
        return []

    r = conn.execute(
        "MATCH (a:Address)-[t1:TRANSFER]->(b:Address)-[t2:TRANSFER]->"
        "(c:Address)-[t3:TRANSFER]->(a) "
        "WHERE a.address IN $addrs AND b.address IN $addrs "
        "AND c.address IN $addrs "
        "AND a.type = 'wallet' AND b.type = 'wallet' AND c.type = 'wallet' "
        "AND a <> b AND b <> c "
        "RETURN a.address, b.address, c.address LIMIT 25",
        {"addrs": holder_addresses}
    )
    cycles = []
    while r.has_next():
        row = r.get_next()
        cycles.append({'a': row[0], 'b': row[1], 'c': row[2]})
    return cycles


# ── CLI ────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Kùzu graph loader')
    parser.add_argument('--load', action='store_true', help='Load from MySQL')
    parser.add_argument('--verify', action='store_true', help='Print stats')
    parser.add_argument('--wipe', action='store_true', help='Delete all data')
    parser.add_argument('--token', type=str, default=None,
                        help='Filter by token mint address (e.g. LegiuxbrPh7SKEpnTN1nB2AjtrmSGP7XJtYZV4spump)')
    args = parser.parse_args()

    conn = get_connection()
    if conn is None:
        print("ERROR: kuzu not installed", file=sys.stderr)
        sys.exit(1)

    if args.wipe:
        print("[KUZU] Wiping data...")
        try:
            conn.execute("MATCH ()-[t:TRANSFER]->() DELETE t")
            conn.execute("MATCH (n:Address) DELETE n")
        except Exception:
            pass
        print("[KUZU] Done")

    if args.load:
        create_schema(conn)
        load_from_mysql(conn, token_mint=args.token)
        verify(conn)
    elif args.verify:
        verify(conn)
