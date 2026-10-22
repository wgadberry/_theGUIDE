#!/usr/bin/env python3
"""
kuzu_benchmark.py — Apples-to-apples: Kùzu vs NetworkX from same MySQL data.

Both graphs built from scratch, same edges, same holder set.
No pre-loaded data needed — runs standalone in any container with kuzu + networkx.

Usage:
  python3 kuzu_benchmark.py --token LegiuxbrPh7SKEpnTN1nB2AjtrmSGP7XJtYZV4spump
"""
import os, sys, time, argparse, tempfile, shutil

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import networkx as nx
import kuzu


def get_mysql_conn():
    import mysql.connector
    from t16o_exchange.guide.common.config import load_config
    cfg = load_config()
    return mysql.connector.connect(
        host=cfg.get('DB_HOST', '127.0.0.1'),
        port=int(cfg.get('DB_PORT', 3306)),
        user=cfg.get('DB_USER', 'root'),
        password=cfg.get('DB_PASSWORD', ''),
        database=cfg.get('DB_NAME', 't16o_db'),
    )


def fetch_holder_edges(token_mint, holder_limit=80):
    """Fetch top holders + all edges between them (mirrors gateway path)."""
    conn = get_mysql_conn()
    cur = conn.cursor(dictionary=True)

    # Resolve token_id
    cur.execute(
        "SELECT t.id FROM tx_token t "
        "JOIN tx_address a ON a.id = t.mint_address_id "
        "WHERE a.address = %s", (token_mint,))
    row = cur.fetchone()
    if not row:
        print(f"Token {token_mint} not found"); sys.exit(1)
    token_id = row['id']

    # Top holders by edge count (wallets only)
    cur.execute("""
        SELECT a.address, COUNT(*) as edge_cnt
        FROM tx_guide g
        JOIN tx_address a ON a.id IN (g.from_address_id, g.to_address_id)
        WHERE g.token_id = %s AND a.address_type = 'wallet'
        GROUP BY a.address
        ORDER BY edge_cnt DESC
        LIMIT %s
    """, (token_id, holder_limit))
    holders = [r['address'] for r in cur.fetchall()]

    # All edges between those holders
    ph = ','.join(['%s'] * len(holders))
    cur.execute(f"""
        SELECT fa.address AS src, ta.address AS dst,
               IFNULL(gt.type_code, 'UNKNOWN') AS edge_type,
               CAST(IFNULL(g.amount, 0) AS CHAR) AS amount,
               IFNULL(g.block_time, 0) AS block_time
        FROM tx_guide g
        JOIN tx_address fa ON fa.id = g.from_address_id
        JOIN tx_address ta ON ta.id = g.to_address_id
        LEFT JOIN tx_guide_type gt ON gt.id = g.edge_type_id
        WHERE g.token_id = %s
          AND fa.address IN ({ph}) AND ta.address IN ({ph})
    """, (token_id, *holders, *holders))
    edges = cur.fetchall()
    cur.close(); conn.close()
    return holders, edges


def bench(label, fn, iterations=5):
    """Benchmark, return (result, avg_ms, min_ms)."""
    times = []
    result = None
    for _ in range(iterations):
        t0 = time.perf_counter()
        result = fn()
        times.append((time.perf_counter() - t0) * 1000)
    avg = sum(times) / len(times)
    mn = min(times)
    print(f"  {label:45s}  avg={avg:8.2f}ms  min={mn:8.2f}ms", flush=True)
    return result, avg


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--token', required=True)
    parser.add_argument('--holders', type=int, default=80)
    args = parser.parse_args()

    print(f"\n{'='*70}")
    print(f"  Kuzu vs NetworkX — SAME DATA benchmark")
    print(f"  Token: {args.token[:16]}...")
    print(f"{'='*70}\n")

    # ── Fetch shared dataset from MySQL ──
    t0 = time.perf_counter()
    holders, edges = fetch_holder_edges(args.token, args.holders)
    mysql_ms = (time.perf_counter() - t0) * 1000
    print(f"MySQL fetch: {len(holders)} holders, {len(edges)} edges ({mysql_ms:.0f}ms)\n")

    if not edges:
        print("No edges found between holders. Nothing to benchmark.")
        return

    # ── Build NetworkX graph ──
    t0 = time.perf_counter()
    G = nx.Graph()
    DG = nx.DiGraph()
    for h in holders:
        G.add_node(h)
    for e in edges:
        s, d = e['src'], e['dst']
        if G.has_node(s) and G.has_node(d):
            if G.has_edge(s, d):
                G[s][d]['cnt'] += 1
            else:
                G.add_edge(s, d, cnt=1)
            DG.add_edge(s, d)
    nx_build_ms = (time.perf_counter() - t0) * 1000
    print(f"NetworkX build: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges ({nx_build_ms:.1f}ms)")

    # ── Build Kuzu graph (temp dir, same edges) ──
    tmpdir = tempfile.mkdtemp(prefix='kuzu_bench_')
    kuzu_path = os.path.join(tmpdir, 't16o_bench')
    t0 = time.perf_counter()
    db = kuzu.Database(kuzu_path)
    kc = kuzu.Connection(db)
    kc.execute("CREATE NODE TABLE IF NOT EXISTS Address (address STRING, PRIMARY KEY (address))")
    kc.execute("""CREATE REL TABLE IF NOT EXISTS TRANSFER (
        FROM Address TO Address,
        edge_type STRING, amount STRING, block_time INT64
    )""")

    for h in holders:
        kc.execute("MERGE (a:Address {address: $addr})", {"addr": h})

    for e in edges:
        kc.execute(
            "MATCH (a:Address {address: $src}), (b:Address {address: $dst}) "
            "CREATE (a)-[:TRANSFER {edge_type: $et, amount: $amt, block_time: $bt}]->(b)",
            {"src": e['src'], "dst": e['dst'], "et": e['edge_type'],
             "amt": e['amount'], "bt": int(e['block_time'])}
        )
    kuzu_build_ms = (time.perf_counter() - t0) * 1000
    print(f"Kuzu build:     {len(holders)} nodes, {len(edges)} edges ({kuzu_build_ms:.1f}ms)")

    holder_set = list(holders)
    print()

    # ══════════════════════════════════════════════════════════════
    # 1. EDGE QUERY — all edges between holders
    # ══════════════════════════════════════════════════════════════
    print("── 1. Edge Query (all edges between holder set) ──")

    def nx_edges():
        return [(u, v, d) for u, v, d in G.edges(data=True)]

    def kuzu_edges():
        r = kc.execute(
            "MATCH (a:Address)-[t:TRANSFER]->(b:Address) "
            "RETURN a.address, b.address, t.edge_type, t.amount")
        out = []
        while r.has_next():
            out.append(r.get_next())
        return out

    nx_e, nx_e_ms = bench("NetworkX  G.edges(data=True)", nx_edges)
    kz_e, kz_e_ms = bench("Kuzu      MATCH (a)-[t]->(b) RETURN ...", kuzu_edges)
    print(f"  NX: {len(nx_e)} edges, Kuzu: {len(kz_e)} edges")
    ratio = nx_e_ms / max(kz_e_ms, 0.001)
    winner = "Kuzu" if ratio > 1 else "NX"
    print(f"  Winner: {winner} ({max(ratio, 1/ratio):.1f}x)\n")

    # ══════════════════════════════════════════════════════════════
    # 2. CONNECTED COMPONENTS
    # ══════════════════════════════════════════════════════════════
    print("── 2. Connected Components ──")

    def nx_cc():
        return list(nx.connected_components(G))

    def kuzu_cc():
        r = kc.execute(
            "MATCH (a:Address)-[:TRANSFER]-(b:Address) "
            "WHERE a.address < b.address "
            "RETURN DISTINCT a.address, b.address")
        parent = {h: h for h in holders}
        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x
        while r.has_next():
            row = r.get_next()
            ra, rb = find(row[0]), find(row[1])
            if ra != rb: parent[ra] = rb
        groups = {}
        for h in holders:
            groups.setdefault(find(h), set()).add(h)
        return list(groups.values())

    nx_cc_r, nx_cc_ms = bench("NetworkX  connected_components", nx_cc)
    kz_cc_r, kz_cc_ms = bench("Kuzu      edge fetch + union-find", kuzu_cc)
    print(f"  NX: {len(nx_cc_r)} components, Kuzu: {len(kz_cc_r)} components")
    ratio = nx_cc_ms / max(kz_cc_ms, 0.001)
    winner = "Kuzu" if ratio > 1 else "NX"
    print(f"  Winner: {winner} ({max(ratio, 1/ratio):.1f}x)\n")

    # ══════════════════════════════════════════════════════════════
    # 3. DEGREE CENTRALITY
    # ══════════════════════════════════════════════════════════════
    print("── 3. Degree Centrality (top node) ──")

    def nx_dc():
        return max(nx.degree_centrality(G).items(), key=lambda x: x[1])

    def kuzu_dc():
        r = kc.execute(
            "MATCH (a:Address)-[t:TRANSFER]-(b:Address) "
            "RETURN a.address, count(t) AS deg ORDER BY deg DESC LIMIT 1")
        return r.get_next() if r.has_next() else None

    nx_dc_r, nx_dc_ms = bench("NetworkX  degree_centrality", nx_dc)
    kz_dc_r, kz_dc_ms = bench("Kuzu      COUNT + ORDER BY", kuzu_dc)
    print(f"  NX top: {nx_dc_r[0][:16]}... ({nx_dc_r[1]:.4f})")
    if kz_dc_r:
        print(f"  Kuzu top: {kz_dc_r[0][:16]}... (degree={kz_dc_r[1]})")
    ratio = nx_dc_ms / max(kz_dc_ms, 0.001)
    winner = "Kuzu" if ratio > 1 else "NX"
    print(f"  Winner: {winner} ({max(ratio, 1/ratio):.1f}x)\n")

    # ══════════════════════════════════════════════════════════════
    # 4. CYCLE DETECTION (triangles — wash trading)
    # ══════════════════════════════════════════════════════════════
    print("── 4. Cycle Detection (triangles) ──")

    def nx_cyc():
        cycles = []
        for c in nx.simple_cycles(DG):
            if len(c) == 3:
                cycles.append(c)
            if len(cycles) >= 25:
                break
        return cycles

    def kuzu_cyc():
        r = kc.execute(
            "MATCH (a:Address)-[:TRANSFER]->(b:Address)-[:TRANSFER]->"
            "(c:Address)-[:TRANSFER]->(a) "
            "WHERE a.address < b.address AND b.address < c.address "
            "RETURN a.address, b.address, c.address LIMIT 25")
        out = []
        while r.has_next():
            out.append(r.get_next())
        return out

    nx_cyc_r, nx_cyc_ms = bench("NetworkX  simple_cycles (len=3, max 25)", nx_cyc)
    kz_cyc_r, kz_cyc_ms = bench("Kuzu      triangle pattern match", kuzu_cyc)
    print(f"  NX: {len(nx_cyc_r)} triangles, Kuzu: {len(kz_cyc_r)} triangles")
    ratio = nx_cyc_ms / max(kz_cyc_ms, 0.001)
    winner = "Kuzu" if ratio > 1 else "NX"
    print(f"  Winner: {winner} ({max(ratio, 1/ratio):.1f}x)\n")

    # ══════════════════════════════════════════════════════════════
    # 5. NEIGHBOR AGGREGATION (per-node edge type breakdown)
    # ══════════════════════════════════════════════════════════════
    print("── 5. Neighbor Aggregation (edge types per node, top 10) ──")

    test_addr = holders[0]

    def nx_nbr():
        out = {}
        for e in edges:
            if e['src'] == test_addr or e['dst'] == test_addr:
                et = e['edge_type']
                out[et] = out.get(et, 0) + 1
        return out

    def kuzu_nbr():
        r = kc.execute(
            "MATCH (a:Address {address: $addr})-[t:TRANSFER]-(b:Address) "
            "RETURN t.edge_type, count(t) AS cnt",
            {"addr": test_addr})
        out = {}
        while r.has_next():
            row = r.get_next()
            out[row[0]] = row[1]
        return out

    nx_nb_r, nx_nb_ms = bench("NetworkX  manual edge scan", nx_nbr)
    kz_nb_r, kz_nb_ms = bench("Kuzu      MATCH + GROUP BY", kuzu_nbr)
    print(f"  Test addr: {test_addr[:16]}...")
    print(f"  NX: {nx_nb_r}")
    print(f"  Kuzu: {kz_nb_r}")
    ratio = nx_nb_ms / max(kz_nb_ms, 0.001)
    winner = "Kuzu" if ratio > 1 else "NX"
    print(f"  Winner: {winner} ({max(ratio, 1/ratio):.1f}x)\n")

    # ══════════════════════════════════════════════════════════════
    print(f"{'='*70}")
    print(f"  SUMMARY (avg ms, 5 iterations each)")
    print(f"{'='*70}")
    print(f"  {'Test':<25s} {'NetworkX':>10s} {'Kuzu':>10s} {'Winner':>10s}")
    print(f"  {'-'*55}")

    results = [
        ("Edge query",    nx_e_ms,   kz_e_ms),
        ("Components",    nx_cc_ms,  kz_cc_ms),
        ("Centrality",    nx_dc_ms,  kz_dc_ms),
        ("Cycles",        nx_cyc_ms, kz_cyc_ms),
        ("Neighbor agg",  nx_nb_ms,  kz_nb_ms),
    ]
    for name, n, k in results:
        w = "Kuzu" if n > k else "NX"
        factor = max(n, k) / max(min(n, k), 0.001)
        print(f"  {name:<25s} {n:>8.2f}ms {k:>8.2f}ms {w:>6s} {factor:>5.1f}x")

    print(f"\n  Graph build:  NX {nx_build_ms:.1f}ms  vs  Kuzu {kuzu_build_ms:.1f}ms")
    print(f"  Dataset:      {len(holders)} holders, {len(edges)} edges (identical)\n")

    # Cleanup
    shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == '__main__':
    main()
