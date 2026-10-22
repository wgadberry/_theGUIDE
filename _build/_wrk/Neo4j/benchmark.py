"""
Benchmark: Neo4j vs NetworkX

Runs identical queries against both graph engines and compares latency.
"""

import time
import json
import requests
from neo4j import GraphDatabase

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "neo4jpassword"
NX_BASE = "http://localhost:8090"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def neo4j_query(cypher, **params):
    t0 = time.time()
    with driver.session() as session:
        result = session.run(cypher, **params)
        records = [dict(r) for r in result]
    elapsed = round((time.time() - t0) * 1000, 2)
    return elapsed, records


def nx_query(endpoint, params=None):
    t0 = time.time()
    resp = requests.get(f"{NX_BASE}{endpoint}", params=params)
    data = resp.json()
    elapsed_total = round((time.time() - t0) * 1000, 2)
    elapsed_server = data.get("elapsed_ms", 0)
    return elapsed_total, elapsed_server, data


def divider(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def run_benchmark():
    print("Neo4j vs NetworkX Benchmark")
    print(f"Neo4j edges: ~1M | NetworkX edges: ~1M\n")

    # ── 1. Edge type counts ─────────────────────────────────
    divider("QUERY 1: Edge type counts (full scan aggregate)")

    neo_ms, neo_rows = neo4j_query("""
        MATCH ()-[e:GUIDE_EDGE]->()
        RETURN e.edge_type AS edge_type, COUNT(*) AS cnt
        ORDER BY cnt DESC
    """)
    print(f"  Neo4j:     {neo_ms:>8.1f} ms  ({len(neo_rows)} types)")

    nx_total, nx_server, nx_data = nx_query("/query/edge-type-counts")
    print(f"  NetworkX:  {nx_server:>8.1f} ms server / {nx_total:.1f} ms total  ({len(nx_data['results'])} types)")

    # ── 2. Program + token counts ───────────────────────────
    divider("QUERY 2: Program-token counts (filtered aggregate)")

    neo_ms, neo_rows = neo4j_query("""
        MATCH ()-[e:GUIDE_EDGE]->()
        WHERE e.program_name IS NOT NULL
        RETURN e.program_name, e.token_symbol, COUNT(*) AS cnt
        ORDER BY cnt DESC LIMIT 10
    """)
    print(f"  Neo4j:     {neo_ms:>8.1f} ms  (top {len(neo_rows)} results)")
    for r in neo_rows[:3]:
        print(f"             {r['e.program_name']:30s} {str(r['e.token_symbol']):10s} {r['cnt']}")

    nx_total, nx_server, nx_data = nx_query("/query/program-token-counts", {"limit": 10})
    print(f"  NetworkX:  {nx_server:>8.1f} ms server / {nx_total:.1f} ms total")
    for r in nx_data["results"][:3]:
        print(f"             {r['program_name']:30s} {str(r['token_symbol']):10s} {r['count']}")

    # ── 3. Single address edges ─────────────────────────────
    divider("QUERY 3: Edges for a single address (point lookup)")

    # Pick a busy address from Neo4j
    _, addr_rows = neo4j_query("""
        MATCH (a:Address)-[e:GUIDE_EDGE]->()
        WITH a, COUNT(e) AS degree
        ORDER BY degree DESC LIMIT 1
        RETURN a.address AS address, degree
    """)
    test_addr = addr_rows[0]["address"]
    degree = addr_rows[0]["degree"]
    print(f"  Test address: {test_addr[:20]}... (degree: {degree})")

    neo_ms, neo_rows = neo4j_query("""
        MATCH (a:Address {address: $addr})-[e:GUIDE_EDGE]-(b:Address)
        RETURN b.address AS neighbor, e.edge_type AS edge_type, e.token_symbol AS token
        LIMIT 25
    """, addr=test_addr)
    print(f"  Neo4j:     {neo_ms:>8.1f} ms  ({len(neo_rows)} results)")

    nx_total, nx_server, nx_data = nx_query("/query/address-edges", {"address": test_addr, "limit": 25})
    print(f"  NetworkX:  {nx_server:>8.1f} ms server / {nx_total:.1f} ms total  ({nx_data['count']} results)")

    # ── 4. Neighborhood (2-hop BFS) ─────────────────────────
    divider("QUERY 4: 2-hop neighborhood (graph traversal)")

    # Use a wallet with moderate degree
    _, wallet_rows = neo4j_query("""
        MATCH (a:Address {address_type: 'wallet'})-[e:GUIDE_EDGE]->()
        WITH a, COUNT(e) AS degree
        WHERE degree > 5 AND degree < 50
        RETURN a.address AS address, degree
        LIMIT 1
    """)
    if wallet_rows:
        walk_addr = wallet_rows[0]["address"]
        walk_degree = wallet_rows[0]["degree"]
        print(f"  Test wallet: {walk_addr[:20]}... (degree: {walk_degree})")

        neo_ms, neo_rows = neo4j_query("""
            MATCH (start:Address {address: $addr})-[:GUIDE_EDGE*1..2]-(neighbor:Address)
            WHERE neighbor.address <> $addr
            RETURN DISTINCT neighbor.address AS address, neighbor.address_type AS type
            LIMIT 100
        """, addr=walk_addr)
        print(f"  Neo4j:     {neo_ms:>8.1f} ms  ({len(neo_rows)} neighbors)")

        nx_total, nx_server, nx_data = nx_query("/query/neighbors", {"address": walk_addr, "depth": 2})
        print(f"  NetworkX:  {nx_server:>8.1f} ms server / {nx_total:.1f} ms total  ({nx_data['count']} neighbors)")
    else:
        print("  (skipped — no suitable wallet found)")

    # ── 5. Shortest path ────────────────────────────────────
    divider("QUERY 5: Shortest path between two wallets")

    _, two_wallets = neo4j_query("""
        MATCH (a:Address {address_type: 'wallet'})-[:GUIDE_EDGE]->(b:Address {address_type: 'wallet'})
        WHERE a.address <> b.address
        RETURN a.address AS src, b.address AS dst
        LIMIT 1
    """)
    if two_wallets:
        src = two_wallets[0]["src"]
        dst = two_wallets[0]["dst"]
        print(f"  From: {src[:20]}...")
        print(f"  To:   {dst[:20]}...")

        neo_ms, neo_rows = neo4j_query("""
            MATCH path = shortestPath(
                (a:Address {address: $src})-[:GUIDE_EDGE*..6]-(b:Address {address: $dst})
            )
            RETURN length(path) AS hops, [n IN nodes(path) | n.address] AS path
        """, src=src, dst=dst)
        if neo_rows:
            print(f"  Neo4j:     {neo_ms:>8.1f} ms  ({neo_rows[0]['hops']} hops)")
        else:
            print(f"  Neo4j:     {neo_ms:>8.1f} ms  (no path found)")

        nx_total, nx_server, nx_data = nx_query("/query/shortest-path", {"source": src, "target": dst})
        if "error" not in nx_data:
            print(f"  NetworkX:  {nx_server:>8.1f} ms server / {nx_total:.1f} ms total  ({nx_data['length']} hops)")
        else:
            print(f"  NetworkX:  {nx_server:>8.1f} ms server / {nx_total:.1f} ms total  ({nx_data['error']})")
    else:
        print("  (skipped — no connected wallet pair found)")

    # ── 6. Connected components ─────────────────────────────
    divider("QUERY 6: Connected components (global graph analysis)")

    print("  Neo4j:     (GDS plugin required — not available in Community edition)")

    nx_total, nx_server, nx_data = nx_query("/query/connected-components", {"min_size": 5})
    print(f"  NetworkX:  {nx_server:>8.1f} ms server / {nx_total:.1f} ms total  ({nx_data['count']} components, largest: {nx_data['components'][0]['size'] if nx_data['components'] else 0})")

    # ── Summary ─────────────────────────────────────────────
    divider("SUMMARY")
    print("  Neo4j:     Disk-backed, Cypher queries, ACID transactions")
    print("  NetworkX:  In-memory Python, no query language overhead")
    print("  Both loaded ~1M edges from same Kafka topic")

    driver.close()


if __name__ == "__main__":
    run_benchmark()
