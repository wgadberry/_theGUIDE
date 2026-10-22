"""
Query Router Service

Smart API gateway that dispatches graph queries to the optimal engine:
  - Neo4j:     indexed lookups, pattern matching, filtered aggregates
  - NetworkX:  graph algorithms, components, centrality, clustering

Runs on port 8095. Both engines must be available.
"""

import time
import requests
from neo4j import GraphDatabase
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from typing import Optional

# ── Config ──────────────────────────────────────────────────
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "neo4jpassword"
NX_BASE = "http://localhost:8090"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

app = FastAPI(title="T16O Graph Query Router", version="1.0")


# ── Helpers ─────────────────────────────────────────────────

def neo4j_query(cypher, **params):
    """Execute a Cypher query and return (elapsed_ms, records)."""
    t0 = time.time()
    with driver.session() as session:
        result = session.run(cypher, **params)
        records = [dict(r) for r in result]
    return round((time.time() - t0) * 1000, 2), records


def nx_query(endpoint, params=None):
    """Call NetworkX service and return (elapsed_ms, data)."""
    t0 = time.time()
    resp = requests.get(f"{NX_BASE}{endpoint}", params=params, timeout=60)
    data = resp.json()
    total_ms = round((time.time() - t0) * 1000, 2)
    return total_ms, data


def route_response(engine, elapsed_ms, results, query_type):
    """Standard response envelope."""
    return {
        "engine": engine,
        "query_type": query_type,
        "elapsed_ms": elapsed_ms,
        "count": len(results) if isinstance(results, list) else None,
        "results": results,
    }


# ─────────────────────────────────────────────────────────────
#  NEO4J-ROUTED QUERIES (indexed lookups, pattern matching)
# ─────────────────────────────────────────────────────────────

@app.get("/api/address/{address}")
def get_address_edges(
    address: str,
    edge_type: Optional[str] = None,
    token: Optional[str] = None,
    limit: int = Query(default=50, le=500),
):
    """Get all edges for an address. Routed to Neo4j (indexed lookup)."""
    where_clauses = []
    params = {"addr": address, "limit": limit}

    if edge_type:
        where_clauses.append("e.edge_type = $edge_type")
        params["edge_type"] = edge_type
    if token:
        where_clauses.append("e.token_symbol = $token")
        params["token"] = token

    where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    cypher = f"""
        MATCH (a:Address {{address: $addr}})-[e:GUIDE_EDGE]-(b:Address)
        {where}
        RETURN b.address AS address, b.address_type AS address_type, b.label AS label,
               e.edge_type AS edge_type, e.category AS category, e.direction AS direction,
               e.token_symbol AS token, e.amount AS amount, e.tx_signature AS signature,
               e.dex AS dex, e.pool_address AS pool_address, e.block_time_utc AS block_time
        ORDER BY e.block_time DESC
        LIMIT $limit
    """

    elapsed, records = neo4j_query(cypher, **params)
    return route_response("neo4j", elapsed, records, "address_lookup")


@app.get("/api/address/{address}/profile")
def get_address_profile(address: str):
    """Address profile with aggregated stats. Routed to Neo4j."""
    cypher = """
        MATCH (a:Address {address: $addr})
        OPTIONAL MATCH (a)-[out:GUIDE_EDGE]->()
        WITH a, COUNT(out) AS out_degree
        OPTIONAL MATCH (a)<-[inc:GUIDE_EDGE]-()
        WITH a, out_degree, COUNT(inc) AS in_degree
        OPTIONAL MATCH (a)-[e:GUIDE_EDGE]-()
        WITH a, out_degree, in_degree,
             COLLECT(DISTINCT e.edge_type) AS edge_types,
             COLLECT(DISTINCT e.token_symbol) AS tokens,
             COLLECT(DISTINCT e.dex) AS dexes
        RETURN a.address AS address, a.address_type AS address_type, a.label AS label,
               a.age_days AS age_days, out_degree, in_degree,
               edge_types, tokens, dexes
    """
    elapsed, records = neo4j_query(cypher, addr=address)
    if not records:
        raise HTTPException(status_code=404, detail="Address not found")
    return route_response("neo4j", elapsed, records[0], "address_profile")


@app.get("/api/transaction/{signature}")
def get_transaction_edges(signature: str):
    """All edges in a transaction. Routed to Neo4j (indexed lookup on sig)."""
    cypher = """
        MATCH (a:Address)-[e:GUIDE_EDGE]->(b:Address)
        WHERE e.tx_signature = $sig
        RETURN a.address AS from_address, a.address_type AS from_type, a.label AS from_label,
               b.address AS to_address, b.address_type AS to_type, b.label AS to_label,
               e.edge_type AS edge_type, e.token_symbol AS token, e.amount AS amount,
               e.dex AS dex, e.pool_address AS pool_address, e.swap_direction AS swap_direction,
               e.fee AS fee, e.priority_fee AS priority_fee
        ORDER BY e.edge_type
    """
    elapsed, records = neo4j_query(cypher, sig=signature)
    return route_response("neo4j", elapsed, records, "transaction_lookup")


@app.get("/api/pattern/swap-path")
def find_swap_path(
    from_token: str,
    to_token: str,
    limit: int = Query(default=10, le=100),
):
    """Find swap routes between two tokens. Routed to Neo4j (pattern matching)."""
    cypher = """
        MATCH (a:Address)-[e1:GUIDE_EDGE]->(pool:Address)-[e2:GUIDE_EDGE]->(b:Address)
        WHERE e1.edge_type = 'swap_in' AND e1.token_symbol = $from_token
          AND e2.edge_type = 'swap_out' AND e2.token_symbol = $to_token
          AND e1.tx_signature = e2.tx_signature
        RETURN DISTINCT e1.dex AS dex, pool.address AS pool_address, pool.label AS pool_label,
               e1.token_symbol AS sold, e2.token_symbol AS bought,
               COUNT(*) AS swap_count
        ORDER BY swap_count DESC
        LIMIT $limit
    """
    elapsed, records = neo4j_query(cypher, from_token=from_token, to_token=to_token, limit=limit)
    return route_response("neo4j", elapsed, records, "pattern_swap_path")


@app.get("/api/pattern/multi-hop")
def multi_hop_trace(
    address: str,
    hops: int = Query(default=2, le=4),
    address_type: Optional[str] = None,
    limit: int = Query(default=100, le=500),
):
    """Multi-hop traversal from an address. Routed to Neo4j."""
    type_filter = ""
    params = {"addr": address, "limit": limit}
    if address_type:
        type_filter = "AND neighbor.address_type = $addr_type"
        params["addr_type"] = address_type

    cypher = f"""
        MATCH path = (start:Address {{address: $addr}})-[:GUIDE_EDGE*1..{hops}]-(neighbor:Address)
        WHERE neighbor.address <> $addr {type_filter}
        WITH DISTINCT neighbor, length(path) AS distance
        RETURN neighbor.address AS address, neighbor.address_type AS address_type,
               neighbor.label AS label, distance
        ORDER BY distance, address
        LIMIT $limit
    """
    elapsed, records = neo4j_query(cypher, **params)
    return route_response("neo4j", elapsed, records, "multi_hop_trace")


@app.get("/api/aggregate/top-addresses")
def top_addresses(
    edge_type: Optional[str] = None,
    token: Optional[str] = None,
    limit: int = Query(default=20, le=100),
):
    """Top addresses by edge count. Routed to Neo4j (filtered aggregate)."""
    where_clauses = []
    params = {"limit": limit}

    if edge_type:
        where_clauses.append("e.edge_type = $edge_type")
        params["edge_type"] = edge_type
    if token:
        where_clauses.append("e.token_symbol = $token")
        params["token"] = token

    where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    cypher = f"""
        MATCH (a:Address)-[e:GUIDE_EDGE]->()
        {where}
        WITH a, COUNT(e) AS degree
        RETURN a.address AS address, a.address_type AS address_type,
               a.label AS label, degree
        ORDER BY degree DESC
        LIMIT $limit
    """
    elapsed, records = neo4j_query(cypher, **params)
    return route_response("neo4j", elapsed, records, "top_addresses")


# ─────────────────────────────────────────────────────────────
#  NETWORKX-ROUTED QUERIES (algorithms, analytics)
# ─────────────────────────────────────────────────────────────

@app.get("/api/algorithm/components")
def connected_components(min_size: int = Query(default=3)):
    """Connected components. Routed to NetworkX (graph algorithm)."""
    elapsed, data = nx_query("/query/connected-components", {"min_size": min_size})
    return route_response("networkx", elapsed, data.get("components", []), "connected_components")


@app.get("/api/algorithm/shortest-path")
def shortest_path(source: str, target: str):
    """Shortest path between two addresses. Routed to NetworkX."""
    elapsed, data = nx_query("/query/shortest-path", {"source": source, "target": target})
    return route_response("networkx", elapsed, data, "shortest_path")


@app.get("/api/algorithm/neighbors")
def bfs_neighbors(address: str, depth: int = Query(default=2, le=4)):
    """BFS neighbor discovery. Routed to NetworkX (traversal algorithm)."""
    elapsed, data = nx_query("/query/neighbors", {"address": address, "depth": depth})
    return route_response("networkx", elapsed, data, "bfs_neighbors")


@app.get("/api/algorithm/edge-distribution")
def edge_distribution():
    """Edge type distribution. Routed to NetworkX (full scan)."""
    elapsed, data = nx_query("/query/edge-type-counts")
    return route_response("networkx", elapsed, data.get("results", []), "edge_distribution")


# ─────────────────────────────────────────────────────────────
#  HYBRID QUERIES (both engines)
# ─────────────────────────────────────────────────────────────

@app.get("/api/hybrid/address-analysis")
def address_analysis(address: str):
    """Full address analysis combining both engines.

    Neo4j:     profile + recent edges (fast indexed lookup)
    NetworkX:  neighborhood graph metrics (algorithms)
    """
    # Neo4j: profile + recent edges
    t0 = time.time()

    profile_cypher = """
        MATCH (a:Address {address: $addr})
        OPTIONAL MATCH (a)-[out:GUIDE_EDGE]->()
        WITH a, COUNT(out) AS out_degree
        OPTIONAL MATCH (a)<-[inc:GUIDE_EDGE]-()
        WITH a, out_degree, COUNT(inc) AS in_degree
        OPTIONAL MATCH (a)-[e:GUIDE_EDGE]-()
        WITH a, out_degree, in_degree,
             COLLECT(DISTINCT e.token_symbol) AS tokens,
             COLLECT(DISTINCT e.dex) AS dexes
        RETURN a.address AS address, a.address_type AS address_type, a.label AS label,
               a.age_days AS age_days, out_degree, in_degree, tokens, dexes
    """
    _, profile = neo4j_query(profile_cypher, addr=address)
    if not profile:
        raise HTTPException(status_code=404, detail="Address not found")

    edges_cypher = """
        MATCH (a:Address {address: $addr})-[e:GUIDE_EDGE]-(b:Address)
        RETURN b.address AS address, b.label AS label,
               e.edge_type AS edge_type, e.token_symbol AS token,
               e.amount AS amount, e.block_time_utc AS block_time
        ORDER BY e.block_time DESC
        LIMIT 10
    """
    _, recent_edges = neo4j_query(edges_cypher, addr=address)

    neo4j_ms = round((time.time() - t0) * 1000, 2)

    # NetworkX: neighborhood metrics
    nx_ms, nx_neighbors = nx_query("/query/neighbors", {"address": address, "depth": 2})

    total_ms = round((time.time() - t0) * 1000, 2)

    return {
        "engine": "hybrid (neo4j + networkx)",
        "query_type": "address_analysis",
        "elapsed_ms": total_ms,
        "neo4j_ms": neo4j_ms,
        "networkx_ms": nx_ms,
        "profile": profile[0],
        "recent_edges": recent_edges,
        "neighborhood": {
            "two_hop_count": nx_neighbors.get("count", 0),
            "neighbors": nx_neighbors.get("neighbors", [])[:20],
        },
    }


@app.get("/api/hybrid/token-analysis")
def token_analysis(token: str, limit: int = Query(default=20)):
    """Token flow analysis combining both engines.

    Neo4j:     top traders, pools, volume (filtered aggregates)
    NetworkX:  connected component containing token traders
    """
    t0 = time.time()

    # Neo4j: top traders for this token
    traders_cypher = """
        MATCH (a:Address)-[e:GUIDE_EDGE]->()
        WHERE e.token_symbol = $token AND a.address_type = 'wallet'
        WITH a, COUNT(e) AS trades, SUM(e.amount) AS total_volume
        RETURN a.address AS address, a.label AS label, trades, total_volume
        ORDER BY trades DESC
        LIMIT $limit
    """
    _, top_traders = neo4j_query(traders_cypher, token=token, limit=limit)

    # Neo4j: pools used for this token
    pools_cypher = """
        MATCH ()-[e:GUIDE_EDGE]->()
        WHERE e.token_symbol = $token AND e.dex IS NOT NULL
        RETURN e.dex AS dex, e.pool_address AS pool_address, COUNT(*) AS edge_count
        ORDER BY edge_count DESC
        LIMIT 10
    """
    _, pools = neo4j_query(pools_cypher, token=token)

    neo4j_ms = round((time.time() - t0) * 1000, 2)

    # NetworkX: component analysis for top trader
    nx_ms = 0
    component_info = None
    if top_traders:
        top_addr = top_traders[0]["address"]
        nx_ms, nx_data = nx_query("/query/neighbors", {"address": top_addr, "depth": 2})
        component_info = {
            "seed_address": top_addr,
            "two_hop_reach": nx_data.get("count", 0),
        }

    total_ms = round((time.time() - t0) * 1000, 2)

    return {
        "engine": "hybrid (neo4j + networkx)",
        "query_type": "token_analysis",
        "elapsed_ms": total_ms,
        "neo4j_ms": neo4j_ms,
        "networkx_ms": nx_ms,
        "token": token,
        "top_traders": top_traders,
        "pools": pools,
        "network_reach": component_info,
    }


# ─────────────────────────────────────────────────────────────
#  HEALTH / META
# ─────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    """Check both engines."""
    neo4j_ok = False
    nx_ok = False

    try:
        _, r = neo4j_query("RETURN 1 AS ok")
        neo4j_ok = len(r) > 0
    except Exception:
        pass

    try:
        resp = requests.get(f"{NX_BASE}/status", timeout=5)
        nx_ok = resp.json().get("status") == "ready"
    except Exception:
        pass

    return {
        "neo4j": "healthy" if neo4j_ok else "down",
        "networkx": "healthy" if nx_ok else "down",
        "router": "healthy",
    }
