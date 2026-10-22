"""
NetworkX Graph Service

FastAPI service that:
1. On startup, consumes all guide-edges from Kafka into a NetworkX DiGraph
2. Exposes query endpoints for benchmarking against Neo4j
"""

import json
import time
import threading
from contextlib import asynccontextmanager

import networkx as nx
from fastapi import FastAPI, Query
from confluent_kafka import Consumer, KafkaError

# ── Config ──────────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap.servers": "kafka:29092",
    "group.id": "networkx-loader-v2",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}
TOPIC = "guide-edges"

# ── Graph ───────────────────────────────────────────────────
G = nx.MultiDiGraph()  # directed, allows parallel edges
load_stats = {"edges": 0, "nodes": 0, "status": "starting", "elapsed": 0}


INT_FIELDS = [
    "block_id", "block_time", "decimals",
    "tax_bps", "tax_amount", "fee", "priority_fee",
    "from_age_days", "source_row_id", "from_funding_amount",
]
FLOAT_FIELDS = [
    "amount",
    "from_token_pre_balance", "from_token_post_balance",
    "to_token_pre_balance", "to_token_post_balance",
    "from_sol_pre_balance", "from_sol_post_balance",
    "to_sol_pre_balance", "to_sol_post_balance",
]


def cast_numerics(record):
    for field in INT_FIELDS:
        val = record.get(field)
        if val is not None:
            try:
                record[field] = int(val)
            except (ValueError, TypeError):
                pass
    for field in FLOAT_FIELDS:
        val = record.get(field)
        if val is not None:
            try:
                record[field] = float(val)
            except (ValueError, TypeError):
                pass
    return record


def add_record_to_graph(record):
    """Add a single edge + nodes to the graph."""
    from_addr = record["from_address"]
    to_addr = record["to_address"]
    edge_hash = record.get("edge_hash", "")

    if from_addr not in G:
        G.add_node(from_addr,
                   node_type="Address",
                   address_type=record.get("from_address_type"),
                   label=record.get("from_label"),
                   age_days=record.get("from_age_days"))

    if to_addr not in G:
        G.add_node(to_addr,
                   node_type="Address",
                   address_type=record.get("to_address_type"),
                   label=record.get("to_label"))

    G.add_edge(from_addr, to_addr, key=edge_hash, **{
        "edge_hash": edge_hash,
        "edge_type": record.get("edge_type_code"),
        "category": record.get("edge_category"),
        "direction": record.get("edge_direction"),
        "swap_direction": record.get("swap_direction"),
        "tx_signature": record.get("signature"),
        "token_symbol": record.get("token_symbol"),
        "token_mint": record.get("token_mint"),
        "decimals": record.get("decimals"),
        "amount": record.get("amount"),
        "from_token_pre_balance": record.get("from_token_pre_balance"),
        "from_token_post_balance": record.get("from_token_post_balance"),
        "to_token_pre_balance": record.get("to_token_pre_balance"),
        "to_token_post_balance": record.get("to_token_post_balance"),
        "from_sol_pre_balance": record.get("from_sol_pre_balance"),
        "from_sol_post_balance": record.get("from_sol_post_balance"),
        "to_sol_pre_balance": record.get("to_sol_pre_balance"),
        "to_sol_post_balance": record.get("to_sol_post_balance"),
        "tax_bps": record.get("tax_bps"),
        "tax_amount": record.get("tax_amount"),
        "fee": record.get("fee"),
        "priority_fee": record.get("priority_fee"),
        "pool_address": record.get("pool_address"),
        "pool_label": record.get("pool_label"),
        "dex": record.get("dex"),
        "token_pair": record.get("token_pair"),
        "program_name": record.get("program_name"),
        "program_type": record.get("program_type"),
        "source_code": record.get("source_code"),
        "source_name": record.get("source_name"),
        "block_time": record.get("block_time"),
        "block_time_utc": record.get("block_time_utc"),
    })


def load_from_kafka():
    """Consume all messages from Kafka into NetworkX graph using batch consume."""
    global load_stats
    load_stats["status"] = "loading"
    start = time.time()

    consumer = Consumer({
        **KAFKA_CONFIG,
        "fetch.min.bytes": 1048576,       # 1MB min fetch
        "fetch.max.bytes": 52428800,      # 50MB max fetch
        "max.partition.fetch.bytes": 10485760,  # 10MB per partition
        "queued.max.messages.kbytes": 65536,    # 64MB internal queue
    })
    consumer.subscribe([TOPIC])

    empty_polls = 0
    while empty_polls < 5:
        msgs = consumer.consume(num_messages=5000, timeout=3.0)

        if not msgs:
            empty_polls += 1
            continue

        empty_polls = 0
        for msg in msgs:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                continue

            try:
                record = json.loads(msg.value().decode("utf-8"))
                record = cast_numerics(record)
                add_record_to_graph(record)
            except (json.JSONDecodeError, KeyError):
                continue

        edge_count = G.number_of_edges()
        load_stats["edges"] = edge_count
        elapsed = round(time.time() - start, 1)
        rate = int(edge_count / elapsed) if elapsed > 0 else 0
        if edge_count % 50000 < 5000:
            load_stats["nodes"] = G.number_of_nodes()
            print(f"  {edge_count:,} edges, {load_stats['nodes']:,} nodes ({rate:,}/sec)")

    consumer.close()
    load_stats["nodes"] = G.number_of_nodes()
    load_stats["edges"] = G.number_of_edges()
    load_stats["elapsed"] = round(time.time() - start, 1)
    load_stats["status"] = "ready"
    print(f"\nLoad complete: {load_stats['edges']:,} edges, {load_stats['nodes']:,} nodes in {load_stats['elapsed']}s")


@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = threading.Thread(target=load_from_kafka, daemon=True)
    thread.start()
    yield


app = FastAPI(title="NetworkX Graph Service", lifespan=lifespan)


# ── Endpoints ───────────────────────────────────────────────

@app.get("/status")
def status():
    return load_stats


@app.get("/query/program-token-counts")
def program_token_counts(limit: int = Query(default=10)):
    """Same as: MATCH (a)-[e]->(b) WHERE e.program_name IS NOT NULL
    RETURN e.program_name, e.token_symbol, COUNT(*) ORDER BY cnt DESC"""
    t0 = time.time()
    counts = {}
    for u, v, k, d in G.edges(keys=True, data=True):
        pn = d.get("program_name")
        if pn is not None:
            key = (pn, d.get("token_symbol"))
            counts[key] = counts.get(key, 0) + 1

    results = sorted(counts.items(), key=lambda x: -x[1])[:limit]
    elapsed_ms = round((time.time() - t0) * 1000, 2)
    return {
        "elapsed_ms": elapsed_ms,
        "results": [
            {"program_name": k[0], "token_symbol": k[1], "count": v}
            for k, v in results
        ],
    }


@app.get("/query/address-edges")
def address_edges(address: str, limit: int = Query(default=25)):
    """All edges for a given address (in or out)."""
    t0 = time.time()
    edges_out = [
        {"to": v, **d}
        for u, v, k, d in G.edges(address, keys=True, data=True)
    ] if address in G else []
    edges_in = [
        {"from": u, **d}
        for u, v, k, d in G.in_edges(address, keys=True, data=True)
    ] if address in G else []

    all_edges = (edges_out + edges_in)[:limit]
    elapsed_ms = round((time.time() - t0) * 1000, 2)
    return {"elapsed_ms": elapsed_ms, "count": len(all_edges), "edges": all_edges}


@app.get("/query/neighbors")
def neighbors(address: str, depth: int = Query(default=1, le=3)):
    """BFS neighbors up to N hops."""
    t0 = time.time()
    if address not in G:
        return {"elapsed_ms": 0, "error": "address not found"}

    visited = set()
    frontier = {address}
    for _ in range(depth):
        next_frontier = set()
        for node in frontier:
            for neighbor in G.successors(node):
                if neighbor not in visited and neighbor != address:
                    next_frontier.add(neighbor)
            for neighbor in G.predecessors(node):
                if neighbor not in visited and neighbor != address:
                    next_frontier.add(neighbor)
        visited.update(frontier)
        frontier = next_frontier

    visited.update(frontier)
    visited.discard(address)

    results = []
    for addr in list(visited)[:100]:
        node_data = G.nodes[addr]
        results.append({
            "address": addr,
            "address_type": node_data.get("address_type"),
            "label": node_data.get("label"),
        })

    elapsed_ms = round((time.time() - t0) * 1000, 2)
    return {"elapsed_ms": elapsed_ms, "count": len(results), "neighbors": results}


@app.get("/query/edge-type-counts")
def edge_type_counts():
    """Count edges by type — comparable to Neo4j aggregate query."""
    t0 = time.time()
    counts = {}
    for u, v, k, d in G.edges(keys=True, data=True):
        et = d.get("edge_type", "unknown")
        counts[et] = counts.get(et, 0) + 1

    results = sorted(counts.items(), key=lambda x: -x[1])
    elapsed_ms = round((time.time() - t0) * 1000, 2)
    return {
        "elapsed_ms": elapsed_ms,
        "results": [{"edge_type": k, "count": v} for k, v in results],
    }


@app.get("/query/connected-components")
def connected_components(min_size: int = Query(default=3)):
    """Find connected components (undirected view)."""
    t0 = time.time()
    undirected = G.to_undirected()
    components = [
        c for c in nx.connected_components(undirected) if len(c) >= min_size
    ]
    components.sort(key=len, reverse=True)

    results = []
    for comp in components[:20]:
        results.append({
            "size": len(comp),
            "sample_addresses": list(comp)[:5],
        })

    elapsed_ms = round((time.time() - t0) * 1000, 2)
    return {"elapsed_ms": elapsed_ms, "count": len(components), "components": results}


@app.get("/query/shortest-path")
def shortest_path(source: str, target: str):
    """Shortest path between two addresses."""
    t0 = time.time()
    if source not in G or target not in G:
        return {"elapsed_ms": 0, "error": "address not found"}

    try:
        path = nx.shortest_path(G, source, target)
        elapsed_ms = round((time.time() - t0) * 1000, 2)
        return {"elapsed_ms": elapsed_ms, "length": len(path) - 1, "path": path}
    except nx.NetworkXNoPath:
        elapsed_ms = round((time.time() - t0) * 1000, 2)
        return {"elapsed_ms": elapsed_ms, "error": "no path found"}
