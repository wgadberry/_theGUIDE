"""
qdrant_search_service.py
─────────────────────────────────────────────────────────────────────────────
Lightweight search microservice for T16O Graph RAG.

Runs on the Qdrant EC2 (54.175.44.146:5400).
Embeds queries locally with all-MiniLM-L6-v2, searches Qdrant, returns
results in the shape expected by bmap-viewer-v3.html.

The gateway on services EC2 proxies /api/graph/rag/search → here.

Usage:
    python3 qdrant_search_service.py              # port 5400
    python3 qdrant_search_service.py --port 5400

Author : theGuide / T16O
─────────────────────────────────────────────────────────────────────────────
"""

import json
import logging
import os
import time
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import requests as http_requests

from qdrant_client_t16o import get_client, T16OVectorClient, VECTOR_DIM

# ─── Logging ─────────────────────────────────────────────────────────────────

log = logging.getLogger("t16o.rag.search")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ─── Config ──────────────────────────────────────────────────────────────────

_config_candidates = [
    os.path.join(os.path.dirname(__file__), '..', 'guide-config.json'),
    os.path.join(os.path.dirname(__file__), 'guide-config.json'),
    os.path.join(os.getcwd(), 'guide-config.json'),
]
_cfg = {}
for _cp in _config_candidates:
    if os.path.exists(_cp):
        with open(_cp) as f:
            _cfg = json.load(f)
        break

OPENAI_API_KEY = _cfg.get('QDRANT_API', '')
OPENAI_MODEL = "text-embedding-3-small"

# ─── Globals (loaded once at startup) ────────────────────────────────────────

vc: T16OVectorClient = None

app = FastAPI(title="T16O RAG Search", version="1.0")
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ─── Startup ─────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup():
    global vc
    log.info("Using OpenAI %s (dim=%d) for query embedding", OPENAI_MODEL, VECTOR_DIM)
    log.info("Connecting to Qdrant...")
    vc = get_client()
    # Ensure oracle_findings collection exists alongside edges
    try:
        vc.init_findings_collection(recreate=False)
    except Exception as exc:
        log.warning("Failed to init oracle_findings collection: %s", exc)
    log.info("RAG search service ready")


def _embed_query(text: str) -> list[float]:
    """Embed a single query string via OpenAI API."""
    resp = http_requests.post(
        "https://api.openai.com/v1/embeddings",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
        json={"model": OPENAI_MODEL, "input": [text], "dimensions": VECTOR_DIM},
        timeout=15,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"OpenAI embedding error {resp.status_code}: {resp.text[:200]}")
    return resp.json()["data"][0]["embedding"]


# ─── Health ──────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    try:
        info = vc.collection_info()
        return {"status": "ok", "collection": info}
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=503)


# ─── Search ──────────────────────────────────────────────────────────────────

@app.get("/api/graph/rag/search")
def search(
    q: str = Query(..., description="Natural language search query"),
    limit: int = Query(20, ge=1, le=100),
    address: Optional[str] = Query(None, description="Wallet address filter"),
    token: Optional[str] = Query(None, description="Token symbol filter"),
    mint: Optional[str] = Query(None, description="Token mint address filter"),
    dex: Optional[str] = Query(None, description="DEX name filter"),
    edge_type: Optional[str] = Query(None, description="Edge type code filter"),
    source: Optional[str] = Query(None, description="Source code filter"),
    after: Optional[int] = Query(None, description="block_time >= (unix epoch)"),
    before: Optional[int] = Query(None, description="block_time <= (unix epoch)"),
    threshold: float = Query(0.25, description="Minimum similarity score"),
):
    """
    Semantic search over tx_guide edge embeddings.

    Query params match what bmap-viewer-v3.html sends.
    Returns { count, results[] } in the shape the viewer expects.
    """
    t0 = time.time()

    # Embed query via OpenAI
    query_vector = _embed_query(q)

    # Map viewer params → Qdrant filter params
    search_kwargs = dict(
        query_vector=query_vector,
        top_k=limit,
        score_threshold=threshold,
        block_time_from=after,
        block_time_to=before,
        token_symbol=token,
        token_mint=mint,
        dex=dex,
        edge_type_code=edge_type,
        source_code=source,
    )

    # Address filter: search both from_address and to_address
    # If address is provided, do two searches and merge
    if address:
        # Search from_address
        results_from = vc.search_edges(**search_kwargs, from_address=address)
        # Search to_address
        results_to = vc.search_edges(**search_kwargs, to_address=address)

        # Merge and deduplicate, keeping highest score
        seen = {}
        for r in results_from + results_to:
            rid = r["id"]
            if rid not in seen or r["score"] > seen[rid]["score"]:
                seen[rid] = r
        raw_results = sorted(seen.values(), key=lambda x: x["score"], reverse=True)[:limit]
    else:
        raw_results = vc.search_edges(**search_kwargs)

    # Transform to viewer-expected shape
    results = []
    for r in raw_results:
        p = r["payload"]
        results.append({
            "score": r["score"],
            "edge_type": p.get("edge_type_code", ""),
            "block_time_utc": str(p.get("block_time_utc", "")),
            "tx_signature": p.get("signature", ""),
            "mint_address": p.get("token_mint", ""),
            "from_address": p.get("from_address", ""),
            "to_address": p.get("to_address", ""),
            "sentence": _build_sentence(p),
            "token_symbol": p.get("token_symbol", ""),
            "dex": p.get("dex", ""),
            "amount": p.get("amount"),
            "pool_label": p.get("pool_label", ""),
        })

    elapsed = time.time() - t0
    log.info("Search: q=%r  results=%d  %.3fs", q[:60], len(results), elapsed)

    return {"count": len(results), "results": results}


def _build_sentence(p: dict) -> str:
    """Build a human-readable one-liner from payload fields."""
    parts = []

    edge = p.get("edge_type_code", "")
    direction = p.get("edge_direction", "")
    if edge:
        parts.append(edge)

    from_addr = p.get("from_address", "")
    to_addr = p.get("to_address", "")
    from_type = p.get("from_address_type", "")
    to_type = p.get("to_address_type", "")

    if from_addr:
        label = p.get("from_label") or f"{from_addr[:6]}..{from_addr[-4:]}"
        parts.append(f"from {label} [{from_type}]")

    if to_addr:
        label = p.get("to_label") or f"{to_addr[:6]}..{to_addr[-4:]}"
        parts.append(f"to {label} [{to_type}]")

    token = p.get("token_symbol", "")
    amount = p.get("amount")
    decimals = p.get("decimals")
    if token and amount is not None and decimals:
        try:
            human_amt = amount / (10 ** int(decimals))
            parts.append(f"{human_amt:,.2f} {token}")
        except (TypeError, ValueError, ZeroDivisionError):
            parts.append(f"{amount} {token} (raw)")
    elif token:
        parts.append(token)

    dex = p.get("dex", "")
    pool_label = p.get("pool_label", "")
    if dex:
        parts.append(f"on {dex}")
    if pool_label and not dex:
        parts.append(f"pool: {pool_label}")

    return " | ".join(parts) if parts else "—"


# ─── Clustering ──────────────────────────────────────────────────────────────

@app.post("/api/graph/rag/cluster")
def cluster_holders(payload: dict):
    """
    PCA → UMAP → HDBSCAN clustering for token holders.

    Input JSON:
        holders: [{address, percentage, ...}]
        edges: [{from, to, transfer_count, total_amount, ...}]

    Returns:
        holders: [...original + cluster_id, x, y]
        clusters: [{id, size, members}]
    """
    import numpy as np
    from sklearn.decomposition import PCA
    import umap
    import hdbscan

    t0 = time.time()

    holders = payload.get("holders", [])
    edges = payload.get("edges", [])

    if len(holders) < 5:
        return {"holders": holders, "clusters": [], "meta": {"error": "too few holders"}}

    n = len(holders)
    addr_to_idx = {h["address"]: i for i, h in enumerate(holders)}

    # ── Build feature matrix ──────────────────────────────────────────
    # N×N adjacency (transfer counts) + per-holder scalar features
    adj = np.zeros((n, n), dtype=np.float32)
    for e in edges:
        i = addr_to_idx.get(e.get("from"))
        j = addr_to_idx.get(e.get("to"))
        if i is not None and j is not None and i != j:
            cnt = float(e.get("transfer_count", 1))
            adj[i][j] += cnt
            adj[j][i] += cnt  # symmetric

    # Per-holder features
    pct = np.array([h.get("percentage", 0) for h in holders], dtype=np.float32)
    out_degree = adj.sum(axis=1)   # total outbound transfer weight
    in_degree = adj.sum(axis=0)    # total inbound transfer weight
    n_counterparties = (adj > 0).sum(axis=1).astype(np.float32)

    # Combine: adjacency rows + scalar features
    scalars = np.column_stack([pct, out_degree, in_degree, n_counterparties])
    features = np.hstack([adj, scalars])

    # Normalize
    from sklearn.preprocessing import StandardScaler
    features = StandardScaler().fit_transform(features)

    # ── PCA ────────────────────────────────────────────────────────────
    pca_dims = min(50, n - 1, features.shape[1])
    if pca_dims > 1:
        features_pca = PCA(n_components=pca_dims).fit_transform(features)
    else:
        features_pca = features

    # ── UMAP → 2D ─────────────────────────────────────────────────────
    n_neighbors = min(15, n - 1)
    try:
        embedding_2d = umap.UMAP(
            n_components=2,
            n_neighbors=n_neighbors,
            min_dist=0.05,       # tight clusters, small gaps
            spread=0.5,          # compact overall layout
            metric="euclidean",
            random_state=42,
        ).fit_transform(features_pca)
    except Exception as e:
        log.warning("UMAP failed: %s, falling back to PCA 2D", e)
        embedding_2d = PCA(n_components=2).fit_transform(features_pca)

    # ── HDBSCAN ───────────────────────────────────────────────────────
    min_cluster = max(2, n // 25)  # ~4% of holders per cluster
    try:
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min_cluster,
            min_samples=1,           # sensitive — find small clusters too
            cluster_selection_epsilon=0.3,  # merge nearby micro-clusters
            metric="euclidean",
        )
        labels = clusterer.fit_predict(embedding_2d)
    except Exception as e:
        log.warning("HDBSCAN failed: %s", e)
        labels = np.full(n, -1)

    # ── Scale 2D positions — tight clusters with clear separation ─────
    x_min, x_max = embedding_2d[:, 0].min(), embedding_2d[:, 0].max()
    y_min, y_max = embedding_2d[:, 1].min(), embedding_2d[:, 1].max()
    x_range = x_max - x_min if x_max > x_min else 1
    y_range = y_max - y_min if y_max > y_min else 1

    # Normalize to 0-1
    norm_x = (embedding_2d[:, 0] - x_min) / x_range
    norm_y = (embedding_2d[:, 1] - y_min) / y_range

    # Compute cluster centroids, then pull members toward their centroid
    # This tightens clusters while keeping inter-cluster separation
    cluster_centroids = {}
    for i in range(n):
        cid = int(labels[i]) if labels[i] >= 0 else -1
        if cid >= 0:
            if cid not in cluster_centroids:
                cluster_centroids[cid] = {'sx': 0, 'sy': 0, 'cnt': 0}
            cluster_centroids[cid]['sx'] += norm_x[i]
            cluster_centroids[cid]['sy'] += norm_y[i]
            cluster_centroids[cid]['cnt'] += 1

    for cid in cluster_centroids:
        c = cluster_centroids[cid]
        c['cx'] = c['sx'] / c['cnt']
        c['cy'] = c['sy'] / c['cnt']

    # Pull clustered points 60% toward their centroid
    # Pull noise points toward the nearest cluster centroid (but less aggressively)
    PULL_MEMBER = 0.6
    PULL_NOISE = 0.4

    # Global centroid as fallback
    global_cx = float(np.mean(norm_x))
    global_cy = float(np.mean(norm_y))

    for i in range(n):
        cid = int(labels[i]) if labels[i] >= 0 else -1
        if cid >= 0 and cid in cluster_centroids:
            # Pull toward own cluster
            cx = cluster_centroids[cid]['cx']
            cy = cluster_centroids[cid]['cy']
            norm_x[i] = norm_x[i] + PULL_MEMBER * (cx - norm_x[i])
            norm_y[i] = norm_y[i] + PULL_MEMBER * (cy - norm_y[i])
        elif cluster_centroids:
            # Noise point — find nearest cluster centroid and drift toward it
            best_dist = float('inf')
            best_cx, best_cy = global_cx, global_cy
            for cc in cluster_centroids.values():
                dx = norm_x[i] - cc['cx']
                dy = norm_y[i] - cc['cy']
                dist = dx * dx + dy * dy
                if dist < best_dist:
                    best_dist = dist
                    best_cx, best_cy = cc['cx'], cc['cy']
            norm_x[i] = norm_x[i] + PULL_NOISE * (best_cx - norm_x[i])
            norm_y[i] = norm_y[i] + PULL_NOISE * (best_cy - norm_y[i])
        else:
            # No clusters at all — pull toward center
            norm_x[i] = norm_x[i] + PULL_NOISE * (global_cx - norm_x[i])
            norm_y[i] = norm_y[i] + PULL_NOISE * (global_cy - norm_y[i])

    # Scale to 200–800 range (tighter)
    for i, h in enumerate(holders):
        h["x"] = float(norm_x[i] * 600 + 200)
        h["y"] = float(norm_y[i] * 600 + 200)
        h["cluster_id"] = int(labels[i]) if labels[i] >= 0 else None

    # ── Build cluster list ────────────────────────────────────────────
    cluster_map = {}
    for h in holders:
        cid = h.get("cluster_id")
        if cid is not None:
            if cid not in cluster_map:
                cluster_map[cid] = []
            cluster_map[cid].append(h["address"])

    clusters = sorted(
        [{"id": cid, "size": len(members), "members": members}
         for cid, members in cluster_map.items()],
        key=lambda c: -c["size"]
    )

    elapsed = time.time() - t0
    log.info("Cluster: %d holders → %d clusters in %.3fs", n, len(clusters), elapsed)

    return {
        "holders": holders,
        "clusters": clusters,
        "meta": {
            "holder_count": n,
            "cluster_count": len(clusters),
            "noise_count": int((labels == -1).sum()),
            "elapsed_ms": round(elapsed * 1000),
        },
    }


# ─── Oracle Findings Endpoints ────────────────────────────────────────────────

@app.post("/api/graph/rag/findings/ingest")
def findings_ingest(payload: dict):
    """
    Ingest an oracle finding into the oracle_findings vector collection.

    Input JSON:
        finding_id: int (OracleResultRecord.Id — used as point ID for idempotency)
        narrative: str (oracle output text to embed)
        oracle_name: str
        finding_type: str
        token_mint: str (optional)
        token_symbol: str (optional)
        token_name: str (optional)
        project_id: int (optional)
        tier: str (optional)
        created_at: int (unix epoch, optional)
        status: str (optional, default "completed")
    """
    t0 = time.time()

    finding_id = payload.get("finding_id")
    narrative = payload.get("narrative", "")

    if not finding_id or not narrative:
        return JSONResponse({"error": "finding_id and narrative are required"}, status_code=400)

    # Truncate narrative to 6000 chars before embedding
    narrative_truncated = narrative[:6000]

    try:
        vector = _embed_query(narrative_truncated)
    except Exception as e:
        log.error("Embedding failed for finding %s: %s", finding_id, e)
        return JSONResponse({"error": f"Embedding failed: {str(e)}"}, status_code=502)

    # Build payload — exclude finding_id and narrative (embedded)
    point_payload = {}
    for k in ("oracle_name", "finding_type", "token_mint", "token_symbol",
              "token_name", "project_id", "tier", "created_at", "status"):
        v = payload.get(k)
        if v is not None:
            point_payload[k] = v

    # Store a content preview for search results
    point_payload["content_preview"] = narrative_truncated[:500]

    ok = vc.insert_finding(int(finding_id), vector, point_payload)
    elapsed = time.time() - t0

    if ok:
        log.info("Finding ingested: id=%s oracle=%s %.3fs",
                 finding_id, payload.get("oracle_name", "?"), elapsed)
        return {"status": "ok", "finding_id": finding_id, "elapsed_ms": round(elapsed * 1000)}
    else:
        return JSONResponse({"error": "Failed to upsert finding"}, status_code=500)


@app.get("/api/graph/rag/findings/search")
def findings_search(
    q: str = Query(..., description="Natural language search query"),
    limit: int = Query(5, ge=1, le=50),
    threshold: float = Query(0.30, description="Minimum similarity score"),
    token_mint: Optional[str] = Query(None, description="Filter by token mint address"),
    token_symbol: Optional[str] = Query(None, description="Filter by token symbol"),
    oracle_name: Optional[str] = Query(None, description="Filter by oracle name"),
    finding_type: Optional[str] = Query(None, description="Filter by finding type"),
):
    """
    Semantic search over oracle finding embeddings.
    Returns scored results with content_preview.
    """
    t0 = time.time()

    try:
        query_vector = _embed_query(q)
    except Exception as e:
        return JSONResponse({"error": f"Embedding failed: {str(e)}"}, status_code=502)

    raw_results = vc.search_findings(
        query_vector=query_vector,
        top_k=limit,
        score_threshold=threshold,
        oracle_name=oracle_name,
        finding_type=finding_type,
        token_mint=token_mint,
        token_symbol=token_symbol,
    )

    results = []
    for r in raw_results:
        p = r["payload"]
        results.append({
            "score": r["score"],
            "finding_id": r["id"],
            "oracle_name": p.get("oracle_name", ""),
            "finding_type": p.get("finding_type", ""),
            "token_mint": p.get("token_mint", ""),
            "token_symbol": p.get("token_symbol", ""),
            "content_preview": p.get("content_preview", ""),
            "tier": p.get("tier", ""),
            "created_at": p.get("created_at"),
        })

    elapsed = time.time() - t0
    log.info("Findings search: q=%r results=%d %.3fs", q[:60], len(results), elapsed)

    return {"count": len(results), "results": results}


@app.get("/api/graph/rag/findings/stats")
def findings_stats():
    """Return corpus stats for oracle_findings collection."""
    try:
        total = vc.get_findings_count()
        info = vc.findings_collection_info()
        return {"total_findings": total, "collection_info": info}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=503)


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=5400)
    parser.add_argument("--host", default="0.0.0.0")
    args = parser.parse_args()

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
