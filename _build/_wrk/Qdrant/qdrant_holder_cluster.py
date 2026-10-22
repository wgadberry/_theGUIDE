"""
qdrant_holder_cluster.py
─────────────────────────────────────────────────────────────────────────────
Behavioral clustering of top token holders using Qdrant edge vectors.

For each holder, fetches their tx_guide edge vectors from Qdrant, computes
an average "profile vector", then clusters holders by cosine similarity.
Holders with similar transaction patterns cluster together — even if they
never transacted directly.

Usage:
    python3 qdrant_holder_cluster.py <mint_address>
    python3 qdrant_holder_cluster.py <mint_address> --holders 40
    python3 qdrant_holder_cluster.py <mint_address> --holders 80 --min-edges 5

Author : theGuide / T16O
─────────────────────────────────────────────────────────────────────────────
"""

import argparse
import json
import logging
import os
import sys
import time

import numpy as np
import pymysql

from qdrant_client_t16o import get_client, COL_GUIDE_EDGES, VECTOR_DIM
from qdrant_client.models import Filter, FieldCondition, MatchValue, MatchAny

# ─── Logging ─────────────────────────────────────────────────────────────────

log = logging.getLogger("t16o.holder_cluster")
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

MYSQL_HOST     = _cfg.get('DB_HOST', '34.196.15.116')
MYSQL_PORT     = _cfg.get('DB_PORT', 3397)
MYSQL_USER     = _cfg.get('DB_USER', 'root')
MYSQL_PASSWORD = _cfg.get('DB_PASSWORD', 'rootpassword')
MYSQL_DB       = _cfg.get('DB_NAME', 't16o_db')


def get_connection():
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT,
        user=MYSQL_USER, password=MYSQL_PASSWORD,
        database=MYSQL_DB, charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=30, read_timeout=120,
    )


# ─── Step 1: Get top holders ────────────────────────────────────────────────

def get_top_holders(conn, mint: str, max_holders: int) -> list:
    """Get top holders from tx_token_holder with their wallet addresses."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT h.rank, a.address, h.amount, h.decimals, h.percentage
            FROM tx_token_holder h
            JOIN tx_address a ON a.id = h.owner_address_id
            JOIN tx_token tk ON tk.id = h.token_id
            JOIN tx_address mint ON mint.id = tk.mint_address_id
            WHERE mint.address = %s
            ORDER BY h.rank
            LIMIT %s
        """, (mint, max_holders))
        return cur.fetchall()


# ─── Step 2: Fetch edge vectors per holder from Qdrant ──────────────────────

def fetch_holder_vectors(vc, mint: str, address: str, max_edges: int = 200):
    """Fetch edge vectors for a single holder address from Qdrant.

    Returns list of vectors (as numpy arrays) for edges where this address
    is either from_address or to_address for the given token.
    """
    col = COL_GUIDE_EDGES
    vectors = []

    # Search as from_address
    try:
        results, _ = vc.client.scroll(
            collection_name=col,
            scroll_filter=Filter(must=[
                FieldCondition(key="token_mint", match=MatchValue(value=mint)),
                FieldCondition(key="from_address", match=MatchValue(value=address)),
            ]),
            limit=max_edges // 2,
            with_payload=["edge_type_code", "edge_category"],
            with_vectors=True,
        )
        for r in results:
            vectors.append(np.array(r.vector))
    except Exception as e:
        log.warning("from_address scroll failed for %s: %s", address[:8], e)

    # Search as to_address
    try:
        results, _ = vc.client.scroll(
            collection_name=col,
            scroll_filter=Filter(must=[
                FieldCondition(key="token_mint", match=MatchValue(value=mint)),
                FieldCondition(key="to_address", match=MatchValue(value=address)),
            ]),
            limit=max_edges // 2,
            with_payload=["edge_type_code", "edge_category"],
            with_vectors=True,
        )
        for r in results:
            vectors.append(np.array(r.vector))
    except Exception as e:
        log.warning("to_address scroll failed for %s: %s", address[:8], e)

    return vectors


def compute_profile_vector(vectors):
    """Average a holder's edge vectors into a single profile vector."""
    if not vectors:
        return None
    stacked = np.stack(vectors)
    profile = np.mean(stacked, axis=0)
    # L2 normalize for cosine similarity
    norm = np.linalg.norm(profile)
    if norm > 0:
        profile = profile / norm
    return profile


# ─── Step 3: Cluster profile vectors ────────────────────────────────────────

def cluster_holders(profiles, method="agglomerative", n_clusters=None):
    """Cluster holder profile vectors.

    Args:
        profiles: list of (holder_info, profile_vector) tuples
        method: "agglomerative" or "kmeans"
        n_clusters: number of clusters (auto-detected if None)

    Returns: list of (holder_info, cluster_id) tuples
    """
    from sklearn.cluster import AgglomerativeClustering, KMeans
    from sklearn.metrics import silhouette_score

    vectors = np.stack([p[1] for p in profiles])
    n = len(vectors)

    if n < 3:
        return [(p[0], 0) for p in profiles]

    if n_clusters is None:
        # Auto-detect optimal cluster count using silhouette score
        best_k = 2
        best_score = -1
        max_k = min(n - 1, 15)

        for k in range(2, max_k + 1):
            if method == "agglomerative":
                labels = AgglomerativeClustering(
                    n_clusters=k, metric="cosine", linkage="average"
                ).fit_predict(vectors)
            else:
                labels = KMeans(n_clusters=k, random_state=42, n_init=10).fit_predict(vectors)

            score = silhouette_score(vectors, labels, metric="cosine")
            if score > best_score:
                best_score = score
                best_k = k

        n_clusters = best_k
        log.info("Auto-detected %d clusters (silhouette=%.3f)", n_clusters, best_score)

    # Final clustering
    if method == "agglomerative":
        labels = AgglomerativeClustering(
            n_clusters=n_clusters, metric="cosine", linkage="average"
        ).fit_predict(vectors)
    else:
        labels = KMeans(n_clusters=n_clusters, random_state=42, n_init=10).fit_predict(vectors)

    return [(profiles[i][0], int(labels[i])) for i in range(n)]


# ─── Step 4: Compute inter-holder similarity matrix ─────────────────────────

def compute_similarity_matrix(profiles):
    """Compute cosine similarity between all holder profile vectors."""
    vectors = np.stack([p[1] for p in profiles])
    # Cosine similarity = dot product of normalized vectors
    similarity = vectors @ vectors.T
    return similarity


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Cluster token holders by behavioral vectors")
    parser.add_argument("mint", help="Token mint address")
    parser.add_argument("--holders", type=int, default=40, help="Number of top holders (default 40)")
    parser.add_argument("--min-edges", type=int, default=3, help="Minimum edges per holder to include (default 3)")
    parser.add_argument("--max-edges", type=int, default=200, help="Max edges to fetch per holder (default 200)")
    parser.add_argument("--clusters", type=int, default=None, help="Force cluster count (auto if omitted)")
    parser.add_argument("--json", action="store_true", help="Output as JSON instead of table")
    args = parser.parse_args()

    t0 = time.time()

    # Connect
    log.info("Connecting to Qdrant...")
    vc = get_client()

    log.info("Connecting to MySQL...")
    conn = get_connection()

    # Step 1: Get holders
    holders = get_top_holders(conn, args.mint, args.holders)
    if not holders:
        log.error("No holders found for mint %s. Run holder fetch first.", args.mint)
        conn.close()
        sys.exit(1)
    log.info("Found %d holders for %s", len(holders), args.mint[:12])

    # Step 2: Fetch vectors and compute profiles
    profiles = []
    skipped = 0
    for h in holders:
        addr = h["address"]
        vectors = fetch_holder_vectors(vc, args.mint, addr, max_edges=args.max_edges)

        if len(vectors) < args.min_edges:
            skipped += 1
            continue

        profile = compute_profile_vector(vectors)
        if profile is not None:
            profiles.append((h, profile))
            log.info("  #%d %s…: %d edges → profile vector",
                     h["rank"], addr[:8], len(vectors))

    log.info("%d holders with profiles (%d skipped, < %d edges)",
             len(profiles), skipped, args.min_edges)

    if len(profiles) < 2:
        log.error("Need at least 2 holders with sufficient edges to cluster")
        conn.close()
        sys.exit(1)

    # Step 3: Cluster
    log.info("Clustering %d holder profiles...", len(profiles))
    clustered = cluster_holders(profiles, n_clusters=args.clusters)

    # Step 4: Similarity matrix
    similarity = compute_similarity_matrix(profiles)

    # Group by cluster
    clusters = {}
    for holder_info, cluster_id in clustered:
        if cluster_id not in clusters:
            clusters[cluster_id] = []
        clusters[cluster_id].append(holder_info)

    elapsed = time.time() - t0

    # Output
    if args.json:
        result = {
            "mint": args.mint,
            "holders_analyzed": len(profiles),
            "holders_skipped": skipped,
            "cluster_count": len(clusters),
            "elapsed_ms": round(elapsed * 1000),
            "clusters": []
        }
        for cid in sorted(clusters.keys()):
            members = clusters[cid]
            cluster_data = {
                "cluster_id": cid,
                "size": len(members),
                "total_percentage": round(sum(float(m["percentage"] or 0) for m in members), 2),
                "members": []
            }
            for m in members:
                cluster_data["members"].append({
                    "rank": m["rank"],
                    "address": m["address"],
                    "percentage": float(m["percentage"]) if m["percentage"] else 0,
                })
            result["clusters"].append(cluster_data)

        # Add top similarities between clusters
        cross_cluster_sims = []
        for i, (h1, c1) in enumerate(clustered):
            for j, (h2, c2) in enumerate(clustered):
                if i < j and c1 != c2:
                    sim = float(similarity[i][j])
                    if sim > 0.85:
                        cross_cluster_sims.append({
                            "holder_a": h1["address"],
                            "holder_b": h2["address"],
                            "cluster_a": c1,
                            "cluster_b": c2,
                            "similarity": round(sim, 4),
                        })
        cross_cluster_sims.sort(key=lambda x: x["similarity"], reverse=True)
        result["cross_cluster_links"] = cross_cluster_sims[:20]

        print(json.dumps(result, indent=2))
    else:
        print(f"\n{'='*70}")
        print(f"Holder Behavioral Clusters — {args.mint[:16]}…")
        print(f"{'='*70}")
        print(f"Holders analyzed: {len(profiles)} | Skipped: {skipped} | "
              f"Clusters: {len(clusters)} | {elapsed:.1f}s\n")

        for cid in sorted(clusters.keys()):
            members = clusters[cid]
            total_pct = sum(float(m["percentage"] or 0) for m in members)
            print(f"  Cluster {cid} ({len(members)} holders, {total_pct:.2f}% supply)")
            print(f"  {'─'*50}")
            for m in members:
                pct = float(m["percentage"]) if m["percentage"] else 0
                print(f"    #{m['rank']:>3}  {m['address'][:12]}…  {pct:>6.2f}%")
            print()

        # Show top intra-cluster similarity pairs
        print(f"  Top cross-cluster similarities (potential links):")
        print(f"  {'─'*50}")
        shown = 0
        for i, (h1, c1) in enumerate(clustered):
            for j, (h2, c2) in enumerate(clustered):
                if i < j and c1 != c2:
                    sim = similarity[i][j]
                    if sim > 0.85 and shown < 10:
                        print(f"    {h1['address'][:12]}… (C{c1}) ↔ "
                              f"{h2['address'][:12]}… (C{c2})  sim={sim:.4f}")
                        shown += 1

    conn.close()


if __name__ == "__main__":
    main()
