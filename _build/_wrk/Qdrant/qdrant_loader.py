"""
qdrant_loader.py
─────────────────────────────────────────────────────────────────────────────
Bulk loader: MySQL tx_guide → OpenAI embeddings → Qdrant

Two worker threads each pull 100 rows, embed, and upsert on 1-second intervals.
This keeps us well under OpenAI's 1M TPM rate limit (~30K tokens/sec).

Usage:
    python3 qdrant_loader.py                         # full load from id=1
    python3 qdrant_loader.py --start 14001           # from specific ID
    python3 qdrant_loader.py --sync                  # new + updated rows since Qdrant's max updated_time
    python3 qdrant_loader.py --sync --mode template  # sync using template vectors (99% fewer OpenAI calls)

Author : theGuide / T16O
─────────────────────────────────────────────────────────────────────────────
"""

import argparse
import hashlib
import json
import logging
import os
import threading
import time
import sys

import pymysql
import requests

from qdrant_client_t16o import get_client, VECTOR_DIM, COL_GUIDE_EDGES, COL_GUIDE_EDGES_TEMPLATES

# ─── Logging ─────────────────────────────────────────────────────────────────

log = logging.getLogger("t16o.loader")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  [%(threadName)s]  %(message)s",
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

MYSQL_HOST      = "34.196.15.116"
MYSQL_PORT      = 3397
MYSQL_USER      = "root"
MYSQL_PASSWORD  = "rootpassword"
MYSQL_DB        = "t16o_db"

OPENAI_API_KEY  = _cfg.get('QDRANT_API', '')
OPENAI_MODEL    = "text-embedding-3-small"
OPENAI_DIM      = VECTOR_DIM   # 384

# Defaults — overridden by DB config table at startup
BATCH_SIZE      = 1000      # rows per worker per tick
TICK_INTERVAL   = 1.0       # seconds between ticks
NUM_WORKERS     = 2         # worker threads
QDRANT_BATCH    = 100       # points per Qdrant upsert call


# ─── Shared State ────────────────────────────────────────────────────────────

_cursor_lock = threading.Lock()
_stats_lock = threading.Lock()
_next_cursor = 0
_end_id = 0
_total_inserted = 0
_total_skipped = 0
_t_start = 0.0

# Dense batch mode: pre-fetched list of real guide_ids
_dense_ids: list[int] = []
_dense_cursor = 0
_dense_mode = False


def claim_batch():
    """Thread-safe: claim the next batch of guide_ids. Returns list of IDs or None if done.

    In dense mode, returns exactly BATCH_SIZE real IDs (no gaps).
    """
    global _dense_cursor
    with _cursor_lock:
        if _dense_cursor >= len(_dense_ids):
            return None
        batch_start = _dense_cursor
        batch_end = min(_dense_cursor + BATCH_SIZE, len(_dense_ids))
        _dense_cursor = batch_end
        return _dense_ids[batch_start:batch_end]


def record_stats(inserted: int, skipped: int):
    global _total_inserted, _total_skipped
    with _stats_lock:
        _total_inserted += inserted
        _total_skipped += skipped


def get_stats() -> tuple[int, int]:
    with _stats_lock:
        return _total_inserted, _total_skipped


# ─── OpenAI Embedding ───────────────────────────────────────────────────────

def embed_texts(texts: list[str]) -> list[list[float]]:
    """Embed texts via OpenAI API with retry on rate limit."""
    if not texts:
        return []
    for attempt in range(5):
        resp = requests.post(
            "https://api.openai.com/v1/embeddings",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": OPENAI_MODEL,
                "input": texts,
                "dimensions": OPENAI_DIM,
            },
            timeout=60,
        )
        if resp.status_code == 429:
            wait = min(2 ** attempt * 2, 30)
            log.warning("Rate limited, waiting %ds...", wait)
            time.sleep(wait)
            continue
        if resp.status_code != 200:
            raise RuntimeError(f"OpenAI API error {resp.status_code}: {resp.text[:200]}")
        data = resp.json()
        embeddings = sorted(data["data"], key=lambda x: x["index"])
        return [e["embedding"] for e in embeddings]
    raise RuntimeError("OpenAI API rate limit exceeded after 5 retries")


# ─── MySQL helpers ───────────────────────────────────────────────────────────

def get_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=30,
        read_timeout=120,
    )


def load_loader_config(conn):
    """Load loader settings from DB config table. Overrides module-level defaults."""
    global BATCH_SIZE, TICK_INTERVAL, NUM_WORKERS
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT config_key, config_value FROM config "
                "WHERE config_type = 'batch' AND config_key LIKE 'qdrant\\_loader\\_%' ESCAPE '\\\\'")
            for row in cur.fetchall():
                k, v = row["config_key"], row["config_value"]
                if k == "qdrant_loader_batch_size":
                    BATCH_SIZE = int(v)
                elif k == "qdrant_loader_tick_interval":
                    TICK_INTERVAL = float(v)
                elif k == "qdrant_loader_workers":
                    NUM_WORKERS = int(v)
        log.info("Config loaded: batch_size=%d, tick_interval=%.1f, workers=%d",
                 BATCH_SIZE, TICK_INTERVAL, NUM_WORKERS)
    except Exception as e:
        log.warning("Could not load DB config, using defaults: %s", e)


def get_id_range(conn) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM tx_guide")
        row = cur.fetchone()
    return int(row["min_id"]), int(row["max_id"])


# ─── Template Mode ──────────────────────────────────────────────────────────

# Shared template cache: pattern_hash → embedding vector (list[float])
_template_cache: dict[str, list[float]] = {}
_template_mode = False
_target_collection = COL_GUIDE_EDGES
_template_stats_lock = threading.Lock()
_template_hits = 0
_template_misses = 0


def compute_balance_signal(row: dict) -> str:
    """Classify the financial significance of an edge from its balance fields.

    Must match the CASE logic in sp_tx_guide_template_discover.
    """
    from_pre = row.get("from_token_pre_balance")
    from_post = row.get("from_token_post_balance")
    to_pre = row.get("to_token_pre_balance")
    amount = row.get("amount")

    # Convert string balances to numeric
    try:
        from_pre = int(from_pre) if from_pre is not None else None
    except (ValueError, TypeError):
        from_pre = None
    try:
        from_post = int(from_post) if from_post is not None else None
    except (ValueError, TypeError):
        from_post = None
    try:
        to_pre = int(to_pre) if to_pre is not None else None
    except (ValueError, TypeError):
        to_pre = None
    try:
        amount = int(amount) if amount is not None else None
    except (ValueError, TypeError):
        amount = None

    if from_post is not None and from_post == 0 and from_pre is not None and from_pre > 0:
        return "drained"
    if to_pre is not None and to_pre == 0 and amount is not None and amount > 0:
        return "funded"
    if amount is not None and amount < 10000:
        return "dust"
    if (amount is not None and from_pre is not None
            and from_pre > 0 and amount > from_pre * 0.9):
        return "large"
    return "normal"


def compute_amount_magnitude(row: dict) -> str:
    """Classify the magnitude of the transfer amount."""
    amount = row.get("amount")
    try:
        amount = int(amount) if amount is not None else None
    except (ValueError, TypeError):
        return "medium"

    if amount is None:
        return "medium"
    if amount < 1_000_000:
        return "micro"
    if amount < 1_000_000_000:
        return "small"
    if amount < 1_000_000_000_000:
        return "medium"
    return "whale"


def compute_label_signal(row: dict) -> str:
    """Classify address label context from from_label / to_label."""
    labels = []
    fl = row.get("from_label")
    tl = row.get("to_label")
    if fl:
        labels.append(fl.lower())
    if tl:
        labels.append(tl.lower())

    if not labels:
        return "unlabeled"

    combined = " ".join(labels)

    if "burn" in combined:
        return "burn_labeled"
    if "authority" in combined:
        return "authority"

    import re
    if re.search(r'raydium|orca|jupiter|meteora|pump\.fun|openbook', combined):
        return "dex_labeled"
    if re.search(r'binance|coinbase|kraken|okx|bybit|ftx|kucoin', combined):
        return "cex_labeled"

    return "labeled"


def compute_pattern_hash(row: dict) -> str:
    """Compute the pattern hash for a tx_guide SP result row.

    Must match the SHA2(CONCAT_WS('|', ...)) used by sp_tx_guide_template_discover.
    11 dimensions: 8 structural + balance_signal + amount_magnitude + label_signal.
    """
    parts = [
        row.get("edge_type_code", ""),
        row.get("edge_category", ""),
        row.get("edge_direction") or "neutral",
        row.get("from_address_type") or "unknown",
        row.get("to_address_type") or "unknown",
        "1" if row.get("pool_address") else "0",
        "1" if row.get("tax_bps") is not None else "0",
        row.get("swap_direction") or "none",
        compute_balance_signal(row),
        compute_amount_magnitude(row),
        compute_label_signal(row),
    ]
    key = "|".join(parts)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def load_template_cache(conn) -> int:
    """Load all templates with embeddings from tx_guide_template into memory.

    Returns the number of templates loaded.
    """
    global _template_cache
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pattern_hash, embedding FROM tx_guide_template "
            "WHERE use_template = 1 AND embedding IS NOT NULL"
        )
        rows = cur.fetchall()

    loaded = 0
    for r in rows:
        ph = r["pattern_hash"]
        emb = r["embedding"]
        if emb:
            if isinstance(emb, str):
                emb = json.loads(emb)
            _template_cache[ph] = emb
            loaded += 1

    log.info("Template cache loaded: %d templates with embeddings", loaded)
    return loaded


def record_template_stats(hits: int, misses: int):
    global _template_hits, _template_misses
    with _template_stats_lock:
        _template_hits += hits
        _template_misses += misses


def get_template_stats() -> tuple[int, int]:
    with _template_stats_lock:
        return _template_hits, _template_misses


def get_sync_watermark(vc) -> int:
    """Get the sync watermark from Qdrant: MIN(max_created_time, max_updated_time).

    Using the minimum of both ensures coverage when:
    - A re-enriched record has a newer updated_time than unloaded new records
    - The loader crashed mid-stream leaving gaps in created_time

    Returns 0 if the collection is empty.

    Qdrant queries:
        scroll(order_by=created_time desc, limit=1)
        scroll(order_by=updated_time desc, limit=1)
    """
    from qdrant_client.models import OrderBy
    col = _target_collection
    try:
        info = vc.collection_info(collection_name=col)
        count = info.get("points_count", 0)
        if count == 0:
            return 0

        max_created = 0
        max_updated = 0

        # Max created_time
        try:
            results, _ = vc.client.scroll(
                collection_name=col,
                limit=1,
                order_by=OrderBy(key="created_time", direction="desc"),
                with_payload=["created_time"],
                with_vectors=False,
            )
            if results and results[0].payload:
                max_created = int(results[0].payload.get("created_time", 0))
        except Exception as e:
            log.warning("created_time query failed: %s", e)

        # Max updated_time
        try:
            results, _ = vc.client.scroll(
                collection_name=col,
                limit=1,
                order_by=OrderBy(key="updated_time", direction="desc"),
                with_payload=["updated_time"],
                with_vectors=False,
            )
            if results and results[0].payload:
                max_updated = int(results[0].payload.get("updated_time", 0))
        except Exception as e:
            log.warning("updated_time query failed: %s", e)

        # Use the earlier of the two as the watermark
        if max_created > 0 and max_updated > 0:
            watermark = min(max_created, max_updated)
        else:
            watermark = max(max_created, max_updated)  # use whichever is available

        log.info("Sync watermark=%d (max_created=%d, max_updated=%d, %d total points)",
                 watermark, max_created, max_updated, count)
        return watermark

    except Exception as e:
        log.warning("Could not determine sync watermark: %s", e)
        return 0


def get_sync_ids(conn, since_epoch: int) -> list[int]:
    """Get guide_ids from MySQL where created_utc or updated_utc > since_epoch.

    Returns sorted list of guide_ids that need (re-)loading into Qdrant.
    Covers both new rows and re-enriched rows.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM tx_guide "
            "WHERE UNIX_TIMESTAMP(updated_utc) > %s "
            "   OR UNIX_TIMESTAMP(created_utc) > %s "
            "ORDER BY id ASC",
            (since_epoch, since_epoch)
        )
        rows = cur.fetchall()
    return [int(r["id"]) for r in rows]


def fetch_narrative_batch(conn, start_id: int, end_id: int) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("CALL sp_get_guide_edge_narratives(%s, %s)", (start_id, end_id))
        rows = cur.fetchall()
    conn.commit()
    return rows


# ─── Worker Thread ───────────────────────────────────────────────────────────

def worker_loop(worker_id: int, vc):
    """Each worker: claim batch → fetch → embed (or template) → upsert, then sleep."""
    conn = get_connection()
    log.info("Worker %d started (template_mode=%s)", worker_id, _template_mode)

    while True:
        tick_start = time.time()

        batch_ids = claim_batch()
        if batch_ids is None:
            log.info("Worker %d: no more batches, exiting", worker_id)
            break

        start = batch_ids[0]
        end = batch_ids[-1]

        try:
            # 1. Fetch from MySQL in tight contiguous sub-ranges
            #    Dense IDs may span a wide range — break into sub-ranges
            #    where each sub-range covers a contiguous block of claimed IDs.
            rows = []
            sub_start = batch_ids[0]
            for i in range(1, len(batch_ids)):
                # If there's a gap > 100 between consecutive IDs, flush the sub-range
                if batch_ids[i] - batch_ids[i-1] > 100:
                    sub_rows = fetch_narrative_batch(conn, sub_start, batch_ids[i-1])
                    if sub_rows:
                        rows.extend(sub_rows)
                    sub_start = batch_ids[i]
            # Flush final sub-range
            sub_rows = fetch_narrative_batch(conn, sub_start, batch_ids[-1])
            if sub_rows:
                rows.extend(sub_rows)

            if not rows:
                record_stats(0, 0)
                elapsed = time.time() - tick_start
                if elapsed < TICK_INTERVAL:
                    time.sleep(TICK_INTERVAL - elapsed)
                continue

            # 2. Split into template-hit vs embed-needed
            valid_rows = []
            skipped = 0
            for r in rows:
                n = r.get("narrative")
                if not n:
                    skipped += 1
                    continue
                valid_rows.append(r)

            if not valid_rows:
                record_stats(0, skipped)
                elapsed = time.time() - tick_start
                if elapsed < TICK_INTERVAL:
                    time.sleep(TICK_INTERVAL - elapsed)
                continue

            if _template_mode:
                # Split rows into template hits and misses
                hit_rows = []
                hit_embeddings = []
                miss_rows = []
                miss_narratives = []

                for r in valid_rows:
                    ph = compute_pattern_hash(r)
                    cached = _template_cache.get(ph)
                    if cached:
                        hit_rows.append(r)
                        hit_embeddings.append(cached)
                    else:
                        miss_rows.append(r)
                        miss_narratives.append(r["narrative"])

                # Embed only the misses via OpenAI
                if miss_narratives:
                    miss_embeddings = embed_texts(miss_narratives)
                else:
                    miss_embeddings = []

                # Combine hits + misses
                all_rows = hit_rows + miss_rows
                all_embeddings = hit_embeddings + miss_embeddings

                record_template_stats(len(hit_rows), len(miss_rows))
            else:
                # Full embed mode — all rows go through OpenAI
                narratives = [r["narrative"] for r in valid_rows]
                all_embeddings = embed_texts(narratives)
                all_rows = valid_rows

            # 3. Upsert to Qdrant
            n_inserted = vc.insert_edges(all_rows, all_embeddings,
                                         batch_size=QDRANT_BATCH,
                                         collection_name=_target_collection)

            record_stats(n_inserted, skipped)
            total, _ = get_stats()
            elapsed_total = time.time() - _t_start
            rate = total / elapsed_total if elapsed_total > 0 else 0

            if _template_mode:
                t_hits, t_misses = get_template_stats()
                log.info(
                    "W%d batch %d–%d: %d rows | total=%d (%.0f/sec) | tmpl=%d/%d (%.0f%%)",
                    worker_id, start, end, n_inserted, total, rate,
                    t_hits, t_hits + t_misses,
                    (t_hits / (t_hits + t_misses) * 100) if (t_hits + t_misses) > 0 else 0,
                )
            else:
                log.info(
                    "W%d batch %d–%d: %d rows | total=%d (%.0f/sec)",
                    worker_id, start, end, n_inserted, total, rate,
                )

        except Exception as e:
            log.error("Worker %d error on batch %d–%d: %s", worker_id, start, end, e)

        # Sleep remainder of tick interval
        elapsed = time.time() - tick_start
        if elapsed < TICK_INTERVAL:
            time.sleep(TICK_INTERVAL - elapsed)

    conn.close()
    log.info("Worker %d finished", worker_id)


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    global _next_cursor, _end_id, _t_start, _template_mode, _target_collection
    global _dense_ids, _dense_cursor, _dense_mode

    parser = argparse.ArgumentParser(description="Load tx_guide edges into Qdrant")
    parser.add_argument("--start", type=int, default=None, help="Start guide_id")
    parser.add_argument("--end", type=int, default=None, help="End guide_id")
    parser.add_argument("--sync", action="store_true",
                        help="Sync mode: load new + re-enriched rows since Qdrant's max updated_time")
    parser.add_argument("--mode", choices=["full", "template"], default="template",
                        help="Embedding mode: 'template' = cached vectors for known patterns (default), "
                             "'full' = OpenAI per row")
    parser.add_argument("--collection", default=None,
                        help="Target Qdrant collection (default: auto-selected by mode)")
    parser.add_argument("--workers", type=int, default=NUM_WORKERS, help="Number of worker threads")
    args = parser.parse_args()

    _template_mode = (args.mode == "template")

    # Auto-select collection by mode, or use explicit override
    if args.collection:
        _target_collection = args.collection
    elif _template_mode:
        _target_collection = COL_GUIDE_EDGES_TEMPLATES
    else:
        _target_collection = COL_GUIDE_EDGES

    if not OPENAI_API_KEY and not _template_mode:
        log.error("QDRANT_API key not found in guide-config.json")
        sys.exit(1)

    log.info("Using OpenAI %s (dim=%d), %d workers, %d rows/tick, %.1fs interval, mode=%s, collection=%s",
             OPENAI_MODEL, OPENAI_DIM, args.workers, BATCH_SIZE, TICK_INTERVAL,
             args.mode, _target_collection)

    log.info("Connecting to Qdrant...")
    vc = get_client()
    vc.init_collection(collection_name=_target_collection)
    log.info("Collection ready: %s", _target_collection)

    log.info("Connecting to MySQL %s:%d/%s...", MYSQL_HOST, MYSQL_PORT, MYSQL_DB)
    conn = get_connection()
    load_loader_config(conn)

    # CLI --workers overrides DB config if explicitly provided
    if args.workers != NUM_WORKERS and args.workers != 2:  # 2 is the old default
        pass  # use CLI value
    else:
        args.workers = NUM_WORKERS

    # Load template cache if in template mode
    if _template_mode:
        n_templates = load_template_cache(conn)
        if n_templates == 0:
            log.warning("No templates with embeddings found — falling back to full mode. "
                        "Run task #4 (generate template narratives) first.")
            _template_mode = False

    if args.sync:
        # ── Sync mode: find rows newer than MIN(max_created, max_updated) ──
        watermark = get_sync_watermark(vc)
        real_ids = get_sync_ids(conn, watermark)

        if not real_ids:
            log.info("Nothing to sync — Qdrant is up to date (watermark=%d)", watermark)
            conn.close()
            return

        log.info("Sync: %d rows to process (created/updated > %d)", len(real_ids), watermark)
    else:
        # ── Full/range load mode ──
        db_min, db_max = get_id_range(conn)
        log.info("tx_guide ID range: %d → %d", db_min, db_max)
        s = args.start or db_min
        e = args.end or db_max

        # Pre-fetch real IDs for dense batching (no wasted ticks on gaps)
        log.info("Fetching real guide_ids for dense batching...")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM tx_guide WHERE id BETWEEN %s AND %s ORDER BY id",
                (s, e)
            )
            real_ids = [int(r["id"]) for r in cur.fetchall()]
        log.info("Found %d real rows in ID range %d → %d (density: %.0f%%)",
                 len(real_ids), s, e, len(real_ids) / max(e - s + 1, 1) * 100)

    if not real_ids:
        log.info("Nothing to load")
        conn.close()
        return

    # Enable dense batch mode — workers claim from real ID list, no gaps
    _dense_ids = real_ids
    _dense_cursor = 0
    _dense_mode = True

    _t_start = time.time()
    log.info("Loading %d rows (guide_id %d → %d) in dense batch mode",
             len(real_ids), real_ids[0], real_ids[-1])

    conn.close()  # main conn not needed — workers create their own

    # Launch worker threads
    threads = []
    for i in range(args.workers):
        t = threading.Thread(target=worker_loop, args=(i, vc), name=f"W{i}")
        t.start()
        threads.append(t)
        time.sleep(0.5)  # stagger starts so ticks don't overlap

    for t in threads:
        t.join()

    # Summary
    total, skipped = get_stats()
    elapsed = time.time() - _t_start
    log.info(
        "Load complete: %d inserted, %d skipped | %.1f sec (%.0f rows/sec)",
        total, skipped, elapsed,
        total / elapsed if elapsed > 0 else 0,
    )

    if _template_mode:
        t_hits, t_misses = get_template_stats()
        t_total = t_hits + t_misses
        log.info(
            "Template stats: %d hits, %d misses (%.1f%% hit rate, %d OpenAI calls saved)",
            t_hits, t_misses,
            (t_hits / t_total * 100) if t_total > 0 else 0,
            t_hits,
        )

    info = vc.collection_info(collection_name=_target_collection)
    log.info("Collection: %s", info)


if __name__ == "__main__":
    main()
