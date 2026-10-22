"""
qdrant_client_t16o.py
─────────────────────────────────────────────────────────────────────────────
T16O Vector Database Client — Qdrant wrapper for theGuide / Graph RAG layer.

Primary collection:
    t16o_guide_edges  — one vector per tx_guide edge, narrative built by
                        sp_get_guide_edge_narratives, embedded externally,
                        then inserted here with full payload.

Deployment target:
    Dedicated t3.xlarge (16 GB RAM / 50 GiB EBS / 4 vCPU)
    Docker — memory capped at 14 GB, ports bound to 127.0.0.1 only.

Workflow:
    1. MySQL: CALL sp_get_guide_edge_narratives(start_id, end_id)
    2. Caller embeds each narrative string (OpenAI / local model)
    3. T16OVectorClient.insert_edges(rows, embeddings)
    4. Search via search_edges() with optional filters

Filtering:
    block_time       (int)  — fast integer range filter (indexed)
    block_time_utc   (str)  — human-readable payload field (display only)
    All other payload fields indexed for keyword / range filtering.

Author : theGuide / T16O
─────────────────────────────────────────────────────────────────────────────
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.http.exceptions import UnexpectedResponse
from qdrant_client.models import (
    Distance,
    VectorParams,
    ScalarQuantization,
    ScalarQuantizationConfig,
    ScalarType,
    HnswConfigDiff,
    OptimizersConfigDiff,
    PointStruct,
    Filter,
    FieldCondition,
    MatchValue,
    MatchAny,
    Range,
    SnapshotDescription,
)

# ─── Logging ─────────────────────────────────────────────────────────────────

log = logging.getLogger("t16o.qdrant")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ─── Constants ────────────────────────────────────────────────────────────────

QDRANT_HOST         = "localhost"
QDRANT_HTTP_PORT    = 6333
QDRANT_GRPC_PORT    = 6334

VECTOR_DIM          = 384           # all-MiniLM-L6-v2 (local, free)
DISTANCE_METRIC     = Distance.COSINE

DEFAULT_BATCH_SIZE  = 500
DEFAULT_TOP_K       = 10

# Collections
COL_GUIDE_EDGES          = "t16o_rag"
COL_GUIDE_EDGES_TEMPLATES = "t16o_rag"  # same — template is the default now
COL_ORACLE_FINDINGS      = "oracle_findings"

# HNSW — dedicated t3.xlarge (16 GB RAM, 4 vCPU)
HNSW_M              = 16
HNSW_EF_CONSTRUCT   = 128
HNSW_ON_DISK        = False         # graph stays in RAM

# Optimizer
INDEXING_THRESHOLD  = 50_000
MEMMAP_THRESHOLD    = 100_000

# ─── Payload field registry ───────────────────────────────────────────────────
# Maps each payload field to its Qdrant schema type.
# All fields sourced directly from sp_get_guide_edge_narratives output columns.

PAYLOAD_INDEXES: list[tuple[str, models.PayloadSchemaType]] = [

    # ── Time ──────────────────────────────────────────────────────────
    # block_time: primary range filter — INTEGER for fast range queries
    ("block_time",              models.PayloadSchemaType.INTEGER),

    # ── Transaction ───────────────────────────────────────────────────
    ("guide_id",                models.PayloadSchemaType.INTEGER),
    ("signature",               models.PayloadSchemaType.KEYWORD),

    # ── Edge classification ───────────────────────────────────────────
    ("edge_type_code",          models.PayloadSchemaType.KEYWORD),
    ("edge_category",           models.PayloadSchemaType.KEYWORD),
    ("edge_direction",          models.PayloadSchemaType.KEYWORD),
    ("risk_weight",             models.PayloadSchemaType.INTEGER),
    ("swap_direction",          models.PayloadSchemaType.KEYWORD),

    # ── From wallet ───────────────────────────────────────────────────
    ("from_address",            models.PayloadSchemaType.KEYWORD),
    ("from_address_type",       models.PayloadSchemaType.KEYWORD),
    ("funded_by",          models.PayloadSchemaType.KEYWORD),

    # ── To wallet ─────────────────────────────────────────────────────
    ("to_address",              models.PayloadSchemaType.KEYWORD),
    ("to_address_type",         models.PayloadSchemaType.KEYWORD),

    # ── Token ─────────────────────────────────────────────────────────
    ("token_symbol",            models.PayloadSchemaType.KEYWORD),
    ("token_mint",              models.PayloadSchemaType.KEYWORD),
    ("token_type",              models.PayloadSchemaType.KEYWORD),

    # ── Amounts ───────────────────────────────────────────────────────
    ("amount",                  models.PayloadSchemaType.INTEGER),

    # ── Tax ───────────────────────────────────────────────────────────
    ("tax_bps",                 models.PayloadSchemaType.INTEGER),

    # ── Fees ──────────────────────────────────────────────────────────
    ("fee",                     models.PayloadSchemaType.INTEGER),
    ("priority_fee",            models.PayloadSchemaType.INTEGER),

    # ── Pool ──────────────────────────────────────────────────────────
    ("pool_address",            models.PayloadSchemaType.KEYWORD),
    ("dex",                     models.PayloadSchemaType.KEYWORD),
    ("token_pair",              models.PayloadSchemaType.KEYWORD),

    # ── Program ───────────────────────────────────────────────────────
    ("program_address",         models.PayloadSchemaType.KEYWORD),
    ("program_type",            models.PayloadSchemaType.KEYWORD),

    # ── Source provenance ─────────────────────────────────────────────
    ("source_code",             models.PayloadSchemaType.KEYWORD),

    # ── Timestamps (unix epoch) ──────────────────────────────────────
    ("created_time",            models.PayloadSchemaType.INTEGER),
    ("updated_time",            models.PayloadSchemaType.INTEGER),

    # ── Request provenance ───────────────────────────────────────────
    ("block_id",                models.PayloadSchemaType.INTEGER),
    ("correlation_id",          models.PayloadSchemaType.KEYWORD),
    ("request_id",              models.PayloadSchemaType.KEYWORD),
    ("request_date",            models.PayloadSchemaType.INTEGER),
    ("api_key",                 models.PayloadSchemaType.KEYWORD),
]

# ─── Oracle findings payload field registry ─────────────────────────────────
# Payload indexes for the oracle_findings collection.

FINDINGS_PAYLOAD_INDEXES: list[tuple[str, models.PayloadSchemaType]] = [
    ("oracle_name",     models.PayloadSchemaType.KEYWORD),
    ("finding_type",    models.PayloadSchemaType.KEYWORD),
    ("token_mint",      models.PayloadSchemaType.KEYWORD),
    ("token_symbol",    models.PayloadSchemaType.KEYWORD),
    ("token_name",      models.PayloadSchemaType.KEYWORD),
    ("project_id",      models.PayloadSchemaType.INTEGER),
    ("created_at",      models.PayloadSchemaType.INTEGER),   # unix epoch for range queries
    ("tier",            models.PayloadSchemaType.KEYWORD),
    ("status",          models.PayloadSchemaType.KEYWORD),
]

# Fields stored in payload but NOT indexed
# (display / context only — not used in Qdrant filters)
PAYLOAD_DISPLAY_FIELDS = {
    "from_label",               # tx_address.label for from_address
    "from_funding_amount",      # lamports — funding tx amount
    "to_label",                 # tx_address.label for to_address
    "token_name",               # full token name
    "decimals",                 # token decimals
    "from_token_pre_balance",
    "from_token_post_balance",
    "to_token_pre_balance",
    "to_token_post_balance",
    "from_sol_pre_balance",
    "from_sol_post_balance",
    "to_sol_pre_balance",
    "to_sol_post_balance",
    "tax_amount",
    "pool_label",
    "program_name",
    "program_verified",
}


# ─── Client ───────────────────────────────────────────────────────────────────

class T16OVectorClient:
    """
    Qdrant client for T16O theGuide Graph RAG layer.

    One collection: t16o_guide_edges
      - One vector per tx_guide edge
      - Point ID  = tx_guide.id (integer — no UUID overhead)
      - Narrative = built by sp_get_guide_edge_narratives, embedded by caller
      - block_time indexed as INTEGER for fast range queries
      - block_time_utc stored as display string in payload

    Dedicated t3.xlarge deployment:
      - INT8 scalar quantization (quantized index pinned in RAM)
      - Raw float32 vectors memory-mapped on disk
      - HNSW graph kept in RAM
      - 14 GB Docker memory cap (2 GB reserved for OS)
    """

    def __init__(
        self,
        host: str = QDRANT_HOST,
        port: int = QDRANT_HTTP_PORT,
        grpc_port: int = QDRANT_GRPC_PORT,
        prefer_grpc: bool = True,
        timeout: int = 30,
    ):
        log.info("Connecting to Qdrant at %s:%d", host, port)
        self.client = QdrantClient(
            host=host,
            port=port,
            grpc_port=grpc_port,
            prefer_grpc=prefer_grpc,
            timeout=timeout,
        )
        self._verify_connection()

    # ── Connection ────────────────────────────────────────────────────────

    def _verify_connection(self) -> None:
        try:
            info = self.client.get_collections()
            existing = [c.name for c in info.collections]
            log.info("Qdrant connected. Collections: %s", existing or "(none)")
        except Exception as exc:
            log.error("Qdrant connection failed: %s", exc)
            raise

    # ── Collection Management ─────────────────────────────────────────────

    def _collection_exists(self, name: str) -> bool:
        try:
            self.client.get_collection(name)
            return True
        except Exception:
            return False

    def init_collection(self, recreate: bool = False,
                        collection_name: str = None) -> None:
        """
        Create a guide edges collection if it doesn't exist.

        Args:
            recreate: Drop and recreate if True.
                      USE WITH CAUTION — destroys all indexed vectors.
            collection_name: Target collection (default: COL_GUIDE_EDGES).
        """
        name = collection_name or COL_GUIDE_EDGES

        if self._collection_exists(name):
            if recreate:
                log.warning("Dropping collection: %s", name)
                self.client.delete_collection(name)
            else:
                log.info("Collection exists, skipping init: %s", name)
                return

        log.info("Creating collection: %s", name)
        self.client.create_collection(
            collection_name=name,
            vectors_config=VectorParams(
                size=VECTOR_DIM,
                distance=DISTANCE_METRIC,
                on_disk=True,           # raw float32 vectors mmap'd on disk
            ),
            on_disk_payload=True,       # payload on disk (edges are verbose)
            hnsw_config=HnswConfigDiff(
                m=HNSW_M,
                ef_construct=HNSW_EF_CONSTRUCT,
                on_disk=HNSW_ON_DISK,
            ),
            quantization_config=ScalarQuantization(
                scalar=ScalarQuantizationConfig(
                    type=ScalarType.INT8,
                    quantile=0.99,
                    always_ram=True,        # quantized index pinned in RAM
                ),
            ),
            optimizers_config=OptimizersConfigDiff(
                indexing_threshold=INDEXING_THRESHOLD,
                memmap_threshold=MEMMAP_THRESHOLD,
            ),
        )

        self._create_payload_indexes(collection_name=name)
        log.info("Collection init complete: %s", name)

    def _create_payload_indexes(self, collection_name: str = None) -> None:
        """Create payload field indexes for all filterable fields."""
        col = collection_name or COL_GUIDE_EDGES
        for field, schema in PAYLOAD_INDEXES:
            try:
                self.client.create_payload_index(
                    collection_name=col,
                    field_name=field,
                    field_schema=schema,
                )
                log.debug("Payload index created: %s.%s", col, field)
            except Exception as exc:
                log.warning("Payload index skipped for %s.%s: %s", col, field, exc)

    def collection_info(self, collection_name: str = None) -> dict:
        """Return point count and status for a collection."""
        col = collection_name or COL_GUIDE_EDGES
        info = self.client.get_collection(col)
        return {
            "name":          col,
            "status":        info.status,
            "points_count":  info.points_count,
            "segments":      info.segments_count,
        }

    # ── Oracle Findings Collection ─────────────────────────────────────────

    def init_findings_collection(self, recreate: bool = False) -> None:
        """
        Create oracle_findings collection if it doesn't exist.
        Same 384-dim / cosine / HNSW config as edges.
        """
        name = COL_ORACLE_FINDINGS

        if self._collection_exists(name):
            if recreate:
                log.warning("Dropping collection: %s", name)
                self.client.delete_collection(name)
            else:
                log.info("Collection exists, skipping init: %s", name)
                return

        log.info("Creating collection: %s", name)
        self.client.create_collection(
            collection_name=name,
            vectors_config=VectorParams(
                size=VECTOR_DIM,
                distance=DISTANCE_METRIC,
                on_disk=True,
            ),
            on_disk_payload=True,
            hnsw_config=HnswConfigDiff(
                m=HNSW_M,
                ef_construct=HNSW_EF_CONSTRUCT,
                on_disk=HNSW_ON_DISK,
            ),
            quantization_config=ScalarQuantization(
                scalar=ScalarQuantizationConfig(
                    type=ScalarType.INT8,
                    quantile=0.99,
                    always_ram=True,
                ),
            ),
            optimizers_config=OptimizersConfigDiff(
                indexing_threshold=INDEXING_THRESHOLD,
                memmap_threshold=MEMMAP_THRESHOLD,
            ),
        )

        for field, schema in FINDINGS_PAYLOAD_INDEXES:
            try:
                self.client.create_payload_index(
                    collection_name=name,
                    field_name=field,
                    field_schema=schema,
                )
                log.debug("Findings payload index created: %s", field)
            except Exception as exc:
                log.warning("Findings payload index skipped for %s: %s", field, exc)

        log.info("Collection init complete: %s", name)

    def insert_finding(self, finding_id: int, vector: list[float], payload: dict) -> bool:
        """
        Upsert a single oracle finding into oracle_findings.
        Point ID = OracleResultRecord.Id (idempotent — re-ingesting overwrites).
        """
        try:
            self.client.upsert(
                collection_name=COL_ORACLE_FINDINGS,
                points=[
                    PointStruct(id=finding_id, vector=vector, payload=payload)
                ],
                wait=True,
            )
            log.info("Finding upserted: id=%d oracle=%s", finding_id, payload.get("oracle_name", "?"))
            return True
        except Exception as exc:
            log.error("insert_finding failed for id=%d: %s", finding_id, exc)
            return False

    def search_findings(
        self,
        query_vector: list[float],
        top_k: int = 5,
        score_threshold: float = 0.30,
        oracle_name: Optional[str] = None,
        finding_type: Optional[str] = None,
        token_mint: Optional[str] = None,
        token_symbol: Optional[str] = None,
        created_at_from: Optional[int] = None,
        created_at_to: Optional[int] = None,
    ) -> list[dict]:
        """
        Semantic search over oracle finding embeddings.
        Returns list of { id, score, payload } sorted by score desc.
        """
        conditions = []

        if oracle_name:
            conditions.append(FieldCondition(
                key="oracle_name", match=MatchValue(value=oracle_name)
            ))
        if finding_type:
            conditions.append(FieldCondition(
                key="finding_type", match=MatchValue(value=finding_type)
            ))
        if token_mint:
            conditions.append(FieldCondition(
                key="token_mint", match=MatchValue(value=token_mint)
            ))
        if token_symbol:
            conditions.append(FieldCondition(
                key="token_symbol", match=MatchValue(value=token_symbol)
            ))
        if created_at_from is not None or created_at_to is not None:
            conditions.append(FieldCondition(
                key="created_at",
                range=Range(gte=created_at_from, lte=created_at_to),
            ))

        query_filter = Filter(must=conditions) if conditions else None

        response = self.client.query_points(
            collection_name=COL_ORACLE_FINDINGS,
            query=query_vector,
            limit=top_k,
            score_threshold=score_threshold,
            query_filter=query_filter,
            with_payload=True,
        )

        return [
            {"id": r.id, "score": r.score, "payload": r.payload}
            for r in response.points
        ]

    def get_findings_count(self) -> int:
        """Return total point count in oracle_findings."""
        try:
            info = self.client.get_collection(COL_ORACLE_FINDINGS)
            return info.points_count or 0
        except Exception:
            return 0

    def findings_collection_info(self) -> dict:
        """Return point count and status for oracle_findings."""
        info = self.client.get_collection(COL_ORACLE_FINDINGS)
        return {
            "name":          COL_ORACLE_FINDINGS,
            "status":        info.status,
            "points_count":  info.points_count,
            "segments":      info.segments_count,
        }

    # ── Insert ────────────────────────────────────────────────────────────

    @staticmethod
    def _build_payload(row: dict) -> dict:
        """
        Build Qdrant point payload from a SP result row.

        Includes all indexed fields + all display fields.
        Excludes 'guide_id' (used as point ID) and 'narrative' (embedded).
        Skips None values to keep payload lean.
        Converts datetime objects to ISO strings for Qdrant compatibility.
        """
        exclude = {
            "guide_id", "narrative",
            # Removed from payload — derivable or low search value
            "block_time_utc", "tx_id", "ins_index", "indicator",
            "from_age_days", "from_token_account", "to_token_account",
            "token_id", "source_name", "source_row_id",
        }
        # Fields to convert from datetime → unix timestamp integer
        unix_time_fields = {
            "created_utc": "created_time",
            "updated_utc": "updated_time",
            "request_date": "request_date",
        }
        # Fields to rename (SP column → Qdrant payload key)
        rename_fields = {
            "from_funded_by": "funded_by",
        }

        payload = {}
        for k, v in row.items():
            k = rename_fields.get(k, k)  # apply rename if mapped
            if k in exclude:
                continue
            if v is None:
                continue
            # Convert created_utc/updated_utc → created_time/updated_time (unix int)
            if k in unix_time_fields:
                if isinstance(v, datetime):
                    payload[unix_time_fields[k]] = int(v.timestamp())
                else:
                    payload[unix_time_fields[k]] = int(v)
                continue
            # MySQL datetime / date objects → ISO string
            if isinstance(v, datetime):
                payload[k] = v.isoformat()
            # MySQL DECIMAL → string (gRPC can't serialize Decimal objects)
            elif hasattr(v, 'as_tuple'):  # duck-type check for Decimal
                payload[k] = str(v)
            else:
                payload[k] = v
        return payload

    def insert_edges(
        self,
        rows: list[dict],
        embeddings: list[list[float]],
        batch_size: int = DEFAULT_BATCH_SIZE,
        collection_name: str = None,
    ) -> int:
        """
        Insert tx_guide edge vectors into a guide edges collection.

        Args:
            rows:            List of dicts from sp_get_guide_edge_narratives.
                             Each dict must contain 'guide_id' and all payload cols.
            embeddings:      Parallel list of embedding vectors (dim=VECTOR_DIM).
                             One vector per row — caller embeds the 'narrative' field.
            batch_size:      Points per upsert call.
            collection_name: Target collection (default: COL_GUIDE_EDGES).

        Returns:
            Total points upserted.

        Typical call pattern:
            db_rows    = call_sp(start_id=1, end_id=1000)
            narratives = [r["narrative"] for r in db_rows]
            vectors    = embed(narratives)         # caller's embedding fn
            vc.insert_edges(db_rows, vectors)
        """
        col = collection_name or COL_GUIDE_EDGES

        if len(rows) != len(embeddings):
            raise ValueError(
                f"rows ({len(rows)}) and embeddings ({len(embeddings)}) "
                "must be the same length"
            )

        total    = len(rows)
        inserted = 0

        for i in range(0, total, batch_size):
            batch_rows = rows[i : i + batch_size]
            batch_vecs = embeddings[i : i + batch_size]
            is_last    = (i + batch_size) >= total

            points = [
                PointStruct(
                    id=int(row["guide_id"]),     # tx_guide.id as integer point ID
                    vector=vec,
                    payload=self._build_payload(row),
                )
                for row, vec in zip(batch_rows, batch_vecs)
            ]

            self.client.upsert(
                collection_name=col,
                points=points,
                wait=is_last,           # async mid-batch, sync on last flush
            )

            inserted += len(points)
            log.debug(
                "Upserted batch %d/%d (%d pts)",
                (i // batch_size) + 1,
                -(-total // batch_size),
                len(points),
            )

        log.info("Insert complete: %d edges → %s", inserted, col)
        return inserted

    # ── Search ────────────────────────────────────────────────────────────

    def search_edges(
        self,
        query_vector: list[float],
        top_k: int = DEFAULT_TOP_K,
        score_threshold: float = 0.70,
        # ── Time range (primary filter) ───────────────────────────
        block_time_from: Optional[int] = None,      # unix epoch
        block_time_to:   Optional[int] = None,      # unix epoch
        # ── Edge filters ─────────────────────────────────────────
        edge_type_code:    Optional[str] = None,    # e.g. 'SWAP_OUT'
        edge_category:     Optional[str] = None,    # e.g. 'swap'
        edge_direction:    Optional[str] = None,    # 'outflow'|'inflow'|'neutral'
        risk_weight_min:   Optional[int] = None,    # 0–100
        swap_direction:    Optional[str] = None,    # 'in'|'out'
        # ── Wallet filters ────────────────────────────────────────
        from_address:      Optional[str] = None,
        from_address_type: Optional[str] = None,   # 'wallet'|'bot'|'dex_wallet'|...
        to_address:        Optional[str] = None,
        to_address_type:   Optional[str] = None,
        from_funded_by:    Optional[str] = None,   # funder address
        # ── Token filters ─────────────────────────────────────────
        token_symbol:      Optional[str] = None,   # e.g. 'BONK'
        token_mint:        Optional[str] = None,   # mint address
        token_type:        Optional[str] = None,   # 'fungible'|'nft'|...
        # ── Pool filters ──────────────────────────────────────────
        dex:               Optional[str] = None,   # e.g. 'Raydium'
        token_pair:        Optional[str] = None,   # e.g. 'SOL-USDC'
        pool_address:      Optional[str] = None,
        # ── Program filters ───────────────────────────────────────
        program_address:   Optional[str] = None,
        program_type:      Optional[str] = None,   # 'dex'|'lending'|...
        # ── Source filter ─────────────────────────────────────────
        source_code:       Optional[str] = None,   # 'tx_swap'|'tx_transfer'|...
    ) -> list[dict]:
        """
        Semantic search over tx_guide edge embeddings.

        All filter args optional and combinable.
        block_time_from / block_time_to use the indexed integer epoch field.
        Returns list of { id, score, payload } sorted by score desc.
        """
        conditions = []

        # Time range — integer epoch, hits block_time payload index
        if block_time_from is not None or block_time_to is not None:
            conditions.append(FieldCondition(
                key="block_time",
                range=Range(gte=block_time_from, lte=block_time_to),
            ))

        # Edge classification
        if edge_type_code:
            conditions.append(FieldCondition(
                key="edge_type_code", match=MatchValue(value=edge_type_code)
            ))
        if edge_category:
            conditions.append(FieldCondition(
                key="edge_category", match=MatchValue(value=edge_category)
            ))
        if edge_direction:
            conditions.append(FieldCondition(
                key="edge_direction", match=MatchValue(value=edge_direction)
            ))
        if risk_weight_min is not None:
            conditions.append(FieldCondition(
                key="risk_weight", range=Range(gte=risk_weight_min)
            ))
        if swap_direction:
            conditions.append(FieldCondition(
                key="swap_direction", match=MatchValue(value=swap_direction)
            ))

        # Wallet
        if from_address:
            conditions.append(FieldCondition(
                key="from_address", match=MatchValue(value=from_address)
            ))
        if from_address_type:
            conditions.append(FieldCondition(
                key="from_address_type", match=MatchValue(value=from_address_type)
            ))
        if to_address:
            conditions.append(FieldCondition(
                key="to_address", match=MatchValue(value=to_address)
            ))
        if to_address_type:
            conditions.append(FieldCondition(
                key="to_address_type", match=MatchValue(value=to_address_type)
            ))
        if from_funded_by:
            conditions.append(FieldCondition(
                key="funded_by", match=MatchValue(value=from_funded_by)
            ))

        # Token
        if token_symbol:
            conditions.append(FieldCondition(
                key="token_symbol", match=MatchValue(value=token_symbol)
            ))
        if token_mint:
            conditions.append(FieldCondition(
                key="token_mint", match=MatchValue(value=token_mint)
            ))
        if token_type:
            conditions.append(FieldCondition(
                key="token_type", match=MatchValue(value=token_type)
            ))

        # Pool
        if dex:
            conditions.append(FieldCondition(
                key="dex", match=MatchValue(value=dex)
            ))
        if token_pair:
            conditions.append(FieldCondition(
                key="token_pair", match=MatchValue(value=token_pair)
            ))
        if pool_address:
            conditions.append(FieldCondition(
                key="pool_address", match=MatchValue(value=pool_address)
            ))

        # Program
        if program_address:
            conditions.append(FieldCondition(
                key="program_address", match=MatchValue(value=program_address)
            ))
        if program_type:
            conditions.append(FieldCondition(
                key="program_type", match=MatchValue(value=program_type)
            ))

        # Source
        if source_code:
            conditions.append(FieldCondition(
                key="source_code", match=MatchValue(value=source_code)
            ))

        query_filter = Filter(must=conditions) if conditions else None

        response = self.client.query_points(
            collection_name=COL_GUIDE_EDGES,
            query=query_vector,
            limit=top_k,
            score_threshold=score_threshold,
            query_filter=query_filter,
            with_payload=True,
        )

        return [
            {"id": r.id, "score": r.score, "payload": r.payload}
            for r in response.points
        ]

    # ── Lookup / Scroll ───────────────────────────────────────────────────

    def get_edge(self, guide_id: int) -> Optional[dict]:
        """Fetch a single edge by tx_guide.id. Returns None if not found."""
        try:
            results = self.client.retrieve(
                collection_name=COL_GUIDE_EDGES,
                ids=[guide_id],
                with_payload=True,
                with_vectors=False,
            )
            if results:
                r = results[0]
                return {"id": r.id, "payload": r.payload}
            return None
        except Exception as exc:
            log.warning("get_edge failed for guide_id=%s: %s", guide_id, exc)
            return None

    def get_edges_by_tx(self, tx_id: int) -> list[dict]:
        """
        Scroll all edges for a given tx_id.
        Uses scroll (no vector needed).
        """
        results, _ = self.client.scroll(
            collection_name=COL_GUIDE_EDGES,
            scroll_filter=Filter(must=[
                FieldCondition(key="tx_id", match=MatchValue(value=tx_id))
            ]),
            with_payload=True,
            with_vectors=False,
            limit=100,
        )
        return [{"id": r.id, "payload": r.payload} for r in results]

    def get_edges_by_wallet(
        self,
        address: str,
        block_time_from: Optional[int] = None,
        block_time_to:   Optional[int] = None,
        limit: int = 200,
    ) -> list[dict]:
        """
        Scroll all edges where from_address OR to_address = address.
        Optionally bounded by block_time range.
        Deduplicates results (edge appears once even if both sides match).
        """
        time_conditions = []
        if block_time_from is not None or block_time_to is not None:
            time_conditions.append(FieldCondition(
                key="block_time",
                range=Range(gte=block_time_from, lte=block_time_to),
            ))

        seen: set[int] = set()
        results: list[dict] = []

        for addr_field in ("from_address", "to_address"):
            scroll_results, _ = self.client.scroll(
                collection_name=COL_GUIDE_EDGES,
                scroll_filter=Filter(must=[
                    FieldCondition(
                        key=addr_field, match=MatchValue(value=address)
                    ),
                    *time_conditions,
                ]),
                with_payload=True,
                with_vectors=False,
                limit=limit,
            )
            for r in scroll_results:
                if r.id not in seen:
                    seen.add(r.id)
                    results.append({"id": r.id, "payload": r.payload})

        return results

    # ── Delete ────────────────────────────────────────────────────────────

    def delete_edges_by_tx(self, tx_id: int) -> None:
        """Delete all edges for a given tx_id."""
        self.client.delete(
            collection_name=COL_GUIDE_EDGES,
            points_selector=models.FilterSelector(
                filter=Filter(must=[
                    FieldCondition(key="tx_id", match=MatchValue(value=tx_id))
                ])
            ),
        )
        log.info("Deleted edges for tx_id=%d", tx_id)

    def delete_edges_by_wallet(self, address: str) -> None:
        """Delete all edges where from_address = address."""
        self.client.delete(
            collection_name=COL_GUIDE_EDGES,
            points_selector=models.FilterSelector(
                filter=Filter(must=[
                    FieldCondition(
                        key="from_address", match=MatchValue(value=address)
                    )
                ])
            ),
        )
        log.info("Deleted edges for from_address=%s", address)

    # ── Snapshots ─────────────────────────────────────────────────────────

    def create_snapshot(self) -> SnapshotDescription:
        """Create a point-in-time snapshot of t16o_guide_edges."""
        snap = self.client.create_snapshot(collection_name=COL_GUIDE_EDGES)
        log.info(
            "Snapshot created: %s (%.1f MB)",
            snap.name,
            (snap.size or 0) / 1_048_576,
        )
        return snap

    def list_snapshots(self) -> list[SnapshotDescription]:
        """List all snapshots for t16o_guide_edges."""
        return self.client.list_snapshots(collection_name=COL_GUIDE_EDGES)

    # ── Health / Diagnostics ──────────────────────────────────────────────

    def health_check(self) -> dict:
        """Return health status and collection stats."""
        status: dict = {"healthy": False, "collection": None, "timestamp": None}
        try:
            status["collection"] = self.collection_info()
            status["healthy"]    = True
            status["timestamp"]  = datetime.now(timezone.utc).isoformat()
        except Exception as exc:
            status["error"] = str(exc)
            log.error("Health check failed: %s", exc)
        return status

    def optimize(self) -> None:
        """
        Flush and merge segments after large bulk ingestion.
        Temporarily drops indexing_threshold to 0 to force optimizer,
        then restores the normal threshold.
        """
        log.info("Triggering optimizer on %s ...", COL_GUIDE_EDGES)
        self.client.update_collection(
            collection_name=COL_GUIDE_EDGES,
            optimizers_config=OptimizersConfigDiff(indexing_threshold=0),
        )
        time.sleep(2)
        self.client.update_collection(
            collection_name=COL_GUIDE_EDGES,
            optimizers_config=OptimizersConfigDiff(
                indexing_threshold=INDEXING_THRESHOLD
            ),
        )
        log.info("Optimizer triggered on %s", COL_GUIDE_EDGES)


# ─── Convenience factory ──────────────────────────────────────────────────────

def get_client(**kwargs) -> T16OVectorClient:
    """Return a connected T16OVectorClient. Raises on connection failure."""
    return T16OVectorClient(**kwargs)


# ─── CLI smoke test ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json
    import random

    print("\n── T16O Qdrant Client Smoke Test ──\n")

    vc = get_client()
    vc.init_collection(recreate=False)

    health = vc.health_check()
    print(json.dumps(health, indent=2, default=str))

    # Synthetic rows mimicking sp_get_guide_edge_narratives output
    def rand_vec(dim: int = VECTOR_DIM) -> list[float]:
        v   = [random.gauss(0, 1) for _ in range(dim)]
        mag = sum(x ** 2 for x in v) ** 0.5
        return [x / mag for x in v]

    test_rows = [
        {
            "guide_id":              i,
            "narrative":             f"Edge: SWAP_OUT [swap/outflow] risk=65 | test edge {i}",
            "block_time":            1_700_000_000 + i * 60,
            "block_time_utc":        "2024-01-15 14:23:11",
            "signature":             f"test_sig_{i}",
            "tx_id":                 1000 + i,
            "ins_index":             0,
            "edge_type_code":        "SWAP_OUT",
            "edge_category":         "swap",
            "edge_direction":        "outflow",
            "risk_weight":           65,
            "indicator":             0,
            "swap_direction":        "out",
            "from_address":          f"wallet_{i % 3}",
            "from_address_type":     "wallet",
            "from_label":            None,
            "from_age_days":         47,
            "funded_by":        "funder_wallet_abc",
            "from_funding_amount":   5_000_000,
            "to_address":            "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
            "to_address_type":       "dex_wallet",
            "to_label":              "Jupiter Aggregator v6",
            "from_token_account":    None,
            "to_token_account":      None,
            "token_id":              42,
            "token_symbol":          "BONK",
            "token_name":            "Bonk",
            "token_mint":            "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
            "decimals":              5,
            "token_type":            "fungible",
            "amount":                1_500_000_000,
            "from_token_pre_balance":  5_000_000_000,
            "from_token_post_balance": 3_500_000_000,
            "to_token_pre_balance":    None,
            "to_token_post_balance":   None,
            "from_sol_pre_balance":    2_500_000_000,
            "from_sol_post_balance":   2_499_500_000,
            "to_sol_pre_balance":      None,
            "to_sol_post_balance":     None,
            "tax_bps":               None,
            "tax_amount":            None,
            "fee":                   5_000,
            "priority_fee":          1_000,
            "pool_address":          f"pool_addr_{i % 2}",
            "pool_label":            "Raydium BONK-SOL",
            "dex":                   "Raydium",
            "token_pair":            "BONK-SOL",
            "program_address":       "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
            "program_name":          "Raydium AMM",
            "program_type":          "dex",
            "program_verified":      1,
            "source_code":           "tx_swap",
            "source_name":           "Swap",
            "source_row_id":         100 + i,
        }
        for i in range(1, 11)
    ]

    test_vecs = [rand_vec() for _ in test_rows]

    n = vc.insert_edges(test_rows, test_vecs, batch_size=5)
    print(f"\nInserted {n} test edges.")

    # Search — Raydium swaps
    results = vc.search_edges(
        query_vector=rand_vec(),
        top_k=3,
        score_threshold=0.0,
        dex="Raydium",
        edge_category="swap",
    )
    print(f"\nSearch (Raydium swaps): {len(results)} results")
    for r in results:
        print(f"  score={r['score']:.4f}  guide_id={r['id']}"
              f"  sig={r['payload']['signature']}")

    # Wallet scroll with time range
    wallet_edges = vc.get_edges_by_wallet(
        address="wallet_0",
        block_time_from=1_700_000_000,
        block_time_to=1_700_001_000,
    )
    print(f"\nWallet edges (wallet_0, time range): {len(wallet_edges)}")

    # Single lookup
    edge = vc.get_edge(guide_id=1)
    print(f"\nLookup guide_id=1: "
          f"{edge['payload']['edge_type_code'] if edge else 'NOT FOUND'}")

    print("\n── Smoke test complete ──\n")
