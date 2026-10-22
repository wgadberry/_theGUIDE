# Session Context - T16O Database Optimization

**Last Updated:** 2026-02-15 (Session 3)
**Working Directory:** `C:\Users\wgadb\source\repos\T16O\sql\shred\_theGuide\_build_all\_wrk`

---

## Database Connection

```
Host: localhost
Port: 3396
User: root
Password: rootpassword
Database: t16o_db
```

Example query:
```bash
mysql -h localhost -P 3396 -u root -prootpassword t16o_db -e "SELECT 1;"
```

---

## Recent Commits (Latest First)

| Commit | Description |
|--------|-------------|
| `a3f19ee` | Fix sp_tx_token_backfill missing empty-string token symbols |
| `a821367` | Optimize sp_tx_bmap_get from 3.9s to 370ms (10x faster) |
| `00040ce` | Fix sp_tx_pool_backfill inflating attempt_cnt and blocking API enrichment |
| `1af4b50` | Remove tx_funding_edge references from guide-funder.py |
| `ca6ba56` | Optimize sp_tx_pool_backfill: isolate working set before calling UDFs |
| `1d310da` | Remove tx_funding_edge table and all references — write-only aggregate never read |
| `8b1f9db` | Drop redundant indexes and FK constraints from 6 tables to reduce disk overhead |
| `eb82cb9` | Add tx_guide indexes for bmap SP and use g.block_time for index coverage |
| `6845b7f` | Add functional index for guide_loader batch selection on tx table |
| `ab79a43` | Remove tx_json NULL-out pass from guide-shredder staging purge |
| `5e36a97` | Drop unused indexes, FK constraints, and dead columns from tx table; fix funder crash |

---

## Key Files

### BMap Viewer
- **`C:\Users\wgadb\source\repos\T16O\sql\shred\ClusterMaps\bmap-viewer\bmap-viewer.html`**
  - Main viewer UI with D3.js visualization
  - TX Flow diagram with buy/sell swap labels and parallel edge fix
- **`C:\Users\wgadb\source\repos\T16O\sql\shred\ClusterMaps\bmap-viewer\bmap-api.py`**
  - Flask API serving bmap data and LLM explain endpoint
  - Anthropic API key loaded from `_wrk/files/anthropic-api-key-bmap-viewer.txt`

### Stored Procedures
- **`sp_tx_bmap_get.sql`** - Main BMap data API procedure (6 params)
- **`sp_tx_guide_loader.sql`** - Guide edge generation from tx_activity/tx_transfer/tx_swap
- **`sp_tx_pool_backfill.sql`** - Pool metadata backfill from local label data
- **`sp_tx_token_backfill.sql`** - Token metadata backfill from local label data

### Python Scripts
- **`guide-shredder.py`** - Staging table consumer, calls sp_tx_parse_staging_decode/detail
- **`guide-aggregator.py`** - Runs sp_tx_guide_loader, syncs tx_token_participant
- **`guide-enricher.py`** - Solscan API enrichment for tokens and pools
- **`guide-funder.py`** - Address classification and funding detection

---

## Session 3 Work Summary (2026-02-15)

### 1. sp_tx_bmap_get Optimization (3.93s → 370ms, 10x faster)

Profiled the SP with `SET profiling_history_size = 100` to identify all step timings.

**Bottleneck 1: pool_label enrichment (2.5s → <1ms)**
- Correlated subquery `SELECT pool_label FROM tx_guide WHERE pool_address_id = n.address_id` did a full table scan (1M+ rows) — no index on `pool_address_id` after session 2 cleanup
- **Fix**: Replaced with direct `JOIN tx_pool p ON p.pool_address_id = n.address_id` — tx_pool has pool_label on 22,880/23,022 records and is indexed

**Bottleneck 2: SOL balance lookups (1.36s → 333ms)**
- Original: INSERT all rows per address with `block_time <= v_block_time`, keep latest via ON DUPLICATE KEY — for dex_wallets with 3,260+ rows, this inserts thousands of rows per address
- Tried LATERAL joins — MySQL 8.0 doesn't do backward index scan for LATERAL, still uses filesort (710ms)
- Tried MAX subquery — worked great interactively (57ms) but slower inside SP context (2.7s)
- **Final fix**: Two-step approach — GROUP BY + MAX(block_time) per address into tmp_sol_max, then fetch just that one row
- **Covering index**: Extended `idx_from_time` and `idx_to_time` with `from/to_sol_post_balance` as 3rd column so IS NOT NULL filter resolves at index level (no row fetches needed)

**Migration**: `migrate_bmap_covering_indexes.sql`

| Step | Before | After |
|------|--------|-------|
| pool_label enrichment | 2,515ms | <1ms |
| SOL balance (FROM+TO) | 1,360ms | 333ms |
| Everything else | 55ms | 37ms |
| **TOTAL** | **3,930ms** | **370ms** |

### 2. sp_tx_token_backfill Empty String Bug Fix

- **Bug**: WHERE clause checked `token_symbol IS NULL` but Solscan API stores empty strings (`''`) for LP tokens without metadata
- **Impact**: 198 tokens with `token_symbol = ''` were never backfilled from address labels (175 had parseable labels)
- **Fix**: Changed to `(token_symbol IS NULL OR token_symbol = '')`, added `a.label IS NOT NULL` guard
- **Result**: 175 tokens backfilled (e.g., "Meteora (WSOL) LP Token" → symbol "WSOL")
- Example: `FZN7QZ8ZUUAxMPfxYEYkH3cXUASzH8EqA6B4tyCL8f1j` — tx_token id=11 now has symbol/name

### 3. Database Backup

- Full backup: `t16o_db_backup_20260214_195920.sql.gz` (246 MB)
- Includes both `t16o_db` and `t16o_db_staging` with routines and triggers

---

## Session 2 Work Summary (2026-02-14)

### 1. Balance Table Index Cleanup (migrate_balance_index_cleanup.sql)
- **tx_sol_balance_change**: Dropped 2 indexes + 2 FKs
  - `idx_sbc_tx_addr` (329 MB) — covering extension of idx_tx_address, redundant
  - `idx_sbc_addr_tx` (145 MB) — redundant, idx_sbc_addr_blocktime covers address_id-first lookups
- **tx_token_balance_change**: Dropped 5 indexes + 4 FKs
  - `idx_tbc_tx_token_acct` (142 MB) — redundant with idx_tbc_tx_token_owner for (tx_id, token_id) JOIN
  - `idx_tbc_tx_token_owner` (145 MB) — same-tx JOIN covered by idx_tx_owner_token
  - `idx_tbc_acct_token_tx` (62 MB) — idx_tbc_acct_token_blocktime covers same leading columns
  - `idx_tbc_owner_token_tx` (62 MB) — idx_tbc_owner_token_blocktime covers same leading columns
  - `idx_token` (30 MB) — no query filters token_id alone
- **Result**: 1,265 MB reclaimed after tablespace rebuild (2,204 MB → 939 MB)

### 2. Round 2 Index Cleanup — 4 Large Tables (migrate_index_cleanup_round2.sql)
- **tx_guide** (662 MB indexes): Dropped 7 indexes + 9 FKs (~202 MB saved)
- **tx_transfer** (368 MB indexes): Dropped 6 indexes + 9 FKs (~157 MB saved)
- **tx_activity** (231 MB indexes): Dropped 4 indexes + 4 FKs (~112 MB saved)
- **tx_swap** (113 MB indexes): Dropped 13 indexes + 14 FKs (~83 MB saved)

### 3. tx_funding_edge Table Removal
- Write-only materialized aggregate — removed from sp_tx_guide_loader, guide-funder.py, sp_tx_data_integrity_check, sp_tx_clear_tables
- Dropped table (27 MB, 124K rows)

### 4. sp_tx_pool_backfill Optimization (28s → 2s)
- Materialize small candidate set into temp tables before calling UDFs

### 5. sp_tx_pool_backfill attempt_cnt Bug Fix
- SP incremented attempt_cnt on every cycle — blocked 631 pools from API enrichment
- Removed increment, reset affected pools

### 6. Staging Query Index + Performance Schema Audit
- No remaining slow query hotspots after all optimizations

---

## Total Database Optimization Summary (Sessions 1+2+3)

| Metric | Before | After |
|--------|--------|-------|
| **Indexes dropped** | — | 51 indexes |
| **Indexes extended** | — | 2 (idx_from_time, idx_to_time with covering columns) |
| **FK constraints dropped** | — | 48 FKs |
| **Tables dropped** | — | tx_funding_edge |
| **Columns dropped** | — | tx.type_state, tx.is_guide_loaded |
| **Disk reclaimed** | — | ~2+ GB index space |
| **sp_tx_bmap_get** | 3.93s | 0.37s (10x) |
| **sp_tx_pool_backfill** | 28s | 2s (14x) |

### Current Index Counts Per Table
| Table | Indexes | FKs |
|-------|---------|-----|
| tx | 6 | 0 |
| tx_guide | 10 | 0 |
| tx_transfer | 6 | 0 |
| tx_activity | 3 | 0 |
| tx_swap | 5 | 0 |
| tx_sol_balance_change | 3 | 0 |
| tx_token_balance_change | 5 | 0 |

---

## Test Transactions

Swap visualization test:
```
3FNYD7AiuEbaEMoVceCjEveFfUr6hcMM89rBJ4ZWEd9rjfnJocLPmNZxYwH9ay6dfiXoS7LfcKrhcMvJZ7mYh1pr
336jYzZx1n3Ajh6ecmMbW8ww2HN38D7NUL3eFXifA1HMyZWrSexSf5KURpAk4ySRK68RNpmUfjPfFN3Ubbz3Bo4Q
```

BMap performance test:
```
5efgv9fDWoQe4NBzkFxcwGdFXCJQ6BNLsySvHK9MwXo4CD5oryGck3eYGhS1KMyY1ymcxcwHfRU1JMxcw7jQTZBA
```

Close_ata test:
```
64EFYV5Doz99nujrghA9eQtTNzUVFRnLLkApUvexRBUoHCLKAtA8WTarcBkyrmRdPp8AwwXrpVvUwuQvdjF2DT67
```

Query:
```sql
CALL sp_tx_bmap_get(NULL, NULL, NULL, '<signature>', NULL, 10);
```

---

## Docker Containers

```
t16o_v1_mysql     - MySQL database
t16o_v1_rabbitmq  - RabbitMQ message queue
t16o_v1_influxdb  - InfluxDB metrics
```

---

## Pending/Future Work

- Guide aggregator had 100K+ pending tx when last run — may need to process backlog
- `innodb_redo_log_capacity` and `innodb_log_buffer_size` in my.cnf should be updated to match actual working values (2G and 256M respectively)
- Consider changing `tx_state` from VARCHAR(16) to BIGINT UNSIGNED for proper bitmask type (currently relies on implicit cast)
- Run `compact_vhdx.ps1` (Admin PowerShell) to reclaim Docker VHDX disk space after index cleanup
- SOL balance GROUP BY still ~333ms — could be further optimized with a dedicated materialized balance table if needed

---

## How to Resume

1. Open Claude Code in the working directory
2. Reference this file: "Read session.md for context"
3. Continue with any new tasks

---

## Git Status at Session End

Branch: `main` (commit `a3f19ee`)

Modified but not committed (pre-existing):
- `_db/procedures/sp_tx_insert_txs_batch.sql`
- `_db/procedures/sp_tx_parse_staging_decode.sql`
- `_db/procedures/sp_tx_parse_staging_detail.sql`
- `_db/procedures/sp_tx_token_count.sql`
- `_db/tables/tx_guide.sql`
- `_wrk/.claude/settings.local.json`

Untracked files (temp/scratch):
- Various `sp_*.sql` files (proposals/backups)
- `bmap_*.json` files (test outputs)
- `__pycache__/`
- `Backups/DB/t16o_db_backup_*.sql.gz`

## Backups

| File | Date | Size |
|------|------|------|
| `t16o_db_backup_20260214_195920.sql.gz` | 2026-02-15 | 246 MB |
| `t16o_db_backup_20260208_205602.sql.gz` | 2026-02-08 | — |
