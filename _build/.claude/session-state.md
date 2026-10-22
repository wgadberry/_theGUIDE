# Claude Session State - 2026-01-14

## Last Updated
2026-01-14 ~Session Continuation

## Current System State

### Staging Table (t16o_db_staging.txs)
- **tx_state=4 (shredded)**: Processed rows
- Pipeline: 8=decoded -> 16=detailed -> 4=shredded

### TX Table (t16o_db.tx)
- **tx_state bitmask**: 4(shredded) + 8(decoded) + 16(detailed) + 32(edged) + 64(mapped)

### Windows Services
Services configured via NSSM:
- T16OExchange.Guide.Aggregator (--daemon mode)
- T16OExchange.Guide.Decoder (--queue-consumer mode)
- T16OExchange.Guide.Detailer (--queue-consumer mode)
- T16OExchange.Guide.Enricher (--daemon mode)
- T16OExchange.Guide.Funder (--daemon mode)
- T16OExchange.Guide.Gateway (--queue-consumer mode)
- T16OExchange.Guide.Producer (--daemon mode)
- T16OExchange.Guide.Shredder (--daemon mode)

## Performance Optimizations Applied (This Session)

### 1. Two-Phase Bulk Insert Pattern (KEY OPTIMIZATION)
**Problem**: fn_tx_ensure_* functions called inline with JSON_TABLE execute per-row (3 queries each), creating N*3 queries per batch.

**Solution**: Two-phase approach:
- **Phase 1**: Pre-populate lookup tables with bulk INSERT IGNORE
- **Phase 2**: Main INSERT with JOINs instead of function calls

### 2. Files Refactored with Two-Phase Pattern

#### sp_tx_insert_txs_batch.sql (NEW)
- Bulk inserts all transactions from staging JSON
- Phase 1: Populates tx_address, tx_token, tx_program
- Phase 2: INSERT tx with JOINs to lookup tables
- Creates tmp_batch_tx_signatures for child processing
- Location: `_db/procedures/sp_tx_insert_txs_batch.sql`

#### sp_tx_insert_sol_balance.sql (REFACTORED)
- Two-phase for SOL balance changes
- Phase 1: Pre-populate tx_address with wallet addresses
- Phase 2: INSERT with JOIN to tx_address
- Location: `_db/procedures/sp_tx_insert_sol_balance.sql`

#### sp_tx_insert_token_balance.sql (REFACTORED)
- Two-phase for token balance changes
- Phase 1a: Pre-populate tx_address (ATAs, owners, mints)
- Phase 1b: Pre-populate tx_token
- Phase 2: INSERT with JOINs to address and token tables
- Location: `_db/procedures/sp_tx_insert_token_balance.sql`

#### sp_tx_parse_staging_decode.sql (MODIFIED)
- Now calls sp_tx_insert_txs_batch for bulk tx insert
- Uses cursor on tmp_batch_tx_signatures for child processing
- Batch UPDATEs at end using temp table
- Location: `_db/procedures/sp_tx_parse_staging_decode.sql`

#### sp_tx_parse_staging_detail.sql (MODIFIED)
- Batch UPDATE moved outside loop using tmp_detail_tx_ids temp table
- Location: `_db/procedures/sp_tx_parse_staging_detail.sql`

### 3. fn_tx_ensure_* Functions Optimized
Changed from 3 queries (SELECT, INSERT IGNORE, SELECT) to 1-2 queries:
```sql
INSERT IGNORE INTO table (...) VALUES (...);
IF ROW_COUNT() > 0 THEN
    RETURN LAST_INSERT_ID();
END IF;
SELECT id INTO v_id FROM table WHERE ... LIMIT 1;
RETURN v_id;
```

Affected functions:
- `fn_tx_ensure_address.sql` - Single address parameter
- `fn_tx_ensure_token.sql` - Single mint_address_id parameter
- `fn_tx_ensure_program.sql` - Single program_address_id parameter
- `fn_tx_ensure_pool.sql` - Single pool_address_id parameter

### 4. Decimal Precision Fix (guide-aggregator.py, sp_tx_bmap_state_backfill.sql)
**Problem**: 66,417 warnings from DOUBLE truncation to DECIMAL(30,9)
**Fix**: Added ROUND(..., 9) to delta and balance calculations
```python
ROUND(amount / POW(10, decimals), 9)
ROUND(SUM(delta), 9)
ROUND(SUM(delta) OVER (PARTITION BY ... ORDER BY ...), 9)
```

### 5. Batch UPDATE Optimization
Moved per-row UPDATEs outside loops using temp tables:
- sp_tx_parse_staging_decode: Uses tmp_batch_tx_signatures
- sp_tx_parse_staging_detail: Uses tmp_detail_tx_ids

## Pending Optimizations

### Apply Two-Phase Pattern to Remaining Child Procedures
These still call fn_tx_ensure_* inline:
- `sp_tx_insert_transfers.sql`
- `sp_tx_insert_swaps.sql`
- `sp_tx_insert_activities.sql`

## tx_state Bitmask Reference
| Bit | Value | Name | Description |
|-----|-------|------|-------------|
| 2 | 4 | shredded | Parsed into tx tables |
| 3 | 8 | decoded | Has decoded data |
| 4 | 16 | detailed | Has balance change data |
| 5 | 32 | edged | Guide edges created |
| 6 | 64 | mapped | Token mapping complete |

## Staging tx_state Reference
| Value | Meaning |
|-------|---------|
| 1 | Processing (claimed by worker) |
| 4 | Shredded (done) |
| 8 | Decoded (ready for shredder) |
| 16 | Detailed (ready for shredder) |

## Recent Commits
```
13660d2 Refactor insert procedures: two-phase bulk approach
857ba07 Add batch tx insert procedure (WIP optimization)
0a42d4e Batch UPDATE statements in staging parse procedures
7594cef Optimize ensure functions and fix bmap precision warnings
```

## Key Code Patterns

### Two-Phase Bulk Insert Pattern
```sql
-- PHASE 1: Pre-populate lookup tables
INSERT IGNORE INTO tx_address (address, address_type)
SELECT DISTINCT addr, addr_type FROM (
    SELECT t.field AS addr, 'type' AS addr_type
    FROM JSON_TABLE(p_json, '$.data[*]' COLUMNS (...)) t
    WHERE t.field IS NOT NULL AND t.field != 'null'
    UNION
    -- more fields...
) AS all_addresses;

-- PHASE 2: Insert with JOINs (no function calls)
INSERT INTO target_table (...)
SELECT ...
FROM JSON_TABLE(...) AS t
JOIN tx_address a ON a.address = t.field
JOIN tx_token tok ON tok.mint_address_id = a.id;
```

## Verification Commands
```powershell
# Check staging state
mysql -h 127.0.0.1 -P 3396 -u root -prootpassword -N -e 'SELECT tx_state, COUNT(*) FROM t16o_db_staging.txs GROUP BY tx_state'

# Check tx state
mysql -h 127.0.0.1 -P 3396 -u root -prootpassword -D t16o_db -N -e 'SELECT tx_state, COUNT(*) FROM tx GROUP BY tx_state'

# Test shredder
python -u guide-shredder.py --once --batch-size 10

# Check performance
mysql -h 127.0.0.1 -P 3396 -u root -prootpassword -D performance_schema -N -e "SELECT DIGEST_TEXT, COUNT_STAR, AVG_TIMER_WAIT/1000000000 as avg_ms FROM events_statements_summary_by_digest ORDER BY SUM_TIMER_WAIT DESC LIMIT 10"
```
