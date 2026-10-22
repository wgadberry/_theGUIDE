# Plan: tx_guide Deduplication and Swap Enrichment

Directory Containing all DB Objects:C:\Users\wgadb\source\repos\T16O\sql\shred\_theGuide\_build_all\_db
## Problem Statement

The `tx_guide` table currently stores **duplicate records** for swap transactions:

1. **Source_id=1 (Raw Transfers)**: From `tx_transfer` table
   - Has: `from_token_account_id`, `to_token_account_id`, pre/post balances, tax info
   - Missing: DEX name, pool label, swap semantics
   - Edge types: `spl_transfer` (2), `create_ata` (8), `close_ata` (9)

2. **Source_id=2 (Swap Summaries)**: From `tx_swap` table
   - Has: DEX name (via join), pool info, swap direction
   - Missing: Token account IDs, balances, tax info
   - Edge types: `swap_in` (4), `swap_out` (3)

**Result**: A single swap appears as 2 rows with the same amount/token but different `ins_index` values, causing duplicate display in TX Flow visualization.

### Example Data (tx_id=113)

| id | source_id | edge_type | token | amount | ins_index | Has DEX? | Has Balances? |
|----|-----------|-----------|-------|--------|-----------|----------|---------------|
| 1338 | 2 | swap_in (4) | WSOL | 0.0945 | 0 | Yes | No |
| 314 | 1 | spl_transfer (2) | WSOL | 0.0945 | 2 | No | Yes |

Both represent the same WSOL transfer but with different metadata.

---

## Chosen Solution: Single Row with Enrichment

Instead of storing 2 rows per swap movement, store **1 enriched row** that combines:
- All transfer detail (token accounts, balances, tax) from `tx_transfer`
- Swap context (DEX, pool, direction) from `tx_swap`

### Benefits
- 50% less storage for swap transactions
- No deduplication needed in queries/UI
- Single source of truth
- Full flexibility to query by type

---

## Schema Changes Required

### Add columns to `tx_guide`:

```sql
ALTER TABLE tx_guide
  ADD COLUMN dex VARCHAR(50) NULL COMMENT 'DEX name (Raydium, Orca, Jupiter, etc.)',
  ADD COLUMN pool_address_id INT UNSIGNED NULL COMMENT 'Pool/AMM address ID',
  ADD COLUMN pool_label VARCHAR(100) NULL COMMENT 'Pool label (e.g., WSOL-USDC Market)',
  ADD COLUMN swap_direction ENUM('in', 'out') NULL COMMENT 'NULL=not a swap, in=received, out=sent';

-- Index for swap queries
CREATE INDEX idx_guide_swap ON tx_guide(swap_direction) WHERE swap_direction IS NOT NULL;
```

### Column Mapping

| New Column | Source | Description |
|------------|--------|-------------|
| `dex` | `tx_swap` via `tx_program` | DEX program name |
| `pool_address_id` | `tx_swap.pool_address_id` | Pool/AMM address |
| `pool_label` | `tx_swap` or derived | Human-readable pool name |
| `swap_direction` | Derived from `tx_swap` | 'in' for token_1, 'out' for token_2 |

---

## Loader Changes (sp_tx_guide_loader)

### Current Flow:
1. INSERT transfers (source_id=1)
2. INSERT swap_in (source_id=2)
3. INSERT swap_out (source_id=2)
4. UPDATE balances
5. UPDATE tax detection

### New Flow:
1. INSERT transfers (source_id=1) - unchanged
2. **UPDATE transfers with swap metadata** (instead of INSERT)
   - Match transfer to swap by: `tx_id + token_id + amount`
   - Set: `dex`, `pool_address_id`, `pool_label`, `swap_direction`
   - Change `edge_type_id` from `spl_transfer` to `swap_in`/`swap_out`
3. UPDATE balances - unchanged
4. UPDATE tax detection - unchanged

### Matching Logic

```sql
-- Enrich transfers that are part of swaps (token received = swap_in)
UPDATE tx_guide g
JOIN tx_swap s ON s.tx_id = g.tx_id
  AND s.token_1_id = g.token_id
  AND s.amount_1 = g.amount
JOIN tx_program p ON p.id = s.program_id
SET
  g.dex = p.name,
  g.pool_address_id = s.pool_address_id,
  g.swap_direction = 'in',
  g.edge_type_id = (SELECT id FROM tx_guide_type WHERE type_code = 'swap_in')
WHERE g.tx_id >= v_start AND g.tx_id < v_end
  AND g.source_id = 1
  AND g.edge_type_id = (SELECT id FROM tx_guide_type WHERE type_code = 'spl_transfer');

-- Enrich transfers that are part of swaps (token sent = swap_out)
UPDATE tx_guide g
JOIN tx_swap s ON s.tx_id = g.tx_id
  AND s.token_2_id = g.token_id
  AND s.amount_2 = g.amount
JOIN tx_program p ON p.id = s.program_id
SET
  g.dex = p.name,
  g.pool_address_id = s.pool_address_id,
  g.swap_direction = 'out',
  g.edge_type_id = (SELECT id FROM tx_guide_type WHERE type_code = 'swap_out')
WHERE g.tx_id >= v_start AND g.tx_id < v_end
  AND g.source_id = 1
  AND g.edge_type_id = (SELECT id FROM tx_guide_type WHERE type_code = 'spl_transfer');
```

### Remove Old Swap INSERTs

Delete the two separate INSERT blocks for swap_in and swap_out (lines ~117-178 in current loader).

---

## Migration for Existing Data

Need to:
1. Add new columns to tx_guide
2. Backfill swap metadata on existing source_id=1 records
3. Delete source_id=2 records (they become redundant)

```sql
-- Step 1: Add columns (see ALTER above)

-- Step 2: Backfill swap_in metadata
UPDATE tx_guide g
JOIN tx_swap s ON s.tx_id = g.tx_id
  AND s.token_1_id = g.token_id
  AND s.amount_1 = g.amount
JOIN tx_program p ON p.id = s.program_id
SET
  g.dex = p.name,
  g.pool_address_id = s.pool_address_id,
  g.swap_direction = 'in',
  g.edge_type_id = (SELECT id FROM tx_guide_type WHERE type_code = 'swap_in')
WHERE g.source_id = 1
  AND g.edge_type_id = (SELECT id FROM tx_guide_type WHERE type_code = 'spl_transfer');

-- Step 3: Backfill swap_out metadata
UPDATE tx_guide g
JOIN tx_swap s ON s.tx_id = g.tx_id
  AND s.token_2_id = g.token_id
  AND s.amount_2 = g.amount
JOIN tx_program p ON p.id = s.program_id
SET
  g.dex = p.name,
  g.pool_address_id = s.pool_address_id,
  g.swap_direction = 'out',
  g.edge_type_id = (SELECT id FROM tx_guide_type WHERE type_code = 'swap_out')
WHERE g.source_id = 1
  AND g.edge_type_id = (SELECT id FROM tx_guide_type WHERE type_code = 'spl_transfer');

-- Step 4: Delete redundant source_id=2 records
DELETE FROM tx_guide WHERE source_id = 2;
```

---

## Files to Modify

1. **tables/tx_guide.sql** - Add new columns
2. **migrations/YYYYMMDD_add_swap_columns.sql** - Migration script
3. **procedures/sp_tx_guide_loader.sql** - Update loader logic
4. **procedures/sp_tx_bmap_get.sql** - May need updates if it references source_id

---

## Verification Steps

1. Run migration on test database
2. Check that swap transfers now have `dex`, `pool_label`, `swap_direction` populated
3. Verify no source_id=2 records remain
4. Test sp_tx_guide_loader on new transactions
5. Verify TX Flow in bmap-viewer shows no duplicates
6. Verify actions panel shows correct DEX names

---

## Open Questions

1. **tx_swap table schema**: Need to verify column names for pool_address_id, program_id
2. **tx_program table**: Need to verify it has DEX names
3. **Pool label derivation**: Where does pool_label come from? Is it in tx_swap or derived?
4. **Matching precision**: Should we match on exact amount or allow small tolerance for fees?

---

## Context from Previous Session

- Working on bmap-viewer TX Flow visualization (Solscan-style)
- Committed changes: `13dc84b Add Solscan-style TX Flow visualization with actions panel`
- Database: `t16o_db` on `localhost:3396` (root/rootpassword)
- Current deduplication is done in JavaScript (bmap-viewer.html) but should be done at data layer
