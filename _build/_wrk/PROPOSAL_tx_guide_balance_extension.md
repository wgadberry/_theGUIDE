# Proposal: Extend tx_guide with Post-Balance Columns

## Goal
Deprecate `tx_bmap_state` table and source bubblemap balance data directly from `tx_guide`.

## Current State

### tx_guide table (35,789 rows)
```
id, tx_id, block_time, from_address_id, to_address_id,
from_token_account_id, to_token_account_id, token_id, amount, decimals,
edge_type_id, source_id, source_row_id, ins_index, fee, priority_fee
```

### tx_bmap_state table (23,523 rows) - TO BE DEPRECATED
- Computes running balance deltas from tx_guide
- Problem: Arbitrage wallets show 0 balance (in/out cancel)
- Already bypassed in sp_tx_bmap_get_token_state (now uses tx_token_balance_change)

### Balance source tables
- `tx_token_balance_change` (43,560 rows): Actual blockchain token balances
- `tx_sol_balance_change` (44,212 rows): Actual blockchain SOL balances

---

## Proposed Schema Changes

### Add 4 new columns to tx_guide:

```sql
ALTER TABLE tx_guide
ADD COLUMN from_token_post_balance DECIMAL(38,0) NULL AFTER decimals,
ADD COLUMN to_token_post_balance DECIMAL(38,0) NULL AFTER from_token_post_balance,
ADD COLUMN from_sol_post_balance BIGINT UNSIGNED NULL AFTER to_token_post_balance,
ADD COLUMN to_sol_post_balance BIGINT UNSIGNED NULL AFTER from_sol_post_balance;
```

| Column | Type | Description |
|--------|------|-------------|
| `from_token_post_balance` | DECIMAL(38,0) | Token balance of from_address AFTER this tx |
| `to_token_post_balance` | DECIMAL(38,0) | Token balance of to_address AFTER this tx |
| `from_sol_post_balance` | BIGINT UNSIGNED | SOL balance (lamports) of from_address AFTER this tx |
| `to_sol_post_balance` | BIGINT UNSIGNED | SOL balance (lamports) of to_address AFTER this tx |

**Note:** Balances stored in raw units (need decimals for human-readable):
- Token: `post_balance / POW(10, decimals)`
- SOL: `post_balance / 1e9`

---

## Procedure Modifications

### 1. sp_tx_guide_batch

**Current INSERT for swaps:**
```sql
INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                      token_id, amount, decimals, edge_type_id, ...)
SELECT s.tx_id, b.block_time, s.account_address_id, tk.mint_address_id,
       s.token_1_id, s.amount_1, tk.decimals, 3, ...
```

**Modified INSERT (add balance lookups):**
```sql
INSERT INTO tx_guide (tx_id, block_time, from_address_id, to_address_id,
                      token_id, amount, decimals,
                      from_token_post_balance, to_token_post_balance,
                      from_sol_post_balance, to_sol_post_balance,
                      edge_type_id, ...)
SELECT s.tx_id, b.block_time, s.account_address_id, tk.mint_address_id,
       s.token_1_id, s.amount_1, tk.decimals,
       -- Token balance for from_address (account doing the swap)
       (SELECT tbc.post_balance
        FROM tx_token_balance_change tbc
        WHERE tbc.tx_id = s.tx_id
          AND tbc.owner_address_id = s.account_address_id
          AND tbc.token_id = s.token_1_id
        LIMIT 1),
       -- Token balance for to_address (mint - usually 0 or pool)
       (SELECT tbc.post_balance
        FROM tx_token_balance_change tbc
        WHERE tbc.tx_id = s.tx_id
          AND tbc.owner_address_id = tk.mint_address_id
          AND tbc.token_id = s.token_1_id
        LIMIT 1),
       -- SOL balance for from_address
       (SELECT sbc.post_balance
        FROM tx_sol_balance_change sbc
        WHERE sbc.tx_id = s.tx_id
          AND sbc.address_id = s.account_address_id
        LIMIT 1),
       -- SOL balance for to_address
       (SELECT sbc.post_balance
        FROM tx_sol_balance_change sbc
        WHERE sbc.tx_id = s.tx_id
          AND sbc.address_id = tk.mint_address_id
        LIMIT 1),
       3, ...
```

### 2. sp_tx_guide_backfill

Same pattern - add subqueries for balance lookups in each INSERT statement.

### 3. sp_tx_bmap_get_token_state

**Current (already modified):**
```sql
UPDATE tmp_nodes n
SET balance = COALESCE((
    SELECT tbc.post_balance / POW(10, tbc.decimals)
    FROM tx_token_balance_change tbc
    JOIN tx t ON t.id = tbc.tx_id
    WHERE tbc.token_id = v_token_id
      AND tbc.owner_address_id = n.address_id
      AND t.block_time <= v_block_time
    ORDER BY t.block_time DESC, tbc.id DESC
    LIMIT 1
), 0);
```

**New (read directly from tx_guide):**
```sql
UPDATE tmp_nodes n
SET balance = COALESCE((
    SELECT CASE
        WHEN g.from_address_id = n.address_id THEN g.from_token_post_balance
        WHEN g.to_address_id = n.address_id THEN g.to_token_post_balance
    END / POW(10, g.decimals)
    FROM tx_guide g
    WHERE g.token_id = v_token_id
      AND (g.from_address_id = n.address_id OR g.to_address_id = n.address_id)
      AND g.block_time <= v_block_time
    ORDER BY g.block_time DESC, g.id DESC
    LIMIT 1
), 0);
```

---

## Backfill Strategy

For existing tx_guide rows, run a one-time UPDATE:

```sql
-- Backfill token balances
UPDATE tx_guide g
LEFT JOIN tx_token_balance_change tbc_from
    ON tbc_from.tx_id = g.tx_id
    AND tbc_from.owner_address_id = g.from_address_id
    AND tbc_from.token_id = g.token_id
LEFT JOIN tx_token_balance_change tbc_to
    ON tbc_to.tx_id = g.tx_id
    AND tbc_to.owner_address_id = g.to_address_id
    AND tbc_to.token_id = g.token_id
SET g.from_token_post_balance = tbc_from.post_balance,
    g.to_token_post_balance = tbc_to.post_balance
WHERE g.from_token_post_balance IS NULL;

-- Backfill SOL balances
UPDATE tx_guide g
LEFT JOIN tx_sol_balance_change sbc_from
    ON sbc_from.tx_id = g.tx_id
    AND sbc_from.address_id = g.from_address_id
LEFT JOIN tx_sol_balance_change sbc_to
    ON sbc_to.tx_id = g.tx_id
    AND sbc_to.address_id = g.to_address_id
SET g.from_sol_post_balance = sbc_from.post_balance,
    g.to_sol_post_balance = sbc_to.post_balance
WHERE g.from_sol_post_balance IS NULL;
```

---

## Implementation Steps

1. **ALTER TABLE** - Add 4 new columns to tx_guide
2. **Backfill** - Populate balances for existing rows (~35K rows)
3. **Update sp_tx_guide_batch** - Include balance lookups in INSERTs
4. **Update sp_tx_guide_backfill** - Include balance lookups in INSERTs
5. **Update sp_tx_bmap_get_token_state** - Read from tx_guide instead of tx_token_balance_change
6. **Test** - Verify bmap viewer shows correct balances
7. **Deprecate** - Remove tx_bmap_state sync from guide-aggregator.py
8. **Drop** - Eventually DROP TABLE tx_bmap_state

---

## Advantages

1. **Single source of truth** - tx_guide has everything needed for bmap
2. **No sync lag** - Balances populated at insert time, not computed later
3. **Simpler queries** - No joins to balance change tables in bmap SP
4. **Accurate balances** - Uses actual blockchain-reported values
5. **Less storage** - Eliminates redundant tx_bmap_state table

## Risks/Considerations

1. **Larger tx_guide rows** - +34 bytes per row (4 new columns)
2. **Slower INSERTs** - Subqueries for balance lookups add overhead
3. **NULL handling** - Some addresses may not have balance change records

---

## Questions Before Proceeding

1. Should we add indexes on the new balance columns?
2. Run backfill in batches to avoid locking?
3. Keep tx_bmap_state temporarily for comparison testing?

**Ready to implement on your approval.**
