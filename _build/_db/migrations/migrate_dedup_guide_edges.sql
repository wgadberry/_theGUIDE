-- Deduplicate tx_guide: tighten unique constraint, then batch-delete dupes
--
-- Step 1: ALTER TABLE first — add generated column, replace unique key
--         This will fail on any existing exact dupes (same tx_id, from, to, token_id_uq, amount, edge_type_id)
--         so we must delete dupes BEFORE the ALTER. But ALTER is expensive on large tables,
--         so we: (a) batch-delete dupes, (b) ALTER to add constraint.

-- ============================================================
-- Phase 1: Batch-delete duplicate rows (keep lowest id per group)
-- ============================================================

-- Build delete target list
DROP TEMPORARY TABLE IF EXISTS tmp_delete_ids;
CREATE TEMPORARY TABLE tmp_delete_ids (
    id BIGINT UNSIGNED PRIMARY KEY
) ENGINE=MEMORY;

INSERT INTO tmp_delete_ids
SELECT g.id
FROM tx_guide g
JOIN (
    SELECT tx_id, from_address_id, to_address_id,
           COALESCE(token_id, 0) AS token_id_safe, amount, edge_type_id,
           MIN(id) AS keep_id
    FROM tx_guide
    GROUP BY tx_id, from_address_id, to_address_id,
             COALESCE(token_id, 0), amount, edge_type_id
    HAVING COUNT(*) > 1
) dupes ON g.tx_id = dupes.tx_id
    AND g.from_address_id = dupes.from_address_id
    AND g.to_address_id = dupes.to_address_id
    AND COALESCE(g.token_id, 0) = dupes.token_id_safe
    AND g.amount = dupes.amount
    AND g.edge_type_id = dupes.edge_type_id
    AND g.id != dupes.keep_id;

SELECT COUNT(*) AS rows_to_delete FROM tmp_delete_ids;

-- Batch delete: 5000 rows per iteration to avoid long locks
-- Run this block repeatedly until 0 rows affected:
DELETE g FROM tx_guide g
JOIN (SELECT id FROM tmp_delete_ids LIMIT 5000) batch ON batch.id = g.id;
DELETE FROM tmp_delete_ids LIMIT 5000;

-- (repeat above two lines until tmp_delete_ids is empty)

-- Verify clean
SELECT COUNT(*) AS remaining FROM tmp_delete_ids;
DROP TEMPORARY TABLE IF EXISTS tmp_delete_ids;

-- ============================================================
-- Phase 2: Tighten unique constraint (run after all deletes complete)
-- ============================================================
ALTER TABLE tx_guide
    ADD COLUMN token_id_uq BIGINT GENERATED ALWAYS AS (COALESCE(token_id, 0)) STORED,
    DROP INDEX uq_guide_edge,
    ADD UNIQUE KEY uq_guide_edge (tx_id, from_address_id, to_address_id, token_id_uq, amount, edge_type_id);
