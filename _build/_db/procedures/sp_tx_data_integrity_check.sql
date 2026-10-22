-- ============================================================================
-- DATA INTEGRITY CHECK FOR tx_guide PIPELINE
-- ============================================================================
--
-- Checks data flow from source tables (tx_activity, tx_swap, tx_transfer)
-- into derived tables (tx_guide, tx_token_participant)
--
-- Usage:
--   mysql -h localhost -P 3396 -u root -p t16o_db < sp_tx_data_integrity_check.sql
--
-- Or run individual sections as needed.
-- ============================================================================

-- ============================================================================
-- 1. ROW COUNTS - Baseline
-- ============================================================================
SELECT '=== 1. ROW COUNTS ===' as section;

SELECT 'tx_activity' as tbl, COUNT(*) as cnt FROM tx_activity
UNION ALL SELECT 'tx_swap', COUNT(*) FROM tx_swap
UNION ALL SELECT 'tx_transfer', COUNT(*) FROM tx_transfer
UNION ALL SELECT 'tx_guide', COUNT(*) FROM tx_guide
UNION ALL SELECT 'tx_token_participant', COUNT(*) FROM tx_token_participant;


-- ============================================================================
-- 2. SOURCE -> tx_guide COVERAGE
-- ============================================================================
SELECT '=== 2. SOURCE -> tx_guide COVERAGE ===' as section;

-- tx_transfer coverage
SELECT
    'tx_transfer total' as metric, COUNT(*) as cnt FROM tx_transfer
UNION ALL
SELECT 'tx_transfer in tx_guide', COUNT(DISTINCT t.id)
FROM tx_transfer t
JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
UNION ALL
SELECT 'tx_transfer MISSING', COUNT(*)
FROM tx_transfer t
LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
WHERE g.id IS NULL;

-- tx_swap coverage
SELECT
    'tx_swap total' as metric, COUNT(*) as cnt FROM tx_swap
UNION ALL
SELECT 'tx_swap in tx_guide', COUNT(DISTINCT s.id)
FROM tx_swap s
JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
UNION ALL
SELECT 'tx_swap MISSING', COUNT(*)
FROM tx_swap s
LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
WHERE g.id IS NULL;

-- tx_activity guide_loaded status
SELECT
    'tx_activity total' as metric, COUNT(*) as cnt FROM tx_activity
UNION ALL
SELECT 'tx_activity guide_loaded=1', COUNT(*) FROM tx_activity WHERE guide_loaded = 1
UNION ALL
SELECT 'tx_activity guide_loaded=0 (pending)', COUNT(*) FROM tx_activity WHERE guide_loaded = 0 OR guide_loaded IS NULL;


-- ============================================================================
-- 3. MISSING tx_transfer BREAKDOWN
-- ============================================================================
SELECT '=== 3. MISSING tx_transfer BREAKDOWN ===' as section;

SELECT t.transfer_type, COUNT(*) as missing_cnt,
    SUM(CASE WHEN t.source_owner_address_id IS NULL THEN 1 ELSE 0 END) as null_source,
    SUM(CASE WHEN t.destination_owner_address_id IS NULL THEN 1 ELSE 0 END) as null_dest
FROM tx_transfer t
LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
WHERE g.id IS NULL
GROUP BY t.transfer_type
ORDER BY missing_cnt DESC;


-- ============================================================================
-- 4. MISSING tx_swap BREAKDOWN
-- ============================================================================
SELECT '=== 4. MISSING tx_swap BREAKDOWN ===' as section;

SELECT
    s.activity_type,
    SUM(CASE WHEN s.account_address_id IS NULL THEN 1 ELSE 0 END) as null_account,
    SUM(CASE WHEN s.token_1_id IS NULL THEN 1 ELSE 0 END) as null_token1,
    SUM(CASE WHEN s.token_2_id IS NULL THEN 1 ELSE 0 END) as null_token2,
    COUNT(*) as missing_cnt
FROM tx_swap s
LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
WHERE g.id IS NULL
GROUP BY s.activity_type
ORDER BY missing_cnt DESC;


-- ============================================================================
-- 5. ORPHAN DETECTION
-- ============================================================================
SELECT '=== 5. ORPHAN DETECTION ===' as section;

SELECT 'tx_swap without tx_activity' as check_name, COUNT(*) as cnt
FROM tx_swap s
LEFT JOIN tx_activity a ON a.id = s.activity_id
WHERE s.activity_id IS NOT NULL AND a.id IS NULL
UNION ALL
SELECT 'tx_transfer without tx_activity', COUNT(*)
FROM tx_transfer t
LEFT JOIN tx_activity a ON a.id = t.activity_id
WHERE t.activity_id IS NOT NULL AND a.id IS NULL
UNION ALL
SELECT 'tx_guide invalid source_row_id (source=1)', COUNT(*)
FROM tx_guide g
LEFT JOIN tx_transfer t ON t.id = g.source_row_id
WHERE g.source_id = 1 AND t.id IS NULL
UNION ALL
SELECT 'tx_guide invalid source_row_id (source=2)', COUNT(*)
FROM tx_guide g
LEFT JOIN tx_swap s ON s.id = g.source_row_id
WHERE g.source_id = 2 AND s.id IS NULL;


-- ============================================================================
-- 6. REFERENTIAL INTEGRITY
-- ============================================================================
SELECT '=== 6. REFERENTIAL INTEGRITY ===' as section;

SELECT 'tx_guide.token_id not in tx_token' as check_name, COUNT(*) as cnt
FROM tx_guide g
LEFT JOIN tx_token t ON t.id = g.token_id
WHERE g.token_id IS NOT NULL AND t.id IS NULL
UNION ALL
SELECT 'tx_guide.from_address_id not in tx_address', COUNT(*)
FROM tx_guide g
LEFT JOIN tx_address a ON a.id = g.from_address_id
WHERE g.from_address_id IS NOT NULL AND a.id IS NULL
UNION ALL
SELECT 'tx_guide.to_address_id not in tx_address', COUNT(*)
FROM tx_guide g
LEFT JOIN tx_address a ON a.id = g.to_address_id
WHERE g.to_address_id IS NOT NULL AND a.id IS NULL
UNION ALL
SELECT 'tx_swap.token_1_id not in tx_token', COUNT(*)
FROM tx_swap s
LEFT JOIN tx_token t ON t.id = s.token_1_id
WHERE s.token_1_id IS NOT NULL AND t.id IS NULL
UNION ALL
SELECT 'tx_swap.token_2_id not in tx_token', COUNT(*)
FROM tx_swap s
LEFT JOIN tx_token t ON t.id = s.token_2_id
WHERE s.token_2_id IS NOT NULL AND t.id IS NULL
UNION ALL
SELECT 'tx_transfer.token_id not in tx_token', COUNT(*)
FROM tx_transfer tr
LEFT JOIN tx_token t ON t.id = tr.token_id
WHERE tr.token_id IS NOT NULL AND t.id IS NULL;


-- ============================================================================
-- 7. DUPLICATE DETECTION
-- ============================================================================
SELECT '=== 7. DUPLICATE DETECTION ===' as section;

SELECT COUNT(*) as dup_groups, SUM(cnt - 1) as extra_rows
FROM (
    SELECT tx_id, from_address_id, to_address_id, token_id, amount, edge_type_id, COUNT(*) as cnt
    FROM tx_guide
    GROUP BY tx_id, from_address_id, to_address_id, token_id, amount, edge_type_id
    HAVING COUNT(*) > 1
) dups;


-- ============================================================================
-- 8. DATA QUALITY - ZEROS AND NULLS
-- ============================================================================
SELECT '=== 8. DATA QUALITY ===' as section;

SELECT 'tx_guide with amount=0' as check_name, COUNT(*) as cnt
FROM tx_guide WHERE amount = 0
UNION ALL
SELECT 'tx_guide with NULL amount', COUNT(*)
FROM tx_guide WHERE amount IS NULL
UNION ALL
SELECT 'tx_swap with amount_1=0', COUNT(*)
FROM tx_swap WHERE amount_1 = 0
UNION ALL
SELECT 'tx_swap with amount_2=0', COUNT(*)
FROM tx_swap WHERE amount_2 = 0
UNION ALL
SELECT 'tx_transfer with amount=0', COUNT(*)
FROM tx_transfer WHERE amount = 0
UNION ALL
SELECT 'tx_guide NULL from_address_id', COUNT(*)
FROM tx_guide WHERE from_address_id IS NULL
UNION ALL
SELECT 'tx_guide NULL to_address_id', COUNT(*)
FROM tx_guide WHERE to_address_id IS NULL;


-- ============================================================================
-- 9. tx_guide EDGE TYPE DISTRIBUTION
-- ============================================================================
SELECT '=== 9. EDGE TYPE DISTRIBUTION ===' as section;

SELECT gt.type_code, gt.category, COUNT(*) as cnt
FROM tx_guide g
JOIN tx_guide_type gt ON gt.id = g.edge_type_id
GROUP BY g.edge_type_id, gt.type_code, gt.category
ORDER BY cnt DESC;


-- ============================================================================
-- 10. STUCK RECORDS (guide_loaded=1 but no tx_guide row)
-- ============================================================================
SELECT '=== 10. STUCK RECORDS ===' as section;

-- Transfers marked done but no edge
SELECT 'Transfers: guide_loaded=1 but no tx_guide' as check_name, COUNT(*) as cnt
FROM tx_transfer t
JOIN tx_activity a ON a.id = t.activity_id
LEFT JOIN tx_guide g ON g.source_id = 1 AND g.source_row_id = t.id
WHERE a.guide_loaded = 1
  AND g.id IS NULL
  AND t.transfer_type = 'ACTIVITY_SPL_TRANSFER'
  AND t.source_owner_address_id IS NOT NULL
  AND t.destination_owner_address_id IS NOT NULL;

-- Swaps marked done but no edge
SELECT 'Swaps: guide_loaded=1 but no tx_guide' as check_name, COUNT(*) as cnt
FROM tx_swap s
JOIN tx_activity a ON a.id = s.activity_id
LEFT JOIN tx_guide g ON g.source_id = 2 AND g.source_row_id = s.id
WHERE a.guide_loaded = 1
  AND g.id IS NULL
  AND s.account_address_id IS NOT NULL
  AND s.token_1_id IS NOT NULL
  AND s.token_2_id IS NOT NULL;


-- ============================================================================
-- 11. tx_token_participant COVERAGE
-- ============================================================================
SELECT '=== 11. tx_token_participant COVERAGE ===' as section;

SELECT
    'Unique token/wallet pairs in tx_guide' as metric,
    COUNT(DISTINCT CONCAT(token_id, '-', from_address_id)) +
    COUNT(DISTINCT CONCAT(token_id, '-', to_address_id)) as cnt
FROM tx_guide WHERE token_id IS NOT NULL
UNION ALL
SELECT 'tx_token_participant total', COUNT(*) FROM tx_token_participant;


-- ============================================================================
-- 12. INDEXES CHECK
-- ============================================================================
SELECT '=== 13. KEY INDEXES ===' as section;

SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'tx_guide'
  AND INDEX_NAME = 'uq_guide_edge'
ORDER BY SEQ_IN_INDEX;


-- ============================================================================
-- END OF INTEGRITY CHECK
-- ============================================================================
SELECT '=== INTEGRITY CHECK COMPLETE ===' as section;
