-- Migration: Fix misclassified address_type values in tx_address
-- Created: 2026-01-30
--
-- This migration corrects addresses that were incorrectly classified during shredding:
--   - ATAs that are actually pool vaults (token_account_X_X fields)
--   - Wallets that are actually pools (owner_2 with amm_id context)
--   - Wallets that appear in tx_pool as pool_address_id
--   - ATAs/Wallets that are pool vault accounts (token_account1_id, token_account2_id)
--
-- Run with: mysql -h 127.0.0.1 -P 3396 -u root -p t16o_db < migrate_fix_address_types.sql

-- ============================================================================
-- PRE-MIGRATION: Show current state
-- ============================================================================
SELECT 'Pre-migration address_type distribution:' AS status;
SELECT address_type, COUNT(*) as count
FROM tx_address
GROUP BY address_type
ORDER BY count DESC;

-- ============================================================================
-- FIX 1: Wallets that are in tx_pool -> pool
-- These are addresses that appear as pool_address_id in tx_pool
-- ============================================================================
SELECT 'FIX 1: Reclassifying wallets in tx_pool as pools...' AS status;

UPDATE tx_address a
JOIN tx_pool p ON p.pool_address_id = a.id
SET a.address_type = 'pool'
WHERE a.address_type = 'wallet';

SELECT ROW_COUNT() AS 'wallets_to_pools';

-- ============================================================================
-- FIX 2: ATAs that are in tx_pool -> pool (edge case)
-- ============================================================================
SELECT 'FIX 2: Reclassifying ATAs in tx_pool as pools...' AS status;

UPDATE tx_address a
JOIN tx_pool p ON p.pool_address_id = a.id
SET a.address_type = 'pool'
WHERE a.address_type = 'ata';

SELECT ROW_COUNT() AS 'atas_to_pools';

-- ============================================================================
-- FIX 3: ATAs/Wallets that are pool vault accounts -> vault
-- token_account1_id and token_account2_id in tx_pool are vault addresses
-- ============================================================================
SELECT 'FIX 3: Reclassifying pool token accounts as vaults...' AS status;

UPDATE tx_address a
SET a.address_type = 'vault'
WHERE a.address_type IN ('ata', 'wallet')
  AND (
    a.id IN (SELECT token_account1_id FROM tx_pool WHERE token_account1_id IS NOT NULL)
    OR a.id IN (SELECT token_account2_id FROM tx_pool WHERE token_account2_id IS NOT NULL)
  );

SELECT ROW_COUNT() AS 'to_vaults';

-- ============================================================================
-- FIX 4: Wallets in tx_token -> mint
-- Addresses that appear as mint_address_id should be mints
-- ============================================================================
SELECT 'FIX 4: Reclassifying wallets in tx_token as mints...' AS status;

UPDATE tx_address a
JOIN tx_token t ON t.mint_address_id = a.id
SET a.address_type = 'mint'
WHERE a.address_type = 'wallet';

SELECT ROW_COUNT() AS 'wallets_to_mints';

-- ============================================================================
-- FIX 5: ATAs in tx_token -> mint (edge case)
-- ============================================================================
SELECT 'FIX 5: Reclassifying ATAs in tx_token as mints...' AS status;

UPDATE tx_address a
JOIN tx_token t ON t.mint_address_id = a.id
SET a.address_type = 'mint'
WHERE a.address_type = 'ata';

SELECT ROW_COUNT() AS 'atas_to_mints';

-- ============================================================================
-- FIX 6: Wallets in tx_program -> program
-- Addresses that appear as program_address_id should be programs
-- ============================================================================
SELECT 'FIX 6: Reclassifying wallets in tx_program as programs...' AS status;

UPDATE tx_address a
JOIN tx_program p ON p.program_address_id = a.id
SET a.address_type = 'program'
WHERE a.address_type = 'wallet';

SELECT ROW_COUNT() AS 'wallets_to_programs';

-- ============================================================================
-- FIX 7: Pattern-based fixes for known address patterns
-- ============================================================================
SELECT 'FIX 7: Pattern-based reclassifications...' AS status;

-- Addresses ending in 'pump' are typically pump.fun mints
UPDATE tx_address
SET address_type = 'mint'
WHERE address_type = 'wallet'
  AND address LIKE '%pump';

SELECT ROW_COUNT() AS 'pump_to_mints';

-- ============================================================================
-- FIX 8: Reclassify transfer source/destination that belong to pools
-- If an ATA's owner (via tx_transfer) is a pool, the ATA is a vault
-- ============================================================================
SELECT 'FIX 8: Reclassifying ATAs owned by pools as vaults...' AS status;

-- Find ATAs where the source_owner or destination_owner is a pool
UPDATE tx_address a
SET a.address_type = 'vault'
WHERE a.address_type = 'ata'
  AND a.id IN (
    -- Source addresses where source_owner is a pool
    SELECT DISTINCT t.source_address_id
    FROM tx_transfer t
    JOIN tx_address owner_addr ON owner_addr.id = t.source_owner_address_id
    WHERE owner_addr.address_type = 'pool'
      AND t.source_address_id IS NOT NULL

    UNION

    -- Destination addresses where destination_owner is a pool
    SELECT DISTINCT t.destination_address_id
    FROM tx_transfer t
    JOIN tx_address owner_addr ON owner_addr.id = t.destination_owner_address_id
    WHERE owner_addr.address_type = 'pool'
      AND t.destination_address_id IS NOT NULL
  );

SELECT ROW_COUNT() AS 'pool_owned_atas_to_vaults';

-- ============================================================================
-- FIX 9: Ensure tx_pool has entries for all pool addresses
-- ============================================================================
SELECT 'FIX 9: Ensuring tx_pool entries exist for all pools...' AS status;

INSERT IGNORE INTO tx_pool (pool_address_id)
SELECT a.id
FROM tx_address a
WHERE a.address_type = 'pool'
  AND NOT EXISTS (SELECT 1 FROM tx_pool p WHERE p.pool_address_id = a.id);

SELECT ROW_COUNT() AS 'new_tx_pool_entries';

-- ============================================================================
-- POST-MIGRATION: Show final state
-- ============================================================================
SELECT 'Post-migration address_type distribution:' AS status;
SELECT address_type, COUNT(*) as count
FROM tx_address
GROUP BY address_type
ORDER BY count DESC;

SELECT 'Migration complete!' AS status;
