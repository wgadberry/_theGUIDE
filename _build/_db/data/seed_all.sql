-- theGuide Seed Data - Master Script
-- Loads all lookup/reference data in dependency order
--
-- Usage:
--   mysql -h localhost -P 3396 -u root -p t16o_db < seed_all.sql
--   OR run via: python 02-create-schema.py --seed

-- =============================================================================
-- LOOKUP TABLES (order matters for foreign keys)
-- =============================================================================

-- 1. Guide type classifications (no dependencies)
SOURCE data/seed_tx_guide_type.sql;

-- 2. Guide source classifications (no dependencies)
SOURCE data/seed_tx_guide_source.sql;

-- 3. Configuration defaults (no dependencies)
SOURCE data/seed_config.sql;

-- 4. Known Solana programs (depends on tx_address)
SOURCE data/seed_tx_program.sql;

-- =============================================================================
-- VERIFICATION
-- =============================================================================
SELECT 'Seed data loaded!' AS status;
SELECT
    (SELECT COUNT(*) FROM tx_guide_type) AS guide_types,
    (SELECT COUNT(*) FROM tx_guide_source) AS guide_sources,
    (SELECT COUNT(*) FROM config) AS config_rows,
    (SELECT COUNT(*) FROM tx_program WHERE name IS NOT NULL) AS named_programs;
