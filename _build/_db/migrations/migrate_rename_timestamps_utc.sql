-- Migration: Standardize all timestamp columns to created_utc / updated_utc
-- Date: 2026-02-16
-- Reason: Consistent naming convention across all tables
-- Status: APPLIED

-- ============================================================
-- config: created_at → created_utc, updated_at → updated_utc
-- Also rename index idx_updated_at → idx_updated_utc
-- ============================================================
ALTER TABLE config RENAME COLUMN created_at TO created_utc;
ALTER TABLE config RENAME COLUMN updated_at TO updated_utc;
ALTER TABLE config DROP INDEX idx_updated_at;
ALTER TABLE config ADD INDEX idx_updated_utc (updated_utc);

-- ============================================================
-- tx: created_at → created_utc
-- ============================================================
ALTER TABLE tx RENAME COLUMN created_at TO created_utc;

-- ============================================================
-- tx_api_key: created_at → created_utc
-- ============================================================
ALTER TABLE tx_api_key RENAME COLUMN created_at TO created_utc;

-- ============================================================
-- tx_request_log: created_at → created_utc
-- Also rename index idx_created_at → idx_created_utc
-- ============================================================
ALTER TABLE tx_request_log RENAME COLUMN created_at TO created_utc;
ALTER TABLE tx_request_log DROP INDEX idx_created_at;
ALTER TABLE tx_request_log ADD INDEX idx_created_utc (created_utc);

-- ============================================================
-- tx_token_participant: created_at → created_utc, updated_at → updated_utc
-- ============================================================
ALTER TABLE tx_token_participant RENAME COLUMN created_at TO created_utc;
ALTER TABLE tx_token_participant RENAME COLUMN updated_at TO updated_utc;

-- ============================================================
-- tx_state_phase: created_at → created_utc
-- ============================================================
ALTER TABLE tx_state_phase RENAME COLUMN created_at TO created_utc;

-- ============================================================
-- tx_pool: updated_at → updated_utc (created_utc already exists)
-- ============================================================
ALTER TABLE tx_pool RENAME COLUMN updated_at TO updated_utc;

-- ============================================================
-- tx_program: updated_at → updated_utc (created_utc already exists)
-- ============================================================
ALTER TABLE tx_program RENAME COLUMN updated_at TO updated_utc;

-- ============================================================
-- tx_token: updated_at → updated_utc (created_utc already exists)
-- ============================================================
ALTER TABLE tx_token RENAME COLUMN updated_at TO updated_utc;

-- ============================================================
-- Tables not in live DB (schema-only, apply when created):
-- tx_account: updated_at → updated_utc
-- tx_party: created_at → created_utc, updated_at → updated_utc
-- tx_funding_edge: created_at → created_utc, updated_at → updated_utc
-- tx_token_holder: updated_at → updated_utc
-- tx_token_market: updated_at → updated_utc
-- ============================================================
