-- Migration: Add token_json column to tx_token
-- Created: 2026-01-31
--
-- Stores the complete JSON response from token-metadata API calls
--
-- Run with: mysql -h 127.0.0.1 -P 3396 -u root -p t16o_db < migrate_add_token_json.sql

SELECT 'Adding token_json column to tx_token...' AS status;

ALTER TABLE tx_token
ADD COLUMN token_json JSON DEFAULT NULL AFTER attempt_cnt;

SELECT 'Verifying change...' AS status;
SHOW COLUMNS FROM tx_token WHERE Field = 'token_json';

SELECT 'Migration complete!' AS status;
