-- Migration: Add 'daemon' to tx_request_log source enum
-- Created: 2026-01-30
--
-- Adds 'daemon' as a valid source type for daemon worker billing tracking
-- (funder daemon, aggregator daemon, etc.)
--
-- Run with: mysql -h 127.0.0.1 -P 3396 -u root -p t16o_db < migrate_add_daemon_source.sql

SELECT 'Adding daemon to source enum...' AS status;

ALTER TABLE tx_request_log
MODIFY COLUMN source ENUM('rest','queue','cascade','scheduler','daemon') NOT NULL;

SELECT 'Verifying change...' AS status;
SHOW COLUMNS FROM tx_request_log WHERE Field = 'source';

SELECT 'Migration complete!' AS status;
