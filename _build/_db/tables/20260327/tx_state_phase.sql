-- tx_state_phase: Reference table for tx_state bitmask values
-- Each row defines a processing phase with its bit position and value
--
-- Usage:
--   SELECT bit_value FROM tx_state_phase WHERE phase_code = 'SHREDDED';
--   UPDATE tx SET tx_state = tx_state | 1 WHERE ...;  -- Set SHREDDED bit
--   SELECT * FROM tx WHERE tx_state & 2 = 0;          -- Find not DECODED

DROP TABLE IF EXISTS tx_state_phase;

CREATE TABLE tx_state_phase (
    bit_position    TINYINT UNSIGNED NOT NULL PRIMARY KEY,
    bit_value       BIGINT UNSIGNED NOT NULL,
    phase_code      VARCHAR(32) NOT NULL UNIQUE,
    phase_name      VARCHAR(64) NOT NULL,
    description     VARCHAR(255),
    worker_name     VARCHAR(64),
    created_utc     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Insert phase definitions
INSERT INTO tx_state_phase (bit_position, bit_value, phase_code, phase_name, description, worker_name) VALUES
(0,  1,    'SHREDDED',           'Shredded',             'Basic tx data inserted (signature, block_time, slot, fee)', 'guide-shredder.py'),
(1,  2,    'DECODED',            'Decoded',              'Solscan decoded actions fetched and stored in tx_json', 'guide-shredder.py'),
(2,  4,    'GUIDE_EDGES',        'Guide Edges',          'tx_guide edges created (transfers, swaps, activities)', 'guide-shredder.py'),
(3,  8,    'ADDRESSES_QUEUED',   'Addresses Queued',     'New addresses sent to funding queue', 'guide-shredder.py'),
(4,  16,   'SWAPS_PARSED',       'Swaps Parsed',         'tx_swap records created from decoded swaps', 'guide-shredder.py'),
(5,  32,   'TRANSFERS_PARSED',   'Transfers Parsed',     'tx_transfer records created from decoded transfers', 'guide-shredder.py'),
(6,  64,   'DETAILED',           'Detailed',             'Detail enrichment complete (guide-detailer)', 'guide-detailer.py'),
(7,  128,  'TOKENS_ENRICHED',    'Tokens Enriched',      'All token metadata fetched', 'guide-backfill-tokens.py'),
(8,  256,  'POOLS_ENRICHED',     'Pools Enriched',       'All pool/AMM data fetched', 'guide-pool-enricher.py'),
(9,  512,  'FUNDING_COMPLETE',   'Funding Complete',     'All addresses have funding info resolved', 'guide-funder.py'),
(10, 1024, 'CLASSIFIED',         'Classified',           'Addresses classified (wallet, dex, cex, etc.)', 'guide-address-classifier.py');

-- Composite state values for convenience
-- FULLY_SHREDDED = 1+2+4+8+16+32 = 63 (all shredder phases)
-- FULLY_PROCESSED = 2047 (all phases complete)
