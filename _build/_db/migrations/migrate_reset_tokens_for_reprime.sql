-- Migration: Reset enriched tokens for re-enrichment + auto-prime
-- Targets tokens that have token_json (Solscan metadata cached)
-- 1. Nulls out parsed fields so enricher re-claims them
-- 2. Resets attempt_cnt so they pass the max_attempts filter
-- 3. Deletes tx_guide records so auto-prime fires on re-enrichment
--
-- Safe: token_json is preserved — enricher will re-parse from API/cache

-- Step 1: Collect token IDs that have token_json
DROP TEMPORARY TABLE IF EXISTS tmp_reprime_tokens;
CREATE TEMPORARY TABLE tmp_reprime_tokens (
    token_id BIGINT PRIMARY KEY
) ENGINE=MEMORY;

INSERT INTO tmp_reprime_tokens (token_id)
SELECT id FROM tx_token WHERE token_json IS NOT NULL;

SELECT COUNT(*) AS tokens_to_reset FROM tmp_reprime_tokens;

-- Step 2: Reset enriched fields + attempt_cnt
UPDATE tx_token
SET token_symbol = NULL,
    token_name = NULL,
    token_icon = NULL,
    decimals = NULL,
    supply = NULL,
    token_type = NULL,
    attempt_cnt = 0,
    primed = 0
WHERE id IN (SELECT token_id FROM tmp_reprime_tokens);

SELECT ROW_COUNT() AS tokens_reset;

-- Step 3: Delete tx_guide records for those tokens
DELETE g FROM tx_guide g
JOIN tmp_reprime_tokens t ON t.token_id = g.token_id;

SELECT ROW_COUNT() AS guide_records_deleted;

-- Cleanup
DROP TEMPORARY TABLE IF EXISTS tmp_reprime_tokens;
