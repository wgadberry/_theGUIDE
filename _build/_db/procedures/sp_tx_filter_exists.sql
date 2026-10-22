-- sp_tx_filter_exists
--
-- Given a JSON array of transaction signatures, return those that do NOT
-- already exist in the tx table.  Used by holder discovery to avoid
-- re-submitting known transactions to the pipeline.
--
-- Input:  p_signatures_json — JSON array of signature strings
--         e.g. '["sig1","sig2","sig3"]'
--
-- Output: Single-column result set of non-existing signatures.
--         Also sets @filter_total and @filter_new for caller diagnostics.
--
-- Usage:
--   CALL sp_tx_filter_exists('["VGUAv...","2FbbY..."]');

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_tx_filter_exists$$

CREATE PROCEDURE sp_tx_filter_exists(
    IN p_signatures_json JSON
)
BEGIN
    DECLARE v_total INT DEFAULT 0;
    DECLARE v_new   INT DEFAULT 0;

    -- ── Step 1: Parse JSON array into temp table ─────────────────
    DROP TEMPORARY TABLE IF EXISTS tmp_filter_sigs;
    CREATE TEMPORARY TABLE tmp_filter_sigs (
        signature VARCHAR(128) NOT NULL,
        PRIMARY KEY (signature)
    ) ENGINE=MEMORY;

    INSERT IGNORE INTO tmp_filter_sigs (signature)
    SELECT jt.sig
    FROM JSON_TABLE(
        p_signatures_json,
        '$[*]' COLUMNS (sig VARCHAR(128) PATH '$')
    ) AS jt
    WHERE jt.sig IS NOT NULL AND jt.sig != '';

    SET v_total = ROW_COUNT();

    -- ── Step 2: Count new and return signatures not in tx ──────
    SELECT COUNT(*) INTO v_new
    FROM tmp_filter_sigs t
    LEFT JOIN tx ON tx.signature = t.signature
    WHERE tx.signature IS NULL;

    -- Diagnostics for caller
    SET @filter_total = v_total;
    SET @filter_new   = v_new;

    -- Result set
    SELECT t.signature
    FROM tmp_filter_sigs t
    LEFT JOIN tx ON tx.signature = t.signature
    WHERE tx.signature IS NULL;

    DROP TEMPORARY TABLE IF EXISTS tmp_filter_sigs;
END$$

DELIMITER ;
