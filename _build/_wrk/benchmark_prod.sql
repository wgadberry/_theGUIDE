DELIMITER //

DROP PROCEDURE IF EXISTS bench_current //
CREATE PROCEDURE bench_current(IN p_json JSON)
BEGIN
    -- Simulates the 4 sections of sp_tx_prepopulate_lookups JSON parsing
    -- Section 1: core addresses (18 JSON_TABLE calls)
    INSERT IGNORE INTO tx_address (address, address_type)
    SELECT DISTINCT addr, addr_type FROM (
        SELECT t.signer_account AS addr, 'wallet' AS addr_type FROM JSON_TABLE(p_json, '$.data[*]' COLUMNS (signer_account VARCHAR(44) PATH '$.one_line_summary.data.account')) t WHERE t.signer_account IS NOT NULL AND t.signer_account != 'null'
        UNION SELECT t.fallback_signer, 'wallet' FROM JSON_TABLE(p_json, '$.data[*]' COLUMNS (fallback_signer VARCHAR(44) PATH '$.transfers[0].source_owner')) t WHERE t.fallback_signer IS NOT NULL AND t.fallback_signer != 'null'
        UNION SELECT t.token_1, 'mint' FROM JSON_TABLE(p_json, '$.data[*]' COLUMNS (token_1 VARCHAR(44) PATH '$.one_line_summary.data.token_1')) t WHERE t.token_1 IS NOT NULL AND t.token_1 != 'null'
        UNION SELECT t.token_2, 'mint' FROM JSON_TABLE(p_json, '$.data[*]' COLUMNS (token_2 VARCHAR(44) PATH '$.one_line_summary.data.token_2')) t WHERE t.token_2 IS NOT NULL AND t.token_2 != 'null'
        UNION SELECT t.fee_token, 'mint' FROM JSON_TABLE(p_json, '$.data[*]' COLUMNS (fee_token VARCHAR(44) PATH '$.one_line_summary.data.fee_token')) t WHERE t.fee_token IS NOT NULL AND t.fee_token != 'null'
        UNION SELECT t.agg_program, 'program' FROM JSON_TABLE(p_json, '$.data[*]' COLUMNS (agg_program VARCHAR(44) PATH '$.one_line_summary.program_id')) t WHERE t.agg_program IS NOT NULL AND t.agg_program != 'null'
        UNION SELECT t.source_owner, 'wallet' FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (source_owner VARCHAR(44) PATH '$.source_owner')) t WHERE t.source_owner IS NOT NULL AND t.source_owner != 'null'
        UNION SELECT t.destination_owner, 'wallet' FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (destination_owner VARCHAR(44) PATH '$.destination_owner')) t WHERE t.destination_owner IS NOT NULL AND t.destination_owner != 'null'
        UNION SELECT t.token_address, 'mint' FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (token_address VARCHAR(44) PATH '$.token_address')) t WHERE t.token_address IS NOT NULL AND t.token_address != 'null'
        UNION SELECT t.base_token, 'mint' FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (base_token VARCHAR(44) PATH '$.base_value.token_address')) t WHERE t.base_token IS NOT NULL AND t.base_token != 'null'
        UNION SELECT t.program_id, 'program' FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (program_id VARCHAR(44) PATH '$.program_id')) t WHERE t.program_id IS NOT NULL AND t.program_id != 'null'
        UNION SELECT t.outer_program_id, 'program' FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (outer_program_id VARCHAR(44) PATH '$.outer_program_id')) t WHERE t.outer_program_id IS NOT NULL AND t.outer_program_id != 'null'
        UNION SELECT a.program_id, 'program' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (program_id VARCHAR(44) PATH '$.program_id')) a WHERE a.program_id IS NOT NULL AND a.program_id != 'null'
        UNION SELECT a.outer_program_id, 'program' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (outer_program_id VARCHAR(44) PATH '$.outer_program_id')) a WHERE a.outer_program_id IS NOT NULL AND a.outer_program_id != 'null'
        UNION SELECT a.account, 'wallet' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (account VARCHAR(44) PATH '$.data.account')) a WHERE a.account IS NOT NULL AND a.account != 'null'
        UNION SELECT a.source, 'wallet' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (source VARCHAR(44) PATH '$.data.source')) a WHERE a.source IS NOT NULL AND a.source != 'null'
        UNION SELECT a.owner_1, 'wallet' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (owner_1 VARCHAR(44) PATH '$.data.owner_1')) a WHERE a.owner_1 IS NOT NULL AND a.owner_1 != 'null'
        UNION SELECT a.token_1, 'mint' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (token_1 VARCHAR(44) PATH '$.data.token_1')) a WHERE a.token_1 IS NOT NULL AND a.token_1 != 'null'
        UNION SELECT a.token_2, 'mint' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (token_2 VARCHAR(44) PATH '$.data.token_2')) a WHERE a.token_2 IS NOT NULL AND a.token_2 != 'null'
        UNION SELECT a.fee_token, 'mint' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (fee_token VARCHAR(44) PATH '$.data.fee_token')) a WHERE a.fee_token IS NOT NULL AND a.fee_token != 'null'
    ) AS core_addresses
    WHERE NOT EXISTS (SELECT 1 FROM tx_address x WHERE x.address = core_addresses.addr);

    -- Section 2: extended addresses (10 JSON_TABLE calls)
    INSERT IGNORE INTO tx_address (address, address_type)
    SELECT DISTINCT addr, addr_type FROM (
        SELECT t.source AS addr, 'ata' AS addr_type FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (source VARCHAR(44) PATH '$.source')) t WHERE t.source IS NOT NULL AND t.source != 'null'
        UNION SELECT t.destination, 'ata' FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (destination VARCHAR(44) PATH '$.destination')) t WHERE t.destination IS NOT NULL AND t.destination != 'null'
        UNION SELECT a.new_account, 'ata' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (new_account VARCHAR(44) PATH '$.data.new_account')) a WHERE a.new_account IS NOT NULL AND a.new_account != 'null'
        UNION SELECT a.init_account, 'ata' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (init_account VARCHAR(44) PATH '$.data.init_account')) a WHERE a.init_account IS NOT NULL AND a.init_account != 'null'
        UNION SELECT a.amm_id, 'pool' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (amm_id VARCHAR(44) PATH '$.data.amm_id')) a WHERE a.amm_id IS NOT NULL AND a.amm_id != 'null'
        UNION SELECT a.owner_2, 'wallet' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (owner_2 VARCHAR(44) PATH '$.data.owner_2')) a WHERE a.owner_2 IS NOT NULL AND a.owner_2 != 'null'
        UNION SELECT a.token_account_1_1, 'vault' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (token_account_1_1 VARCHAR(44) PATH '$.data.token_account_1_1')) a WHERE a.token_account_1_1 IS NOT NULL AND a.token_account_1_1 != 'null'
        UNION SELECT a.token_account_1_2, 'vault' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (token_account_1_2 VARCHAR(44) PATH '$.data.token_account_1_2')) a WHERE a.token_account_1_2 IS NOT NULL AND a.token_account_1_2 != 'null'
        UNION SELECT a.token_account_2_1, 'vault' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (token_account_2_1 VARCHAR(44) PATH '$.data.token_account_2_1')) a WHERE a.token_account_2_1 IS NOT NULL AND a.token_account_2_1 != 'null'
        UNION SELECT a.token_account_2_2, 'vault' FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (token_account_2_2 VARCHAR(44) PATH '$.data.token_account_2_2')) a WHERE a.token_account_2_2 IS NOT NULL AND a.token_account_2_2 != 'null'
    ) AS extended_addresses
    WHERE NOT EXISTS (SELECT 1 FROM tx_address x WHERE x.address = extended_addresses.addr);

    -- Section 3: request_log_id core (20 JSON_TABLE calls, duplicate of section 1)
    SELECT COUNT(*) INTO @dummy FROM (
        SELECT t.x AS addr FROM JSON_TABLE(p_json, '$.data[*]' COLUMNS (x VARCHAR(44) PATH '$.one_line_summary.data.account')) t WHERE t.x IS NOT NULL AND t.x != 'null'
        UNION SELECT t.x FROM JSON_TABLE(p_json, '$.data[*]' COLUMNS (x VARCHAR(44) PATH '$.transfers[0].source_owner')) t WHERE t.x IS NOT NULL AND t.x != 'null'
        UNION SELECT t.x FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (x VARCHAR(44) PATH '$.source_owner')) t WHERE t.x IS NOT NULL AND t.x != 'null'
        UNION SELECT t.x FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (x VARCHAR(44) PATH '$.destination_owner')) t WHERE t.x IS NOT NULL AND t.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.data.account')) a WHERE a.x IS NOT NULL AND a.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.data.source')) a WHERE a.x IS NOT NULL AND a.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.data.owner_1')) a WHERE a.x IS NOT NULL AND a.x != 'null'
        UNION SELECT t.x FROM JSON_TABLE(p_json, '$.data[*]' COLUMNS (x VARCHAR(44) PATH '$.one_line_summary.data.token_1')) t WHERE t.x IS NOT NULL AND t.x != 'null'
        UNION SELECT t.x FROM JSON_TABLE(p_json, '$.data[*]' COLUMNS (x VARCHAR(44) PATH '$.one_line_summary.data.token_2')) t WHERE t.x IS NOT NULL AND t.x != 'null'
        UNION SELECT t.x FROM JSON_TABLE(p_json, '$.data[*]' COLUMNS (x VARCHAR(44) PATH '$.one_line_summary.data.fee_token')) t WHERE t.x IS NOT NULL AND t.x != 'null'
        UNION SELECT t.x FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (x VARCHAR(44) PATH '$.token_address')) t WHERE t.x IS NOT NULL AND t.x != 'null'
        UNION SELECT t.x FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (x VARCHAR(44) PATH '$.base_value.token_address')) t WHERE t.x IS NOT NULL AND t.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.data.token_1')) a WHERE a.x IS NOT NULL AND a.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.data.token_2')) a WHERE a.x IS NOT NULL AND a.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.data.fee_token')) a WHERE a.x IS NOT NULL AND a.x != 'null'
        UNION SELECT t.x FROM JSON_TABLE(p_json, '$.data[*]' COLUMNS (x VARCHAR(44) PATH '$.one_line_summary.program_id')) t WHERE t.x IS NOT NULL AND t.x != 'null'
        UNION SELECT t.x FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (x VARCHAR(44) PATH '$.program_id')) t WHERE t.x IS NOT NULL AND t.x != 'null'
        UNION SELECT t.x FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (x VARCHAR(44) PATH '$.outer_program_id')) t WHERE t.x IS NOT NULL AND t.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.program_id')) a WHERE a.x IS NOT NULL AND a.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.outer_program_id')) a WHERE a.x IS NOT NULL AND a.x != 'null'
    ) x;

    -- Section 4: request_log_id extended (10 JSON_TABLE calls, duplicate of section 2)
    SELECT COUNT(*) INTO @dummy FROM (
        SELECT t.x AS addr FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (x VARCHAR(44) PATH '$.source')) t WHERE t.x IS NOT NULL AND t.x != 'null'
        UNION SELECT t.x FROM JSON_TABLE(p_json, '$.data[*].transfers[*]' COLUMNS (x VARCHAR(44) PATH '$.destination')) t WHERE t.x IS NOT NULL AND t.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.data.new_account')) a WHERE a.x IS NOT NULL AND a.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.data.init_account')) a WHERE a.x IS NOT NULL AND a.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.data.amm_id')) a WHERE a.x IS NOT NULL AND a.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.data.owner_2')) a WHERE a.x IS NOT NULL AND a.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.data.token_account_1_1')) a WHERE a.x IS NOT NULL AND a.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.data.token_account_1_2')) a WHERE a.x IS NOT NULL AND a.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.data.token_account_2_1')) a WHERE a.x IS NOT NULL AND a.x != 'null'
        UNION SELECT a.x FROM JSON_TABLE(p_json, '$.data[*].activities[*]' COLUMNS (x VARCHAR(44) PATH '$.data.token_account_2_2')) a WHERE a.x IS NOT NULL AND a.x != 'null'
    ) x;
END //

DELIMITER ;
