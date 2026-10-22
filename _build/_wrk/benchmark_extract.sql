SET @j = (SELECT txs FROM t16o_db_staging.txs WHERE id = 2151);
SELECT JSON_LENGTH(@j, '$.data') AS tx_count, LENGTH(@j) AS json_bytes;

SET @dummy_log_id = 99999;

-- Warm up both
CALL bench_original_prepop(@j, @dummy_log_id, 2);
UPDATE tx_address SET request_log_id = NULL WHERE request_log_id = @dummy_log_id;

-- ======= BENCHMARK: Original (61 JSON_TABLE on full 373KB doc) =======
SET @t1 = NOW(6);
CALL bench_original_prepop(@j, @dummy_log_id, 2);
UPDATE tx_address SET request_log_id = NULL WHERE request_log_id = @dummy_log_id;
CALL bench_original_prepop(@j, @dummy_log_id, 2);
UPDATE tx_address SET request_log_id = NULL WHERE request_log_id = @dummy_log_id;
CALL bench_original_prepop(@j, @dummy_log_id, 2);
UPDATE tx_address SET request_log_id = NULL WHERE request_log_id = @dummy_log_id;
SET @t2 = NOW(6);
SELECT TIMESTAMPDIFF(MICROSECOND, @t1, @t2) / 3000 AS original_avg_ms;

-- ======= BENCHMARK: JSON_EXTRACT approach (20 extracts + 3 JSON_TABLE on tiny arrays) =======
-- Warm up
SET @wallets = JSON_MERGE_PRESERVE(
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.account'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[0].source_owner'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].source_owner'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].destination_owner'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.account'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.source'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.owner_1'), JSON_ARRAY())
);
SET @mints = JSON_MERGE_PRESERVE(
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.token_1'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.token_2'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.fee_token'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].token_address'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].base_value.token_address'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.token_1'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.token_2'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.fee_token'), JSON_ARRAY())
);
SET @programs = JSON_MERGE_PRESERVE(
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].outer_program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].outer_program_id'), JSON_ARRAY())
);
INSERT IGNORE INTO tx_address (address, address_type)
SELECT DISTINCT j.addr, 'wallet' FROM JSON_TABLE(@wallets, '$[*]' COLUMNS (addr VARCHAR(44) PATH '$')) j WHERE j.addr IS NOT NULL AND j.addr != 'null';
INSERT IGNORE INTO tx_address (address, address_type)
SELECT DISTINCT j.addr, 'mint' FROM JSON_TABLE(@mints, '$[*]' COLUMNS (addr VARCHAR(44) PATH '$')) j WHERE j.addr IS NOT NULL AND j.addr != 'null';
INSERT IGNORE INTO tx_address (address, address_type)
SELECT DISTINCT j.addr, 'program' FROM JSON_TABLE(@programs, '$[*]' COLUMNS (addr VARCHAR(44) PATH '$')) j WHERE j.addr IS NOT NULL AND j.addr != 'null';

-- Run 1
SET @t3 = NOW(6);

SET @wallets = JSON_MERGE_PRESERVE(
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.account'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[0].source_owner'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].source_owner'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].destination_owner'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.account'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.source'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.owner_1'), JSON_ARRAY())
);
SET @mints = JSON_MERGE_PRESERVE(
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.token_1'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.token_2'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.fee_token'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].token_address'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].base_value.token_address'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.token_1'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.token_2'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.fee_token'), JSON_ARRAY())
);
SET @programs = JSON_MERGE_PRESERVE(
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].outer_program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].outer_program_id'), JSON_ARRAY())
);
INSERT IGNORE INTO tx_address (address, address_type)
SELECT DISTINCT j.addr, 'wallet' FROM JSON_TABLE(@wallets, '$[*]' COLUMNS (addr VARCHAR(44) PATH '$')) j WHERE j.addr IS NOT NULL AND j.addr != 'null';
INSERT IGNORE INTO tx_address (address, address_type)
SELECT DISTINCT j.addr, 'mint' FROM JSON_TABLE(@mints, '$[*]' COLUMNS (addr VARCHAR(44) PATH '$')) j WHERE j.addr IS NOT NULL AND j.addr != 'null';
INSERT IGNORE INTO tx_address (address, address_type)
SELECT DISTINCT j.addr, 'program' FROM JSON_TABLE(@programs, '$[*]' COLUMNS (addr VARCHAR(44) PATH '$')) j WHERE j.addr IS NOT NULL AND j.addr != 'null';

-- Run 2
SET @wallets = JSON_MERGE_PRESERVE(
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.account'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[0].source_owner'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].source_owner'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].destination_owner'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.account'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.source'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.owner_1'), JSON_ARRAY())
);
SET @mints = JSON_MERGE_PRESERVE(
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.token_1'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.token_2'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.fee_token'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].token_address'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].base_value.token_address'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.token_1'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.token_2'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.fee_token'), JSON_ARRAY())
);
SET @programs = JSON_MERGE_PRESERVE(
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].outer_program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].outer_program_id'), JSON_ARRAY())
);
INSERT IGNORE INTO tx_address (address, address_type)
SELECT DISTINCT j.addr, 'wallet' FROM JSON_TABLE(@wallets, '$[*]' COLUMNS (addr VARCHAR(44) PATH '$')) j WHERE j.addr IS NOT NULL AND j.addr != 'null';
INSERT IGNORE INTO tx_address (address, address_type)
SELECT DISTINCT j.addr, 'mint' FROM JSON_TABLE(@mints, '$[*]' COLUMNS (addr VARCHAR(44) PATH '$')) j WHERE j.addr IS NOT NULL AND j.addr != 'null';
INSERT IGNORE INTO tx_address (address, address_type)
SELECT DISTINCT j.addr, 'program' FROM JSON_TABLE(@programs, '$[*]' COLUMNS (addr VARCHAR(44) PATH '$')) j WHERE j.addr IS NOT NULL AND j.addr != 'null';

-- Run 3
SET @wallets = JSON_MERGE_PRESERVE(
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.account'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[0].source_owner'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].source_owner'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].destination_owner'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.account'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.source'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.owner_1'), JSON_ARRAY())
);
SET @mints = JSON_MERGE_PRESERVE(
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.token_1'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.token_2'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.data.fee_token'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].token_address'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].base_value.token_address'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.token_1'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.token_2'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].data.fee_token'), JSON_ARRAY())
);
SET @programs = JSON_MERGE_PRESERVE(
    COALESCE(JSON_EXTRACT(@j, '$.data[*].one_line_summary.program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].transfers[*].outer_program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].program_id'), JSON_ARRAY()),
    COALESCE(JSON_EXTRACT(@j, '$.data[*].activities[*].outer_program_id'), JSON_ARRAY())
);
INSERT IGNORE INTO tx_address (address, address_type)
SELECT DISTINCT j.addr, 'wallet' FROM JSON_TABLE(@wallets, '$[*]' COLUMNS (addr VARCHAR(44) PATH '$')) j WHERE j.addr IS NOT NULL AND j.addr != 'null';
INSERT IGNORE INTO tx_address (address, address_type)
SELECT DISTINCT j.addr, 'mint' FROM JSON_TABLE(@mints, '$[*]' COLUMNS (addr VARCHAR(44) PATH '$')) j WHERE j.addr IS NOT NULL AND j.addr != 'null';
INSERT IGNORE INTO tx_address (address, address_type)
SELECT DISTINCT j.addr, 'program' FROM JSON_TABLE(@programs, '$[*]' COLUMNS (addr VARCHAR(44) PATH '$')) j WHERE j.addr IS NOT NULL AND j.addr != 'null';

SET @t4 = NOW(6);
SELECT TIMESTAMPDIFF(MICROSECOND, @t3, @t4) / 3000 AS json_extract_avg_ms;
