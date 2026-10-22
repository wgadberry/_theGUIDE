-- Grab real prod JSON from staging
SET @prod_json = (SELECT txs FROM t16o_db_staging.txs ORDER BY id DESC LIMIT 1);
SELECT JSON_LENGTH(@prod_json, '$.data') AS tx_count, LENGTH(@prod_json) AS json_bytes;

-- Use a dummy request_log_id so the request_log_id path actually runs
SET @dummy_log_id = 99999;

-- Warm up
CALL bench_current(@prod_json);
CALL sp_tx_prepopulate_lookups(@prod_json, @dummy_log_id, 2);

-- BENCHMARK: Old approach (bench_current simulates 58 JSON_TABLE calls)
SET @t1 = NOW(6);
CALL bench_current(@prod_json);
CALL bench_current(@prod_json);
CALL bench_current(@prod_json);
SET @t2 = NOW(6);
SELECT TIMESTAMPDIFF(MICROSECOND, @t1, @t2) / 3000 AS old_58_calls_avg_ms;

-- BENCHMARK: New procedure with request_log_id (31 JSON_TABLE calls + temp table reuse)
SET @t3 = NOW(6);
CALL sp_tx_prepopulate_lookups(@prod_json, @dummy_log_id, 2);
CALL sp_tx_prepopulate_lookups(@prod_json, @dummy_log_id, 2);
CALL sp_tx_prepopulate_lookups(@prod_json, @dummy_log_id, 2);
SET @t4 = NOW(6);
SELECT TIMESTAMPDIFF(MICROSECOND, @t3, @t4) / 3000 AS new_31_calls_avg_ms;

-- Clean up dummy request_log_id tags
UPDATE tx_address SET request_log_id = NULL WHERE request_log_id = @dummy_log_id;
