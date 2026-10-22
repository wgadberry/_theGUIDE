-- Grab real prod JSON from staging
SET @prod_json = (SELECT txs FROM t16o_db_staging.txs ORDER BY id DESC LIMIT 1);
SELECT JSON_LENGTH(@prod_json, '$.data') AS tx_count, LENGTH(@prod_json) AS json_bytes;

-- Warm up both
CALL bench_current(@prod_json);
CALL sp_tx_prepopulate_lookups(@prod_json, NULL, 2);

-- BENCHMARK: Old approach (58 JSON_TABLE calls via bench_current)
SET @t1 = NOW(6);
CALL bench_current(@prod_json);
CALL bench_current(@prod_json);
CALL bench_current(@prod_json);
SET @t2 = NOW(6);
SELECT TIMESTAMPDIFF(MICROSECOND, @t1, @t2) / 3000 AS old_avg_ms;

-- BENCHMARK: New approach (31 JSON_TABLE calls via updated sp_tx_prepopulate_lookups)
SET @t3 = NOW(6);
CALL sp_tx_prepopulate_lookups(@prod_json, NULL, 2);
CALL sp_tx_prepopulate_lookups(@prod_json, NULL, 2);
CALL sp_tx_prepopulate_lookups(@prod_json, NULL, 2);
SET @t4 = NOW(6);
SELECT TIMESTAMPDIFF(MICROSECOND, @t3, @t4) / 3000 AS new_avg_ms;
