-- Grab real prod JSON from staging
SET @prod_json = (SELECT txs FROM t16o_db_staging.txs ORDER BY id DESC LIMIT 1);
SELECT JSON_LENGTH(@prod_json, '$.data') AS tx_count, LENGTH(@prod_json) AS json_bytes;

-- Warm up
CALL bench_current(@prod_json);
CALL bench_nested_path(@prod_json);

-- BENCHMARK: Current approach (58 JSON_TABLE calls)
SET @t1 = NOW(6);
CALL bench_current(@prod_json);
CALL bench_current(@prod_json);
CALL bench_current(@prod_json);
SET @t2 = NOW(6);
SELECT TIMESTAMPDIFF(MICROSECOND, @t1, @t2) / 3000 AS current_avg_ms;

-- BENCHMARK: Optimized approach (1 JSON_TABLE + column loop)
SET @t3 = NOW(6);
CALL bench_nested_path(@prod_json);
CALL bench_nested_path(@prod_json);
CALL bench_nested_path(@prod_json);
SET @t4 = NOW(6);
SELECT TIMESTAMPDIFF(MICROSECOND, @t3, @t4) / 3000 AS optimized_avg_ms;
