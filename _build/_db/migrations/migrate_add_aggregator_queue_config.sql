-- Migration: Add aggregator queue worker config entries
-- These drive the supervisor/worker thread pattern in guide-aggregator.py

INSERT IGNORE INTO config (config_type, config_key, config_value) VALUES
('queue', 'aggregator_wrk_cnt_threads',           '1'),
('queue', 'aggregator_wrk_cnt_prefetch',          '5'),
('queue', 'aggregator_wrk_supervisor_poll_sec',   '5'),
('queue', 'aggregator_wrk_poll_idle_sec',         '0.25'),
('queue', 'aggregator_wrk_reconnect_sec',         '5'),
('queue', 'aggregator_wrk_shutdown_timeout_sec',  '10'),
('queue', 'aggregator_wrk_batch_size',            '10000'),
('queue', 'aggregator_wrk_deadlock_max_retries',  '5'),
('queue', 'aggregator_wrk_deadlock_base_delay',   '0.1'),
('queue', 'aggregator_wrk_db_poll_sec',           '30');
