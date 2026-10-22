-- Migration: Add funder queue worker config entries
-- These drive the supervisor/worker thread pattern in guide-funder.py

INSERT IGNORE INTO config (config_type, config_key, config_value) VALUES
('queue', 'funder_wrk_cnt_threads',           '2'),
('queue', 'funder_wrk_cnt_prefetch',          '5'),
('queue', 'funder_wrk_supervisor_poll_sec',   '5'),
('queue', 'funder_wrk_poll_idle_sec',         '0.25'),
('queue', 'funder_wrk_reconnect_sec',         '5'),
('queue', 'funder_wrk_shutdown_timeout_sec',  '10'),
('queue', 'funder_wrk_api_timeout_sec',       '60'),
('queue', 'funder_wrk_api_delay_sec',         '0.30'),
('queue', 'funder_wrk_batch_delay_sec',       '1.0'),
('queue', 'funder_wrk_deadlock_max_retries',  '5'),
('queue', 'funder_wrk_deadlock_base_delay',   '0.1'),
('queue', 'funder_wrk_db_poll_sec',           '30');
