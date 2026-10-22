-- Migration: Add enricher queue worker config entries
-- These drive the supervisor/worker thread pattern in guide-enricher.py

INSERT IGNORE INTO config (config_type, config_key, config_value) VALUES
('queue', 'enricher_wrk_cnt_threads',           '1'),
('queue', 'enricher_wrk_cnt_prefetch',          '5'),
('queue', 'enricher_wrk_supervisor_poll_sec',   '5'),
('queue', 'enricher_wrk_poll_idle_sec',         '0.25'),
('queue', 'enricher_wrk_reconnect_sec',         '5'),
('queue', 'enricher_wrk_shutdown_timeout_sec',  '10'),
('queue', 'enricher_wrk_api_timeout_sec',       '60'),
('queue', 'enricher_wrk_api_delay_sec',         '0.30'),
('queue', 'enricher_wrk_batch_limit',           '100'),
('queue', 'enricher_wrk_max_attempts',          '3'),
('queue', 'enricher_wrk_db_poll_sec',           '30');
