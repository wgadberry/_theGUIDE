-- Migration: Add detailer queue worker config entries
-- These drive the supervisor/worker thread pattern in guide-detailer.py

INSERT IGNORE INTO config (config_type, config_key, config_value) VALUES
('queue', 'detailer_wrk_cnt_threads',          '2'),
('queue', 'detailer_wrk_cnt_prefetch',         '5'),
('queue', 'detailer_wrk_supervisor_poll_sec',  '5'),
('queue', 'detailer_wrk_poll_idle_sec',        '0.25'),
('queue', 'detailer_wrk_reconnect_sec',        '5'),
('queue', 'detailer_wrk_shutdown_timeout_sec', '10'),
('queue', 'detailer_wrk_api_timeout_sec',      '60'),
('queue', 'detailer_wrk_api_max_retries',      '3');
