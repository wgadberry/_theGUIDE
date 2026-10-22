-- Migration: Add decoder queue worker config entries
-- These drive the supervisor/worker thread pattern in guide-decoder.py

INSERT IGNORE INTO config (config_type, config_key, config_value) VALUES
('queue', 'decoder_wrk_cnt_threads',          '2'),
('queue', 'decoder_wrk_cnt_prefetch',         '5'),
('queue', 'decoder_wrk_supervisor_poll_sec',  '5'),
('queue', 'decoder_wrk_poll_idle_sec',        '0.25'),
('queue', 'decoder_wrk_reconnect_sec',        '5'),
('queue', 'decoder_wrk_shutdown_timeout_sec', '10'),
('queue', 'decoder_wrk_api_timeout_sec',      '30');
