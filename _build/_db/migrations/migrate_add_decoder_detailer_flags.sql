-- Add separate decoder_processed / detailer_processed flags to tx table
-- Eliminates tx_state bit-OR race condition between concurrent decoder and detailer

ALTER TABLE tx
  ADD COLUMN decoder_processed TINYINT NOT NULL DEFAULT 0 AFTER tx_state,
  ADD COLUMN detailer_processed TINYINT NOT NULL DEFAULT 0 AFTER decoder_processed;

-- Index for guide_loader batch selection
CREATE INDEX idx_tx_processing ON tx (decoder_processed, detailer_processed, tx_state);

-- Backfill existing data based on current tx_state bits
UPDATE tx SET decoder_processed = 1 WHERE tx_state & 4 != 0;
UPDATE tx SET detailer_processed = 1 WHERE tx_state & 16 != 0;
