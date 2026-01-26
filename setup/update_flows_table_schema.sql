-- Update nifi_flows table schema to support history tracking
-- Run this in Databricks SQL Editor or notebook
-- IMPORTANT: Run this AFTER create_history_table.sql

-- Add new columns for history tracking
ALTER TABLE main.default.nifi_flows
ADD COLUMNS (
  current_attempt_id STRING COMMENT 'Reference to current attempt in history table',
  total_attempts INT DEFAULT 0 COMMENT 'Total number of conversion attempts',
  successful_conversions INT DEFAULT 0 COMMENT 'Number of successful conversions',
  last_attempt_at TIMESTAMP COMMENT 'Timestamp of most recent attempt',
  first_attempt_at TIMESTAMP COMMENT 'Timestamp of first attempt'
);

-- Note: We keep the old columns for backward compatibility during migration
-- databricks_run_id, conversion_started_at, conversion_completed_at
-- These will be deprecated in favor of history table

-- Verify schema update
DESCRIBE TABLE main.default.nifi_flows;

SELECT 'Schema updated successfully' AS status;
