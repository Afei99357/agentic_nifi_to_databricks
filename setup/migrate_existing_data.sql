-- Migrate existing conversion data to history table
-- Run this in Databricks SQL Editor or notebook
-- IMPORTANT: Run this AFTER create_history_table.sql AND update_flows_table_schema.sql

-- Step 1: Migrate existing conversion runs to history table
-- For flows that have already been converted (have databricks_run_id)
INSERT INTO main.default.nifi_conversion_history (
  attempt_id,
  flow_name,
  databricks_job_id,
  databricks_run_id,
  job_name,
  status,
  started_at,
  completed_at,
  duration_seconds,
  progress_percentage,
  iterations,
  validation_percentage,
  generated_notebooks,
  error_message,
  status_message,
  attempt_number,
  triggered_by
)
SELECT
  CONCAT(flow_name, '_attempt_migrated') AS attempt_id,
  flow_name,
  COALESCE(databricks_job_id, 'MIGRATED_UNKNOWN') AS databricks_job_id,
  databricks_run_id,
  CONCAT('NiFi_Conversion_', flow_name, '_migrated') AS job_name,
  CASE
    WHEN status = 'DONE' THEN 'SUCCESS'
    WHEN status = 'CONVERTING' THEN 'RUNNING'
    WHEN status = 'NEEDS_ATTENTION' THEN 'FAILED'
    ELSE 'UNKNOWN'
  END AS status,
  COALESCE(conversion_started_at, created_at) AS started_at,
  conversion_completed_at,
  CASE
    WHEN conversion_started_at IS NOT NULL AND conversion_completed_at IS NOT NULL
    THEN CAST((UNIX_TIMESTAMP(conversion_completed_at) - UNIX_TIMESTAMP(conversion_started_at)) AS INT)
    ELSE NULL
  END AS duration_seconds,
  progress_percentage,
  iterations,
  validation_percentage,
  generated_notebooks,
  error_message,
  status_message,
  1 AS attempt_number,
  'user' AS triggered_by
FROM main.default.nifi_flows
WHERE databricks_run_id IS NOT NULL;  -- Only migrate flows that have been converted

-- Step 2: Update flows table with references to migrated history
UPDATE main.default.nifi_flows
SET
  current_attempt_id = CONCAT(flow_name, '_attempt_migrated'),
  total_attempts = 1,
  successful_conversions = CASE WHEN status = 'DONE' THEN 1 ELSE 0 END,
  first_attempt_at = COALESCE(conversion_started_at, created_at),
  last_attempt_at = COALESCE(conversion_started_at, created_at)
WHERE databricks_run_id IS NOT NULL;

-- Step 3: Verify migration
SELECT
  'Migration completed' AS status,
  COUNT(*) AS migrated_attempts
FROM main.default.nifi_conversion_history;

-- Step 4: Show summary of migrated data
SELECT
  f.flow_name,
  f.total_attempts,
  f.successful_conversions,
  h.status AS last_attempt_status,
  h.started_at AS last_attempt_started
FROM main.default.nifi_flows f
LEFT JOIN main.default.nifi_conversion_history h
  ON f.current_attempt_id = h.attempt_id
WHERE f.total_attempts > 0
ORDER BY f.last_attempt_at DESC
LIMIT 10;
