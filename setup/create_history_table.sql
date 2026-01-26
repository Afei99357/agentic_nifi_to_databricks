-- Create Delta table for NiFi conversion history tracking
-- Run this in Databricks SQL Editor or notebook
-- This table tracks ALL conversion attempts for auditing and comparison

CREATE TABLE IF NOT EXISTS main.default.nifi_conversion_history (
  -- Primary Key
  attempt_id STRING NOT NULL COMMENT 'Unique identifier for this conversion attempt',

  -- Foreign Key
  flow_id STRING NOT NULL COMMENT 'References nifi_flows.flow_id',

  -- Job Information
  databricks_job_id STRING NOT NULL COMMENT 'Databricks job ID created for this attempt',
  databricks_run_id STRING COMMENT 'Databricks run ID when job starts',
  job_name STRING COMMENT 'Name of the Databricks job',

  -- Attempt Status
  status STRING NOT NULL COMMENT 'CREATING | RUNNING | SUCCESS | FAILED | CANCELLED',
  started_at TIMESTAMP NOT NULL DEFAULT current_timestamp() COMMENT 'When this attempt started',
  completed_at TIMESTAMP COMMENT 'When this attempt finished',
  duration_seconds INT COMMENT 'Total duration in seconds',

  -- Progress Metrics (updated by agent during conversion)
  progress_percentage INT DEFAULT 0 COMMENT 'Conversion progress 0-100',
  iterations INT DEFAULT 0 COMMENT 'Number of agent iterations',
  validation_percentage INT DEFAULT 0 COMMENT 'Validation coverage 0-100',

  -- Results
  generated_notebooks ARRAY<STRING> COMMENT 'List of generated notebook paths',
  error_message STRING COMMENT 'Error details if failed',
  status_message STRING COMMENT 'Human-readable status message',

  -- Metadata
  attempt_number INT COMMENT 'Sequential attempt number for this flow (1, 2, 3...)',
  triggered_by STRING COMMENT 'user | retry | scheduled',

  PRIMARY KEY (attempt_id)
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'description' = 'Tracks all NiFi flow conversion attempts for history and auditing'
);

-- Create index on flow_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_history_flow_id
ON main.default.nifi_conversion_history(flow_id);

-- Verify table was created
DESCRIBE TABLE main.default.nifi_conversion_history;

SELECT 'History table created successfully' AS status;
