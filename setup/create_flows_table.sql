-- Create Delta table for NiFi flow tracking
-- Run this in Databricks SQL Editor or notebook

CREATE TABLE IF NOT EXISTS main.default.nifi_flows (
  -- Primary Key
  flow_name STRING NOT NULL COMMENT 'Flow name (XML filename without .xml extension)',

  -- Flow Metadata (Pre-populated from XML)
  server STRING NOT NULL COMMENT 'NiFi server/environment (derived from folder path)',
  nifi_xml_path STRING NOT NULL COMMENT 'Path to XML file in UC volume',
  description STRING COMMENT 'Flow description (extracted from XML if available)',
  priority STRING COMMENT 'P0, P1, P2 for prioritization',
  owner STRING COMMENT 'Team or pod responsible (Pod-A, Pod-B, etc.)',

  -- Conversion Status (Updated by app during conversion)
  status STRING NOT NULL DEFAULT 'NOT_STARTED' COMMENT 'NOT_STARTED | CONVERTING | DONE | NEEDS_ATTENTION',
  databricks_job_id STRING COMMENT 'Databricks job ID for conversion',
  databricks_run_id STRING COMMENT 'Current or last run ID',
  progress_percentage INT DEFAULT 0 COMMENT 'Estimated progress 0-100',
  iterations INT DEFAULT 0 COMMENT 'Number of agent iterations',
  validation_percentage INT DEFAULT 0 COMMENT 'Validation coverage 0-100',

  -- Timestamps
  created_at TIMESTAMP DEFAULT current_timestamp() COMMENT 'When flow was added to table',
  last_updated TIMESTAMP DEFAULT current_timestamp() COMMENT 'Last status update',
  conversion_started_at TIMESTAMP COMMENT 'When conversion started',
  conversion_completed_at TIMESTAMP COMMENT 'When conversion finished',

  -- Results & Errors
  generated_notebooks ARRAY<STRING> COMMENT 'List of generated notebook paths',
  error_message STRING COMMENT 'Error details if failed',
  status_message STRING COMMENT 'Human-readable status message',

  PRIMARY KEY (flow_name)
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);

-- Verify table was created
DESCRIBE TABLE main.default.nifi_flows;
