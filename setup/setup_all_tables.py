# Databricks notebook source
"""
Complete Setup for NiFi Flow Conversion System

This notebook creates all required tables and schema for the NiFi to Databricks
conversion system with dynamic job creation and history tracking.

Run this notebook ONCE to set up your environment.

Steps:
1. Create nifi_flows table
2. Create nifi_conversion_history table
3. Add history tracking columns to nifi_flows
4. Optionally migrate existing data (if you have old flows)

After running this, you can use setup/populate_flows_from_volume.py to load your XML files.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Update these if your catalog/schema names are different:

# COMMAND ----------

# Configuration - Change these if your catalog/schema names are different
CATALOG = "main"
SCHEMA = "default"
FLOWS_TABLE = f"{CATALOG}.{SCHEMA}.nifi_flows"
HISTORY_TABLE = f"{CATALOG}.{SCHEMA}.nifi_conversion_history"

# spark is available in Databricks notebook runtime
spark  # type: ignore

print(f"Setting up tables:")
print(f"  - Flows table: {FLOWS_TABLE}")
print(f"  - History table: {HISTORY_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create nifi_flows Table
# MAGIC
# MAGIC This table stores metadata about each NiFi flow to be converted.

# COMMAND ----------

# type: ignore
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FLOWS_TABLE} (
  -- Primary Key
  flow_name STRING NOT NULL COMMENT 'Flow name (XML filename without .xml extension)',

  -- Flow Metadata (Pre-populated from XML)
  server STRING NOT NULL COMMENT 'NiFi server/environment (derived from folder path)',
  nifi_xml_path STRING NOT NULL COMMENT 'Path to XML file in UC volume',
  description STRING COMMENT 'Flow description (extracted from XML if available)',
  priority STRING COMMENT 'P0, P1, P2 for prioritization',
  owner STRING COMMENT 'Team or pod responsible (Pod-A, Pod-B, etc.)',

  -- Conversion Status (Updated by app during conversion)
  status STRING NOT NULL COMMENT 'NOT_STARTED | CONVERTING | DONE | NEEDS_ATTENTION',
  databricks_job_id STRING COMMENT 'Databricks job ID for conversion',
  databricks_run_id STRING COMMENT 'Current or last run ID',
  progress_percentage INT COMMENT 'Estimated progress 0-100',
  iterations INT COMMENT 'Number of agent iterations',
  validation_percentage INT COMMENT 'Validation coverage 0-100',

  -- Timestamps
  created_at TIMESTAMP COMMENT 'When flow was added to table',
  last_updated TIMESTAMP COMMENT 'Last status update',
  conversion_started_at TIMESTAMP COMMENT 'When conversion started',
  conversion_completed_at TIMESTAMP COMMENT 'When conversion finished',

  -- Results & Errors
  generated_notebooks ARRAY<STRING> COMMENT 'List of generated notebook paths',
  error_message STRING COMMENT 'Error details if failed',
  status_message STRING COMMENT 'Human-readable status message',

  PRIMARY KEY (flow_name)
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported'
)
""")

print(f"✅ Created table: {FLOWS_TABLE}")

# COMMAND ----------

# Add default values
spark.sql(f"ALTER TABLE {FLOWS_TABLE} ALTER COLUMN status SET DEFAULT 'NOT_STARTED'")
spark.sql(f"ALTER TABLE {FLOWS_TABLE} ALTER COLUMN progress_percentage SET DEFAULT 0")
spark.sql(f"ALTER TABLE {FLOWS_TABLE} ALTER COLUMN iterations SET DEFAULT 0")
spark.sql(f"ALTER TABLE {FLOWS_TABLE} ALTER COLUMN validation_percentage SET DEFAULT 0")
spark.sql(f"ALTER TABLE {FLOWS_TABLE} ALTER COLUMN created_at SET DEFAULT current_timestamp()")
spark.sql(f"ALTER TABLE {FLOWS_TABLE} ALTER COLUMN last_updated SET DEFAULT current_timestamp()")

print("✅ Default values configured")

# COMMAND ----------

print("✅ Step 1: nifi_flows table created with defaults")
spark.sql(f"DESCRIBE TABLE {FLOWS_TABLE}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create nifi_conversion_history Table
# MAGIC
# MAGIC This table tracks ALL conversion attempts for auditing and troubleshooting.

# COMMAND ----------

# type: ignore
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
  -- Primary Key
  attempt_id STRING NOT NULL COMMENT 'Unique identifier for this conversion attempt',

  -- Foreign Key
  flow_name STRING NOT NULL COMMENT 'References nifi_flows.flow_name',

  -- Job Information
  databricks_job_id STRING NOT NULL COMMENT 'Databricks job ID created for this attempt',
  databricks_run_id STRING COMMENT 'Databricks run ID when job starts',
  job_name STRING COMMENT 'Name of the Databricks job',

  -- Attempt Status
  status STRING NOT NULL COMMENT 'CREATING | RUNNING | SUCCESS | FAILED | CANCELLED',
  started_at TIMESTAMP NOT NULL COMMENT 'When this attempt started',
  completed_at TIMESTAMP COMMENT 'When this attempt finished',
  duration_seconds INT COMMENT 'Total duration in seconds',

  -- Progress Metrics (updated by agent during conversion)
  progress_percentage INT COMMENT 'Conversion progress 0-100',
  iterations INT COMMENT 'Number of agent iterations',
  validation_percentage INT COMMENT 'Validation coverage 0-100',

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
  'delta.feature.allowColumnDefaults' = 'supported',
  'description' = 'Tracks all NiFi flow conversion attempts for history and auditing'
)
""")

print(f"✅ Created table: {HISTORY_TABLE}")

# COMMAND ----------

# Add default values for history table
spark.sql(f"ALTER TABLE {HISTORY_TABLE} ALTER COLUMN started_at SET DEFAULT current_timestamp()")
spark.sql(f"ALTER TABLE {HISTORY_TABLE} ALTER COLUMN progress_percentage SET DEFAULT 0")
spark.sql(f"ALTER TABLE {HISTORY_TABLE} ALTER COLUMN iterations SET DEFAULT 0")
spark.sql(f"ALTER TABLE {HISTORY_TABLE} ALTER COLUMN validation_percentage SET DEFAULT 0")

print("✅ Default values configured")
print("ℹ️  Note: Unity Catalog automatically optimizes queries, no manual indexes needed")

# COMMAND ----------

print("✅ Step 2: nifi_conversion_history table created with defaults")
spark.sql(f"DESCRIBE TABLE {HISTORY_TABLE}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Add History Tracking Columns to nifi_flows
# MAGIC
# MAGIC Add new columns to track conversion attempts.

# COMMAND ----------

# Add new columns for history tracking
try:
    # type: ignore
    spark.sql(f"""
        ALTER TABLE {FLOWS_TABLE}
        ADD COLUMNS (
          current_attempt_id STRING COMMENT 'Reference to current attempt in history table',
          total_attempts INT COMMENT 'Total number of conversion attempts',
          successful_conversions INT COMMENT 'Number of successful conversions',
          last_attempt_at TIMESTAMP COMMENT 'Timestamp of most recent attempt',
          first_attempt_at TIMESTAMP COMMENT 'Timestamp of first attempt'
        )
    """)
except Exception as e:
    if "already exists" in str(e).lower():
        print("Columns already exist, skipping...")
    else:
        raise

# COMMAND ----------

# Set default values for new columns
spark.sql(f"ALTER TABLE {FLOWS_TABLE} ALTER COLUMN total_attempts SET DEFAULT 0")
spark.sql(f"ALTER TABLE {FLOWS_TABLE} ALTER COLUMN successful_conversions SET DEFAULT 0")

# COMMAND ----------

print("✅ Step 3: History tracking columns added to nifi_flows with defaults")
spark.sql(f"DESCRIBE TABLE {FLOWS_TABLE}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Migrate Existing Data (Optional)
# MAGIC
# MAGIC **Run this ONLY if you have existing flows with conversion data.**
# MAGIC
# MAGIC If this is a fresh setup, skip this cell.

# COMMAND ----------

# Check if there are any flows with existing conversion data
# type: ignore
existing_conversions = spark.sql(f"""
    SELECT COUNT(*) as count
    FROM {FLOWS_TABLE}
    WHERE databricks_run_id IS NOT NULL
""").collect()[0]['count']

if existing_conversions > 0:
    print(f"Found {existing_conversions} flows with existing conversion data")
    print("Migrating to history table...")

    # Migrate existing conversion runs to history table
    # type: ignore
    spark.sql(f"""
        INSERT INTO {HISTORY_TABLE} (
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
        FROM {FLOWS_TABLE}
        WHERE databricks_run_id IS NOT NULL
    """)

    # Update flows table with references to migrated history
    # type: ignore
    spark.sql(f"""
        UPDATE {FLOWS_TABLE}
        SET
          current_attempt_id = CONCAT(flow_name, '_attempt_migrated'),
          total_attempts = 1,
          successful_conversions = CASE WHEN status = 'DONE' THEN 1 ELSE 0 END,
          first_attempt_at = COALESCE(conversion_started_at, created_at),
          last_attempt_at = COALESCE(conversion_started_at, created_at)
        WHERE databricks_run_id IS NOT NULL
    """)

    print(f"✅ Step 4: Migrated {existing_conversions} existing conversions to history table")
else:
    print("ℹ️  Step 4: No existing conversion data found, skipping migration")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification & Summary

# COMMAND ----------

# Show table counts
flows_count = spark.sql(f"SELECT COUNT(*) as count FROM {FLOWS_TABLE}").collect()[0]['count']
history_count = spark.sql(f"SELECT COUNT(*) as count FROM {HISTORY_TABLE}").collect()[0]['count']

print("=" * 60)
print("SETUP COMPLETE ✅")
print("=" * 60)
print(f"\n📊 Current Status:")
print(f"  - Flows table: {flows_count} flows")
print(f"  - History table: {history_count} attempts")
print(f"\n📋 Next Steps:")
print(f"  1. Upload NiFi XML files to Unity Catalog volume")
print(f"     Example: /Volumes/{CATALOG}/{SCHEMA}/nifi_xmls/")
print(f"  2. Run setup/populate_flows_from_volume.py to scan and load flows")
print(f"  3. Upload notebooks/nifi_conversion_dummy_agent.py to:")
print(f"     /Workspace/Shared/nifi_conversion_dummy_agent")
print(f"  4. Deploy the app: databricks apps deploy")
print("=" * 60)

# COMMAND ----------

# Show sample data if any flows exist
if flows_count > 0:
    print("\nSample flows:")
    # type: ignore
    spark.sql(f"""
        SELECT
            flow_name,
            server,
            status,
            total_attempts,
            successful_conversions
        FROM {FLOWS_TABLE}
        LIMIT 5
    """).show(truncate=False)
