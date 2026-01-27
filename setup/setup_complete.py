# Databricks notebook source
"""
Complete Setup for NiFi Migration System (Cori's Architecture)

This notebook performs the complete setup:
1. Creates all 7 migration_* tables
2. Ingests XML files from UC volume into migration_flows
3. Migrates existing data from old schema (if applicable)

Philosophy: "The App should never 'compute' progress. It should display
            the latest rows and statuses written by the job."

Run this notebook ONCE to set up everything.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration
# MAGIC
# MAGIC Update these settings for your environment:

# COMMAND ----------

# Catalog and Schema
CATALOG = "eliao"
SCHEMA = "nifi_to_databricks"

# UC Volume for XML files
VOLUME_NAME = "nifi_files"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

# Old tables (for migration, if they exist)
OLD_FLOWS_TABLE = f"{CATALOG}.{SCHEMA}.nifi_flows"
OLD_HISTORY_TABLE = f"{CATALOG}.{SCHEMA}.nifi_conversion_history"

print(f"Configuration:")
print(f"  - Catalog/Schema: {CATALOG}.{SCHEMA}")
print(f"  - Volume path: {VOLUME_PATH}")
print(f"  - Old flows table: {OLD_FLOWS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 1: Create Migration Tables
# MAGIC
# MAGIC Creates all 7 migration_* tables following Cori's architecture.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import xml.etree.ElementTree as ET
from datetime import datetime

# spark is available in Databricks notebook runtime
spark  # type: ignore
w = WorkspaceClient()

print("=" * 80)
print("PART 1: Creating Migration Tables")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 1: migration_flows

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.migration_flows (
    flow_id STRING NOT NULL COMMENT 'UUID from XML <rootGroup id="...">',
    flow_name STRING NOT NULL COMMENT 'XML filename (for reference)',
    migration_status STRING NOT NULL COMMENT 'NOT_STARTED | RUNNING | NEEDS_HUMAN | FAILED | DONE | STOPPED',
    current_run_id STRING COMMENT 'FK to migration_runs.run_id',
    last_update_ts TIMESTAMP COMMENT 'Last status update timestamp',
    server STRING COMMENT 'NiFi server/environment',
    nifi_xml_path STRING COMMENT 'Path to XML in UC volume',
    description STRING COMMENT 'Flow description',
    priority STRING COMMENT 'P0, P1, P2',
    owner STRING COMMENT 'Team/pod responsible',
    PRIMARY KEY (flow_id)
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print("✅ Created: migration_flows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 2: migration_runs

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.migration_runs (
    run_id STRING NOT NULL COMMENT 'Format: {{flow_id}}_run_{{timestamp}}',
    flow_id STRING NOT NULL COMMENT 'FK to migration_flows.flow_id',
    job_run_id STRING COMMENT 'Databricks run ID',
    start_ts TIMESTAMP COMMENT 'Run start timestamp',
    end_ts TIMESTAMP COMMENT 'Run end timestamp',
    status STRING COMMENT 'CREATING | QUEUED | RUNNING | SUCCESS | FAILED | CANCELLED',
    PRIMARY KEY (run_id)
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print("✅ Created: migration_runs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 3: migration_iterations

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.migration_iterations (
    run_id STRING NOT NULL COMMENT 'FK to migration_runs.run_id',
    iteration_num INT NOT NULL COMMENT 'Sequential iteration number',
    step STRING COMMENT 'Step name: parse_xml, generate_code, validate',
    step_status STRING COMMENT 'RUNNING | COMPLETED | FAILED',
    ts TIMESTAMP COMMENT 'Event timestamp',
    summary STRING COMMENT 'Human-readable progress description',
    error STRING COMMENT 'Error message if step_status = FAILED',
    PRIMARY KEY (run_id, iteration_num)
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print("✅ Created: migration_iterations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 4: migration_patches

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.migration_patches (
    run_id STRING NOT NULL COMMENT 'FK to migration_runs.run_id',
    iteration_num INT NOT NULL,
    artifact_ref STRING NOT NULL COMMENT 'Which file/artifact was modified',
    patch_summary STRING COMMENT 'Human-readable description',
    diff_ref STRING COMMENT 'Path to diff file or Git commit',
    ts TIMESTAMP COMMENT 'Patch timestamp',
    PRIMARY KEY (run_id, iteration_num, artifact_ref)
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print("✅ Created: migration_patches")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 5: migration_sast_results

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.migration_sast_results (
    run_id STRING NOT NULL COMMENT 'FK to migration_runs.run_id',
    iteration_num INT NOT NULL,
    artifact_ref STRING NOT NULL COMMENT 'Which artifact was scanned',
    scan_status STRING COMMENT 'PENDING | SCANNING | PASSED | BLOCKED',
    findings_json STRING COMMENT 'Full SAST report as JSON',
    ts TIMESTAMP COMMENT 'Scan timestamp',
    PRIMARY KEY (run_id, iteration_num, artifact_ref)
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print("✅ Created: migration_sast_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 6: migration_exec_results

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.migration_exec_results (
    run_id STRING NOT NULL COMMENT 'FK to migration_runs.run_id',
    iteration_num INT NOT NULL,
    job_type STRING NOT NULL COMMENT 'flow_job | validation_job',
    status STRING COMMENT 'SUCCESS | FAILED | TIMEOUT',
    runtime_s INT COMMENT 'Runtime in seconds',
    error STRING COMMENT 'Error message if failed',
    logs_ref STRING COMMENT 'Path to log file in UC Volume',
    ts TIMESTAMP COMMENT 'Execution timestamp',
    PRIMARY KEY (run_id, iteration_num, job_type)
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print("✅ Created: migration_exec_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table 7: migration_human_requests

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.migration_human_requests (
    run_id STRING NOT NULL COMMENT 'FK to migration_runs.run_id',
    iteration_num INT NOT NULL,
    reason STRING COMMENT 'sast_blocked, validation_failed, manual_review',
    instructions STRING COMMENT 'What human should do',
    status STRING COMMENT 'PENDING | RESOLVED | IGNORED',
    ts TIMESTAMP COMMENT 'Request timestamp',
    PRIMARY KEY (run_id, iteration_num)
) USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")
print("✅ Created: migration_human_requests")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Indexes

# COMMAND ----------

print("Creating indexes...")

# migration_flows indexes
spark.sql(f"CREATE INDEX IF NOT EXISTS idx_migration_flows_flow_name ON {CATALOG}.{SCHEMA}.migration_flows(flow_name)")
spark.sql(f"CREATE INDEX IF NOT EXISTS idx_migration_flows_status ON {CATALOG}.{SCHEMA}.migration_flows(migration_status)")

# migration_runs indexes
spark.sql(f"CREATE INDEX IF NOT EXISTS idx_migration_runs_flow_id ON {CATALOG}.{SCHEMA}.migration_runs(flow_id)")
spark.sql(f"CREATE INDEX IF NOT EXISTS idx_migration_runs_status ON {CATALOG}.{SCHEMA}.migration_runs(status)")

# migration_iterations indexes
spark.sql(f"CREATE INDEX IF NOT EXISTS idx_migration_iterations_run_id ON {CATALOG}.{SCHEMA}.migration_iterations(run_id)")

print("✅ Created all indexes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Tables Created

# COMMAND ----------

tables_df = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA} LIKE 'migration_*'")
tables_count = tables_df.count()

print(f"\n{'='*80}")
print(f"✅ Part 1 Complete: Created {tables_count}/7 migration tables")
print(f"{'='*80}\n")

if tables_count < 7:
    print("⚠️ Warning: Not all tables were created. Check for errors above.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2: Ingest XML Files from UC Volume
# MAGIC
# MAGIC Scans the UC volume for NiFi XML files and populates migration_flows table.

# COMMAND ----------

print("=" * 80)
print("PART 2: Ingesting XML Files from UC Volume")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def extract_group_id_from_xml(xml_content):
    """Extract root group ID from NiFi XML."""
    try:
        root = ET.fromstring(xml_content)
        root_group = root.find(".//rootGroup")
        if root_group is not None:
            group_id = root_group.get("id")
            if group_id:
                return group_id
        raise ValueError("No rootGroup id attribute found in XML")
    except ET.ParseError as e:
        raise ValueError(f"XML parse error: {e}")


def scan_volume_for_xmls(volume_path):
    """Recursively scan volume for ALL XML files."""
    xml_files = []

    def scan_directory(path, depth=0):
        try:
            items = list(w.files.list_directory_contents(directory_path=path))
            for item in items:
                if item.is_directory:
                    scan_directory(item.path, depth + 1)
                elif item.path.endswith('.xml'):
                    xml_files.append(item.path)
        except Exception as e:
            print(f"  ⚠️ Error scanning {path}: {e}")

    scan_directory(volume_path)
    return xml_files


def extract_flow_info(xml_path, volume_name='nifi_files'):
    """Extract flow information from NiFi XML file."""
    try:
        filename = xml_path.split('/')[-1]
        flow_name = filename.replace('.xml', '')

        # Extract server from path
        path_parts = xml_path.split('/')
        server = 'default'
        for i, part in enumerate(path_parts):
            if part == volume_name and i + 1 < len(path_parts):
                potential_server = path_parts[i + 1]
                if potential_server != filename:
                    server = potential_server
                break

        # Parse XML to extract flow_id and description
        content = w.files.download(xml_path).contents.read().decode('utf-8')
        flow_id = extract_group_id_from_xml(content)

        # Try to extract description
        description = None
        try:
            root = ET.fromstring(content)
            for child in root:
                if child.tag == 'comment' or child.tag == 'description':
                    description = child.text
                    break
        except:
            pass

        return {
            'flow_id': flow_id,
            'flow_name': flow_name,
            'migration_status': 'NOT_STARTED',
            'server': server,
            'nifi_xml_path': xml_path,
            'description': description,
            'priority': None,
            'owner': None
        }
    except Exception as e:
        print(f"  ❌ Error processing {xml_path}: {e}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scan Volume and Extract Flow Info

# COMMAND ----------

print(f"Scanning volume: {VOLUME_PATH}")
xml_files = scan_volume_for_xmls(VOLUME_PATH)
print(f"Found {len(xml_files)} XML files")

if len(xml_files) == 0:
    print("⚠️ No XML files found. Check VOLUME_PATH.")
else:
    # Extract flow info from each XML
    flows = []
    for i, xml_path in enumerate(xml_files, 1):
        print(f"Processing {i}/{len(xml_files)}: {xml_path}")
        flow_info = extract_flow_info(xml_path, VOLUME_NAME)
        if flow_info:
            flows.append(flow_info)
            print(f"  ✓ flow_id: {flow_info['flow_id']}")

    print(f"\nSuccessfully parsed {len(flows)} flows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert into migration_flows Table

# COMMAND ----------

if len(xml_files) > 0 and flows:
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType

    schema = StructType([
        StructField("flow_id", StringType(), False),
        StructField("flow_name", StringType(), False),
        StructField("migration_status", StringType(), False),
        StructField("server", StringType(), True),
        StructField("nifi_xml_path", StringType(), False),
        StructField("description", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("owner", StringType(), True)
    ])

    flows_df = spark.createDataFrame(flows, schema=schema)

    # Show preview
    print("\nPreview of flows to be inserted:")
    flows_df.select('flow_id', 'flow_name', 'server').show(5, truncate=False)

    # Merge into table
    flows_df.createOrReplaceTempView("new_flows")

    spark.sql(f"""
        MERGE INTO {CATALOG}.{SCHEMA}.migration_flows AS target
        USING new_flows AS source
        ON target.flow_id = source.flow_id
        WHEN MATCHED THEN UPDATE SET
            flow_name = source.flow_name,
            migration_status = source.migration_status,
            server = source.server,
            nifi_xml_path = source.nifi_xml_path,
            description = source.description,
            priority = source.priority,
            owner = source.owner,
            last_update_ts = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            flow_id, flow_name, migration_status, server,
            nifi_xml_path, description, priority, owner, last_update_ts
        ) VALUES (
            source.flow_id, source.flow_name, source.migration_status, source.server,
            source.nifi_xml_path, source.description, source.priority, source.owner,
            current_timestamp()
        )
    """)

    count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.migration_flows").collect()[0][0]
    print(f"\n{'='*80}")
    print(f"✅ Part 2 Complete: Ingested {len(flows)} flows. Total in table: {count}")
    print(f"{'='*80}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3: Migrate Existing Data (Optional)
# MAGIC
# MAGIC Migrates data from old nifi_flows and nifi_conversion_history tables if they exist.

# COMMAND ----------

print("=" * 80)
print("PART 3: Migrating Existing Data (if old tables exist)")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check if Old Tables Exist

# COMMAND ----------

try:
    old_flows_count = spark.sql(f"SELECT COUNT(*) FROM {OLD_FLOWS_TABLE}").collect()[0][0]
    print(f"✓ Found old flows table: {old_flows_count} rows")
    has_old_flows = True
except:
    print(f"⚠️ Old flows table not found: {OLD_FLOWS_TABLE}")
    has_old_flows = False

try:
    old_history_count = spark.sql(f"SELECT COUNT(*) FROM {OLD_HISTORY_TABLE}").collect()[0][0]
    print(f"✓ Found old history table: {old_history_count} rows")
    has_old_history = True
except:
    print(f"⚠️ Old history table not found: {OLD_HISTORY_TABLE}")
    has_old_history = False

if not has_old_flows and not has_old_history:
    print("\n✅ Part 3 Complete: No old data to migrate (fresh setup)")
    print("="*80)
    dbutils.notebook.exit("Setup complete - no migration needed")  # type: ignore

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions for Migration

# COMMAND ----------

def extract_group_id_from_xml_path(xml_path):
    """Extract root group ID from NiFi XML file path."""
    try:
        content = w.files.download(xml_path).contents.read().decode('utf-8')
        root = ET.fromstring(content)
        root_group = root.find(".//rootGroup")
        if root_group is not None:
            group_id = root_group.get("id")
            if group_id:
                return group_id
    except Exception as e:
        print(f"  ⚠️ Error parsing {xml_path}: {e}")
    return None


def map_old_status_to_new(old_status):
    """Map old status values to new migration_status values."""
    status_map = {
        'NEEDS_ATTENTION': 'NEEDS_HUMAN',
        'CONVERTING': 'RUNNING',
        'NOT_STARTED': 'NOT_STARTED',
        'DONE': 'DONE',
        'FAILED': 'FAILED'
    }
    return status_map.get(old_status, old_status)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migrate Flows Table

# COMMAND ----------

if has_old_flows:
    print("Migrating flows from old schema...")

    old_flows_df = spark.sql(f"SELECT * FROM {OLD_FLOWS_TABLE}")
    old_flows = old_flows_df.collect()

    migrated_flows = []

    for i, flow in enumerate(old_flows, 1):
        flow_name = flow['flow_name']
        nifi_xml_path = flow.get('nifi_xml_path')

        print(f"[{i}/{len(old_flows)}] Processing: {flow_name}")

        # Extract flow_id from XML
        flow_id = None
        if nifi_xml_path:
            flow_id = extract_group_id_from_xml_path(nifi_xml_path)

        # If can't extract, generate from hash
        if not flow_id:
            import hashlib, uuid
            name_hash = hashlib.md5(flow_name.encode()).hexdigest()
            flow_id = str(uuid.UUID(name_hash))
            print(f"  ⚠️ Generated flow_id from hash: {flow_id}")

        # Map status
        old_status = flow.get('status', 'NOT_STARTED')
        new_status = map_old_status_to_new(old_status)

        # Convert attempt_id to run_id
        current_run_id = None
        if flow.get('current_attempt_id'):
            attempt_id = flow['current_attempt_id']
            if '_attempt_' in attempt_id:
                timestamp = attempt_id.split('_attempt_')[1]
                current_run_id = f"{flow_id}_run_{timestamp}"

        migrated_flow = {
            'flow_id': flow_id,
            'flow_name': flow_name,
            'migration_status': new_status,
            'current_run_id': current_run_id,
            'last_update_ts': flow.get('last_updated'),
            'server': flow.get('server'),
            'nifi_xml_path': nifi_xml_path,
            'description': flow.get('description'),
            'priority': flow.get('priority'),
            'owner': flow.get('owner')
        }

        migrated_flows.append(migrated_flow)
        print(f"  ✓ Migrated: {flow_id}")

    # Insert into migration_flows
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType

    schema = StructType([
        StructField("flow_id", StringType(), False),
        StructField("flow_name", StringType(), False),
        StructField("migration_status", StringType(), False),
        StructField("current_run_id", StringType(), True),
        StructField("last_update_ts", TimestampType(), True),
        StructField("server", StringType(), True),
        StructField("nifi_xml_path", StringType(), True),
        StructField("description", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("owner", StringType(), True)
    ])

    migrated_df = spark.createDataFrame(migrated_flows, schema=schema)

    # Merge (don't overwrite existing flows from XML ingestion)
    migrated_df.createOrReplaceTempView("migrated_flows")
    spark.sql(f"""
        MERGE INTO {CATALOG}.{SCHEMA}.migration_flows AS target
        USING migrated_flows AS source
        ON target.flow_id = source.flow_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"\n✅ Migrated {len(migrated_flows)} flows from old schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migrate History Table

# COMMAND ----------

if has_old_history:
    print("Migrating history from old schema...")

    # Get flow_name to flow_id mapping
    flow_mapping_df = spark.sql(f"SELECT flow_name, flow_id FROM {CATALOG}.{SCHEMA}.migration_flows")
    flow_mapping = {row['flow_name']: row['flow_id'] for row in flow_mapping_df.collect()}

    old_history_df = spark.sql(f"SELECT * FROM {OLD_HISTORY_TABLE}")
    old_history = old_history_df.collect()

    migrated_runs = []
    migrated_iterations = []

    for i, history in enumerate(old_history, 1):
        attempt_id = history['attempt_id']
        flow_name = history['flow_name']

        flow_id = flow_mapping.get(flow_name)
        if not flow_id:
            print(f"  ⚠️ Flow {flow_name} not found, skipping")
            continue

        # Convert attempt_id to run_id
        if '_attempt_' in attempt_id:
            timestamp = attempt_id.split('_attempt_')[1]
            run_id = f"{flow_id}_run_{timestamp}"
        else:
            ts = int(datetime.now().timestamp())
            run_id = f"{flow_id}_run_{ts}"

        old_status = history.get('status', 'FAILED')

        migrated_run = {
            'run_id': run_id,
            'flow_id': flow_id,
            'job_run_id': history.get('databricks_run_id'),
            'start_ts': history.get('started_at'),
            'end_ts': history.get('completed_at'),
            'status': old_status
        }
        migrated_runs.append(migrated_run)

        # Create placeholder iteration
        iteration_summary = f"Migrated from old schema: {old_status}"
        migrated_iteration = {
            'run_id': run_id,
            'iteration_num': 1,
            'step': 'migration_placeholder',
            'step_status': 'COMPLETED' if old_status == 'SUCCESS' else 'FAILED',
            'ts': history.get('completed_at') or history.get('started_at'),
            'summary': iteration_summary,
            'error': history.get('error_message')
        }
        migrated_iterations.append(migrated_iteration)

    # Insert runs
    if migrated_runs:
        from pyspark.sql.types import TimestampType

        runs_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("flow_id", StringType(), False),
            StructField("job_run_id", StringType(), True),
            StructField("start_ts", TimestampType(), True),
            StructField("end_ts", TimestampType(), True),
            StructField("status", StringType(), False)
        ])

        runs_df = spark.createDataFrame(migrated_runs, schema=runs_schema)
        runs_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.migration_runs")
        print(f"✅ Migrated {len(migrated_runs)} runs")

    # Insert iterations
    if migrated_iterations:
        from pyspark.sql.types import IntegerType

        iterations_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("iteration_num", IntegerType(), False),
            StructField("step", StringType(), False),
            StructField("step_status", StringType(), False),
            StructField("ts", TimestampType(), True),
            StructField("summary", StringType(), True),
            StructField("error", StringType(), True)
        ])

        iterations_df = spark.createDataFrame(migrated_iterations, schema=iterations_schema)
        iterations_df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.migration_iterations")
        print(f"✅ Migrated {len(migrated_iterations)} iterations")

print(f"\n{'='*80}")
print("✅ Part 3 Complete: Migration finished")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Complete!
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ **Part 1**: Created 7 migration tables
# MAGIC ✅ **Part 2**: Ingested XML files from UC volume
# MAGIC ✅ **Part 3**: Migrated existing data (if applicable)
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Start the Flask app** and verify it can read from the new tables
# MAGIC 2. **Test a conversion** to ensure the new schema works end-to-end
# MAGIC 3. **Once verified**, you can drop the old tables: `nifi_flows`, `nifi_conversion_history`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC 🎉 Your NiFi migration system is ready!

# COMMAND ----------

print("\n" + "="*80)
print("FINAL VERIFICATION")
print("="*80)

# Show flow counts
flow_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.migration_flows").collect()[0][0]
run_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.migration_runs").collect()[0][0]
iter_count = spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.migration_iterations").collect()[0][0]

print(f"\nData Summary:")
print(f"  - Flows: {flow_count}")
print(f"  - Runs: {run_count}")
print(f"  - Iterations: {iter_count}")

print("\nFlow Status Distribution:")
spark.sql(f"""
    SELECT migration_status, COUNT(*) as count
    FROM {CATALOG}.{SCHEMA}.migration_flows
    GROUP BY migration_status
    ORDER BY count DESC
""").show()

print("\n" + "="*80)
print("🎉 SETUP COMPLETE!")
print("="*80)
