# Databricks notebook source
"""
Migrate Existing Data from Old Schema to New Migration Schema

This notebook migrates data from the old nifi_flows and nifi_conversion_history
tables to the new migration_* schema following Cori's architecture.

IMPORTANT: This is a ONE-TIME migration script. Run it after:
1. Creating new migration_* tables (setup/setup_complete.py)
2. Backing up old tables (just in case)

Steps:
1. Read data from old nifi_flows and nifi_conversion_history tables
2. Parse XML files to extract flow_id (UUID from <rootGroup id="...">)
3. Map old status values to new (NEEDS_ATTENTION → NEEDS_HUMAN, CONVERTING → RUNNING)
4. Insert into migration_flows and migration_runs
5. Create placeholder iterations for existing runs
6. Verify data integrity
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration

# COMMAND ----------

# Catalog and Schema
CATALOG = "eliao"
SCHEMA = "nifi_to_databricks"

# Old tables
OLD_FLOWS_TABLE = f"{CATALOG}.{SCHEMA}.nifi_flows"
OLD_HISTORY_TABLE = f"{CATALOG}.{SCHEMA}.nifi_conversion_history"

# New tables
NEW_FLOWS_TABLE = f"{CATALOG}.{SCHEMA}.migration_flows"
NEW_RUNS_TABLE = f"{CATALOG}.{SCHEMA}.migration_runs"
NEW_ITERATIONS_TABLE = f"{CATALOG}.{SCHEMA}.migration_iterations"

print(f"Configuration:")
print(f"  - Old flows table: {OLD_FLOWS_TABLE}")
print(f"  - Old history table: {OLD_HISTORY_TABLE}")
print(f"  - New flows table: {NEW_FLOWS_TABLE}")
print(f"  - New runs table: {NEW_RUNS_TABLE}")
print(f"  - New iterations table: {NEW_ITERATIONS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1: Check Old Tables Exist

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import xml.etree.ElementTree as ET
from pyspark.sql.functions import col, lit, current_timestamp
import hashlib

w = WorkspaceClient()

# Check if old tables exist
try:
    old_flows_df = spark.table(OLD_FLOWS_TABLE)
    old_flows_count = old_flows_df.count()
    print(f"✅ Found {old_flows_count} flows in old table")
except Exception as e:
    print(f"⚠️ Old flows table not found: {e}")
    print("Nothing to migrate. Exiting.")
    dbutils.notebook.exit("Old tables not found")

try:
    old_history_df = spark.table(OLD_HISTORY_TABLE)
    old_history_count = old_history_df.count()
    print(f"✅ Found {old_history_count} history records in old table")
except Exception as e:
    print(f"⚠️ Old history table not found: {e}")
    old_history_count = 0

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2: Define Helper Functions

# COMMAND ----------

def extract_group_id_from_xml(xml_content):
    """
    Extract group ID from NiFi XML.

    Supports two formats:
    1. <template><groupId>uuid</groupId></template>  (NiFi templates)
    2. <rootGroup id="uuid">...</rootGroup>  (NiFi flow definitions)
    """
    try:
        root = ET.fromstring(xml_content)

        # Try format 1: <template><groupId>
        group_id_elem = root.find(".//groupId")
        if group_id_elem is not None and group_id_elem.text:
            return group_id_elem.text.strip()

        # Try format 2: <rootGroup id="...">
        root_group = root.find(".//rootGroup")
        if root_group is not None:
            group_id = root_group.get("id")
            if group_id:
                return group_id

        raise ValueError("No groupId element or rootGroup id attribute found in XML")
    except ET.ParseError as e:
        raise ValueError(f"XML parse error: {e}")


def get_flow_id_from_xml_path(xml_path):
    """
    Get flow_id by parsing the XML file.
    Falls back to hash of flow_name if XML not accessible.
    """
    try:
        content = w.files.download(xml_path).contents.read().decode('utf-8')
        return extract_group_id_from_xml(content)
    except Exception as e:
        print(f"  ⚠️ Could not parse XML at {xml_path}: {e}")
        # Fallback: generate UUID from flow_name hash
        flow_name = xml_path.split('/')[-1].replace('.xml', '')
        return hashlib.md5(flow_name.encode()).hexdigest()


def map_old_status_to_new(old_status):
    """Map old status values to new schema."""
    status_map = {
        'NOT_STARTED': 'NOT_STARTED',
        'CONVERTING': 'RUNNING',
        'DONE': 'DONE',
        'NEEDS_ATTENTION': 'NEEDS_HUMAN',
        'FAILED': 'FAILED'
    }
    return status_map.get(old_status, old_status)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3: Migrate Flows (nifi_flows → migration_flows)

# COMMAND ----------

print("=" * 80)
print("MIGRATING FLOWS")
print("=" * 80)

# Read old flows
old_flows_data = old_flows_df.collect()
migrated_flows = []

for row in old_flows_data:
    flow_name = row['flow_name']
    xml_path = row.get('nifi_xml_path')

    print(f"\nProcessing flow: {flow_name}")

    # Extract flow_id from XML
    if xml_path:
        flow_id = get_flow_id_from_xml_path(xml_path)
        print(f"  ✓ flow_id: {flow_id}")
    else:
        # No XML path - generate from flow_name
        flow_id = hashlib.md5(flow_name.encode()).hexdigest()
        print(f"  ⚠️ No XML path, generated flow_id: {flow_id}")

    # Map old status to new
    old_status = row.get('status', 'NOT_STARTED')
    new_status = map_old_status_to_new(old_status)
    print(f"  ✓ Status: {old_status} → {new_status}")

    migrated_flow = {
        'flow_id': flow_id,
        'flow_name': flow_name,
        'migration_status': new_status,
        'current_run_id': None,  # Will be populated when migrations run
        'server': row.get('server'),
        'nifi_xml_path': xml_path,
        'description': row.get('description'),
        'priority': row.get('priority'),
        'owner': row.get('owner')
    }
    migrated_flows.append(migrated_flow)

print(f"\n✅ Prepared {len(migrated_flows)} flows for migration")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 4: Insert Flows into migration_flows

# COMMAND ----------

if migrated_flows:
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([
        StructField("flow_id", StringType(), False),
        StructField("flow_name", StringType(), False),
        StructField("migration_status", StringType(), False),
        StructField("current_run_id", StringType(), True),
        StructField("server", StringType(), True),
        StructField("nifi_xml_path", StringType(), True),
        StructField("description", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("owner", StringType(), True)
    ])

    migrated_flows_df = spark.createDataFrame(migrated_flows, schema=schema)

    # Show preview
    print("\nPreview of migrated flows:")
    migrated_flows_df.select('flow_id', 'flow_name', 'migration_status').show(10, truncate=False)

    # Merge into migration_flows
    migrated_flows_df.createOrReplaceTempView("migrated_flows_temp")

    spark.sql(f"""
        MERGE INTO {NEW_FLOWS_TABLE} AS target
        USING migrated_flows_temp AS source
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
            flow_id, flow_name, migration_status, current_run_id,
            server, nifi_xml_path, description, priority, owner, last_update_ts
        ) VALUES (
            source.flow_id, source.flow_name, source.migration_status, source.current_run_id,
            source.server, source.nifi_xml_path, source.description, source.priority, source.owner,
            current_timestamp()
        )
    """)

    count = spark.sql(f"SELECT COUNT(*) FROM {NEW_FLOWS_TABLE}").collect()[0][0]
    print(f"\n✅ Migrated flows to migration_flows. Total rows: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 5: Migrate History (nifi_conversion_history → migration_runs)

# COMMAND ----------

if old_history_count > 0:
    print("=" * 80)
    print("MIGRATING HISTORY")
    print("=" * 80)

    # Read old history
    old_history_data = old_history_df.collect()
    migrated_runs = []

    # Create flow_name -> flow_id mapping
    flow_name_to_id = {flow['flow_name']: flow['flow_id'] for flow in migrated_flows}

    for row in old_history_data:
        attempt_id = row.get('attempt_id')
        flow_name = row.get('flow_name')
        old_status = row.get('status', 'FAILED')

        # Map to flow_id
        flow_id = flow_name_to_id.get(flow_name)
        if not flow_id:
            print(f"  ⚠️ Flow not found for history record: {flow_name}")
            continue

        # Map old status to new run status
        status_map = {
            'CREATING': 'CREATING',
            'RUNNING': 'RUNNING',
            'SUCCESS': 'SUCCESS',
            'FAILED': 'FAILED',
            'CANCELLED': 'CANCELLED'
        }
        new_status = status_map.get(old_status, 'FAILED')

        # Use attempt_id as run_id (already has timestamp)
        run_id = attempt_id

        migrated_run = {
            'run_id': run_id,
            'flow_id': flow_id,
            'job_run_id': row.get('job_id'),  # Map job_id to job_run_id
            'start_ts': row.get('started_at'),
            'end_ts': row.get('completed_at'),
            'status': new_status
        }
        migrated_runs.append(migrated_run)

    print(f"\n✅ Prepared {len(migrated_runs)} history records for migration")

    # Insert into migration_runs
    if migrated_runs:
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType

        schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("flow_id", StringType(), False),
            StructField("job_run_id", StringType(), True),
            StructField("start_ts", TimestampType(), True),
            StructField("end_ts", TimestampType(), True),
            StructField("status", StringType(), True)
        ])

        migrated_runs_df = spark.createDataFrame(migrated_runs, schema=schema)

        # Show preview
        print("\nPreview of migrated runs:")
        migrated_runs_df.select('run_id', 'flow_id', 'status').show(10, truncate=False)

        # Insert (MERGE not needed since run_id is unique)
        migrated_runs_df.write.mode("append").saveAsTable(NEW_RUNS_TABLE)

        count = spark.sql(f"SELECT COUNT(*) FROM {NEW_RUNS_TABLE}").collect()[0][0]
        print(f"\n✅ Migrated runs to migration_runs. Total rows: {count}")

else:
    print("No history records to migrate")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 6: Create Placeholder Iterations

# COMMAND ----------

if old_history_count > 0:
    print("=" * 80)
    print("CREATING PLACEHOLDER ITERATIONS")
    print("=" * 80)

    placeholder_iterations = []

    for run in migrated_runs:
        run_id = run['run_id']
        status = run['status']

        # Create a summary iteration based on final status
        if status == 'SUCCESS':
            summary = "Conversion completed (migrated from old schema)"
            step_status = 'COMPLETED'
            error = None
        elif status == 'FAILED':
            summary = "Conversion failed (migrated from old schema)"
            step_status = 'FAILED'
            error = "Migrated from old schema - check original history for details"
        elif status == 'RUNNING':
            summary = "Conversion in progress (migrated from old schema)"
            step_status = 'RUNNING'
            error = None
        else:
            summary = f"Conversion {status.lower()} (migrated from old schema)"
            step_status = 'COMPLETED'
            error = None

        placeholder_iteration = {
            'run_id': run_id,
            'iteration_num': 1,
            'step': 'migration_placeholder',
            'step_status': step_status,
            'summary': summary,
            'error': error
        }
        placeholder_iterations.append(placeholder_iteration)

    # Insert placeholder iterations
    if placeholder_iterations:
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

        schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("iteration_num", IntegerType(), False),
            StructField("step", StringType(), True),
            StructField("step_status", StringType(), True),
            StructField("summary", StringType(), True),
            StructField("error", StringType(), True)
        ])

        iterations_df = spark.createDataFrame(placeholder_iterations, schema=schema)
        iterations_df.write.mode("append").saveAsTable(NEW_ITERATIONS_TABLE)

        count = spark.sql(f"SELECT COUNT(*) FROM {NEW_ITERATIONS_TABLE}").collect()[0][0]
        print(f"\n✅ Created {len(placeholder_iterations)} placeholder iterations. Total rows: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 7: Verify Data Integrity

# COMMAND ----------

print("\n" + "=" * 80)
print("DATA INTEGRITY VERIFICATION")
print("=" * 80)

# Count rows in each table
flow_count = spark.sql(f"SELECT COUNT(*) FROM {NEW_FLOWS_TABLE}").collect()[0][0]
run_count = spark.sql(f"SELECT COUNT(*) FROM {NEW_RUNS_TABLE}").collect()[0][0]
iter_count = spark.sql(f"SELECT COUNT(*) FROM {NEW_ITERATIONS_TABLE}").collect()[0][0]

print(f"\nMigrated data summary:")
print(f"  - Flows: {flow_count} (from {old_flows_count} old flows)")
print(f"  - Runs: {run_count} (from {old_history_count} old history records)")
print(f"  - Iterations: {iter_count}")

# Check for orphaned runs (runs without flows)
orphaned_runs = spark.sql(f"""
    SELECT COUNT(*) as orphan_count
    FROM {NEW_RUNS_TABLE} r
    LEFT JOIN {NEW_FLOWS_TABLE} f ON r.flow_id = f.flow_id
    WHERE f.flow_id IS NULL
""").collect()[0]['orphan_count']

if orphaned_runs > 0:
    print(f"\n⚠️ WARNING: Found {orphaned_runs} orphaned runs (runs without matching flows)")
else:
    print(f"\n✅ No orphaned runs")

# Check for orphaned iterations (iterations without runs)
orphaned_iters = spark.sql(f"""
    SELECT COUNT(*) as orphan_count
    FROM {NEW_ITERATIONS_TABLE} i
    LEFT JOIN {NEW_RUNS_TABLE} r ON i.run_id = r.run_id
    WHERE r.run_id IS NULL
""").collect()[0]['orphan_count']

if orphaned_iters > 0:
    print(f"⚠️ WARNING: Found {orphaned_iters} orphaned iterations (iterations without matching runs)")
else:
    print(f"✅ No orphaned iterations")

# Show status distribution
print("\nFlow status distribution:")
spark.sql(f"""
    SELECT migration_status, COUNT(*) as count
    FROM {NEW_FLOWS_TABLE}
    GROUP BY migration_status
    ORDER BY count DESC
""").show()

print("\nRun status distribution:")
spark.sql(f"""
    SELECT status, COUNT(*) as count
    FROM {NEW_RUNS_TABLE}
    GROUP BY status
    ORDER BY count DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Migration Complete!
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ Migrated flows from old nifi_flows table
# MAGIC ✅ Migrated history from old nifi_conversion_history table
# MAGIC ✅ Created placeholder iterations for historical runs
# MAGIC ✅ Verified data integrity
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Test the Flask app** with the new schema
# MAGIC 2. **Run a test conversion** to ensure end-to-end flow works
# MAGIC 3. **Keep old tables for 1-2 weeks** as backup (don't drop yet)
# MAGIC 4. **After verification**, run `setup/drop_old_tables.py` to clean up

# COMMAND ----------

print("\n" + "=" * 80)
print("🎉 MIGRATION COMPLETE!")
print("=" * 80)
print("\n⚠️ IMPORTANT:")
print("- Old tables (nifi_flows, nifi_conversion_history) are still present")
print("- DO NOT drop them until you've verified the new schema works")
print("- Recommended: Keep for 1-2 weeks, then run setup/drop_old_tables.py")
print("=" * 80)
