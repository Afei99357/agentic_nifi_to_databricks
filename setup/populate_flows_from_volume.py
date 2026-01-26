# Databricks notebook source
"""
Populate nifi_flows table by scanning XML files in Unity Catalog volume.

This notebook scans a Unity Catalog volume for NiFi XML files and populates
the nifi_flows table with metadata extracted from the files.

Usage:
  1. Update VOLUME_PATH variable below to point to your XML files
  2. Run in Databricks notebook

Expected volume structure (optional subdirectories for organizing by environment):
  /Volumes/eliao/nifi_to_databricks/nifi_files/
    ├── prod/
    │   ├── nifi_flow1.xml
    │   └── nifi_flow2.xml
    ├── thailand/
    │   └── nifi_flow_3.xml
    └── ...

Or flat structure (all XMLs in root):
  /Volumes/eliao/nifi_to_databricks/nifi_files/
    ├── nifi_flow1.xml
    ├── nifi_flow2.xml
    └── nifi_flow_3.xml
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Update these if your catalog/schema names or volume path are different:

# COMMAND ----------

# Configuration - Change these if your catalog/schema names are different
CATALOG = "eliao"
SCHEMA = "nifi_to_databricks"
VOLUME_NAME = "nifi_files"

# Derived paths
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"
TABLE_NAME = f"{CATALOG}.{SCHEMA}.nifi_flows"

print(f"Configuration:")
print(f"  - Volume path: {VOLUME_PATH}")
print(f"  - Target table: {TABLE_NAME}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import xml.etree.ElementTree as ET
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# spark is available in Databricks notebook runtime
spark  # type: ignore
w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Debug: Check Volume Contents
# MAGIC
# MAGIC Run this cell to verify the volume exists and see what files are present

# COMMAND ----------

# Debug: List contents of volume to see what's actually there
try:
    print(f"Checking volume: {VOLUME_PATH}")
    items = list(w.files.list_directory_contents(directory_path=VOLUME_PATH))

    if len(items) == 0:
        print("⚠️ Volume is EMPTY - no files or folders found")
    else:
        print(f"Found {len(items)} items in volume:")
        for item in items[:20]:  # Show first 20 items
            item_type = "DIR " if item.is_directory else "FILE"
            print(f"  [{item_type}] {item.path}")
        if len(items) > 20:
            print(f"  ... and {len(items) - 20} more items")

        # Count XML files
        xml_count = sum(1 for item in items if not item.is_directory and item.path.endswith('.xml'))
        print(f"\n📊 Summary: {xml_count} XML files found in root directory")

except Exception as e:
    print(f"❌ Error accessing volume: {e}")
    print(f"   Please verify:")
    print(f"   1. Volume exists: {VOLUME_PATH}")
    print(f"   2. You have read permissions")
    print(f"   3. Path is correct (check catalog.schema.volume names)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def scan_volume_for_xmls(volume_path):
    """Recursively scan volume for ALL XML files in any subdirectory."""
    xml_files = []
    dirs_scanned = 0
    files_checked = 0

    def scan_directory(path, depth=0):
        nonlocal dirs_scanned, files_checked
        try:
            dirs_scanned += 1
            indent = "  " * depth
            print(f"{indent}Scanning: {path}")

            items = list(w.files.list_directory_contents(directory_path=path))
            print(f"{indent}  Found {len(items)} items")

            for item in items:
                if item.is_directory:
                    print(f"{indent}  [DIR] {item.path}")
                    # Recursively scan ALL subdirectories
                    scan_directory(item.path, depth + 1)
                else:
                    files_checked += 1
                    if item.path.endswith('.xml'):
                        print(f"{indent}  [XML] ✓ {item.path}")
                        xml_files.append(item.path)
                    else:
                        # Show non-XML files too for debugging
                        ext = item.path.split('.')[-1] if '.' in item.path else 'no-ext'
                        print(f"{indent}  [.{ext}] {item.path}")

        except Exception as e:
            print(f"{indent}❌ Error scanning {path}: {e}")

    print(f"Starting recursive scan from: {volume_path}\n")
    scan_directory(volume_path)

    print(f"\n{'='*60}")
    print(f"Scan complete:")
    print(f"  - Directories scanned: {dirs_scanned}")
    print(f"  - Files checked: {files_checked}")
    print(f"  - XML files found: {len(xml_files)}")
    print(f"{'='*60}\n")

    return xml_files

# COMMAND ----------

def extract_flow_info(xml_path, volume_name='nifi_files'):
    """Extract flow information from NiFi XML file."""
    try:
        # Extract flow_name from filename (without .xml extension)
        filename = xml_path.split('/')[-1]
        flow_name = filename.replace('.xml', '')

        # Extract server/environment from path (e.g., "prod", "thailand")
        # If file is in subdirectory: /Volumes/.../volume_name/server/file.xml
        # If file is in root: /Volumes/.../volume_name/file.xml
        path_parts = xml_path.split('/')
        server = 'default'  # Default value if no subdirectory
        for i, part in enumerate(path_parts):
            if part == volume_name and i + 1 < len(path_parts):
                potential_server = path_parts[i + 1]
                # If next part is not the filename, it's a subdirectory (server)
                if potential_server != filename:
                    server = potential_server
                break

        # Optionally try to extract description from XML
        description = None
        try:
            content = w.files.download(xml_path).contents.read().decode('utf-8')
            root = ET.fromstring(content)
            for child in root:
                if child.tag == 'comment' or child.tag == 'description':
                    description = child.text
                    break
        except Exception as e:
            print(f"  Warning: Could not parse XML for description: {e}")

        return {
            'flow_name': flow_name,
            'server': server,
            'nifi_xml_path': xml_path,
            'description': description,
            'priority': None,  # Can be set manually later
            'owner': None,     # Can be set manually later
            'status': 'NOT_STARTED',
            'progress_percentage': 0,
            'iterations': 0,
            'validation_percentage': 0,
            'total_attempts': 0,
            'successful_conversions': 0
        }

    except Exception as e:
        print(f"Error processing {xml_path}: {e}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Population Function

# COMMAND ----------

def populate_flows_table():
    """Main function to populate nifi_flows table."""
    print(f"Scanning volume: {VOLUME_PATH}")
    xml_files = scan_volume_for_xmls(VOLUME_PATH)
    print(f"Found {len(xml_files)} XML files")

    if len(xml_files) == 0:
        print("No XML files found. Check VOLUME_PATH.")
        return

    # Extract flow info from each XML
    flows = []
    for i, xml_path in enumerate(xml_files, 1):
        print(f"Processing {i}/{len(xml_files)}: {xml_path}")
        flow_info = extract_flow_info(xml_path, VOLUME_NAME)
        if flow_info:
            flows.append(flow_info)

    print(f"Successfully parsed {len(flows)} flows")

    # Define explicit schema to handle nullable fields
    schema = StructType([
        StructField("flow_name", StringType(), False),
        StructField("server", StringType(), False),
        StructField("nifi_xml_path", StringType(), False),
        StructField("description", StringType(), True),  # Nullable
        StructField("priority", StringType(), True),  # Nullable
        StructField("owner", StringType(), True),  # Nullable
        StructField("status", StringType(), False),
        StructField("progress_percentage", IntegerType(), False),
        StructField("iterations", IntegerType(), False),
        StructField("validation_percentage", IntegerType(), False),
        StructField("total_attempts", IntegerType(), False),
        StructField("successful_conversions", IntegerType(), False)
    ])

    # Create DataFrame with explicit schema
    # type: ignore
    flows_df = spark.createDataFrame(flows, schema=schema)

    # Show preview
    print("\nPreview of flows to be inserted:")
    flows_df.select('flow_name', 'server', 'nifi_xml_path').show(5, truncate=False)

    # Insert into Delta table (append mode to avoid duplicates if run multiple times)
    # Use merge to handle duplicates based on flow_name
    flows_df.createOrReplaceTempView("new_flows")

    # type: ignore
    spark.sql(f"""
        MERGE INTO {TABLE_NAME} AS target
        USING new_flows AS source
        ON target.flow_name = source.flow_name
        WHEN MATCHED THEN UPDATE SET
            server = source.server,
            nifi_xml_path = source.nifi_xml_path,
            description = source.description,
            priority = source.priority,
            owner = source.owner,
            status = source.status,
            progress_percentage = source.progress_percentage,
            iterations = source.iterations,
            validation_percentage = source.validation_percentage,
            total_attempts = source.total_attempts,
            successful_conversions = source.successful_conversions,
            last_updated = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            flow_name,
            server,
            nifi_xml_path,
            description,
            priority,
            owner,
            status,
            progress_percentage,
            iterations,
            validation_percentage,
            total_attempts,
            successful_conversions
        ) VALUES (
            source.flow_name,
            source.server,
            source.nifi_xml_path,
            source.description,
            source.priority,
            source.owner,
            source.status,
            source.progress_percentage,
            source.iterations,
            source.validation_percentage,
            source.total_attempts,
            source.successful_conversions
        )
    """)

    print(f"\n✅ Successfully populated {len(flows)} flows into {TABLE_NAME}")

    # Verify
    # type: ignore
    count = spark.sql(f"SELECT COUNT(*) FROM {TABLE_NAME}").collect()[0][0]
    print(f"Total flows in table: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Population
# MAGIC
# MAGIC Execute the main population function to scan and load flows

# COMMAND ----------

populate_flows_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Verification
# MAGIC
# MAGIC Show summary by server/environment

# COMMAND ----------

# type: ignore
spark.sql(f"""
    SELECT
        server,
        COUNT(*) as flow_count,
        SUM(CASE WHEN status = 'NOT_STARTED' THEN 1 ELSE 0 END) as not_started
    FROM {TABLE_NAME}
    GROUP BY server
    ORDER BY flow_count DESC
""").show()
