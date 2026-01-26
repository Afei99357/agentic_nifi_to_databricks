# Databricks notebook source
"""
Populate nifi_flows table by scanning XML files in Unity Catalog volume.

This notebook scans a Unity Catalog volume for NiFi XML files and populates
the nifi_flows table with metadata extracted from the files.

Usage:
  1. Update VOLUME_PATH variable below to point to your XML files
  2. Run in Databricks notebook

Expected volume structure:
  /Volumes/eliao/nifi_to_databricks/nifi_xmls/
    ├── prod/
    │   ├── nifi_flow1.xml
    │   └── nifi_flow2.xml
    ├── thailand/
    │   └── nifi_flow_3.xml
    └── ...
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
VOLUME_NAME = "nifi_xmls"

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

# spark is available in Databricks notebook runtime
spark  # type: ignore
w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def scan_volume_for_xmls(volume_path):
    """Recursively scan volume for XML files."""
    xml_files = []

    def scan_directory(path):
        try:
            items = w.files.list_directory_contents(directory_path=path)
            for item in items:
                if item.is_directory:
                    # Recursively scan subdirectories
                    scan_directory(item.path)
                elif item.path.endswith('.xml'):
                    xml_files.append(item.path)
        except Exception as e:
            print(f"Error scanning {path}: {e}")

    scan_directory(volume_path)
    return xml_files

# COMMAND ----------

def extract_flow_info(xml_path, volume_name='nifi_xmls'):
    """Extract flow information from NiFi XML file."""
    try:
        # Extract flow_name from filename (without .xml extension)
        filename = xml_path.split('/')[-1]
        flow_name = filename.replace('.xml', '')

        # Extract server/environment from path (e.g., "prod", "thailand")
        path_parts = xml_path.split('/')
        server = 'unknown'
        for i, part in enumerate(path_parts):
            if part == volume_name and i + 1 < len(path_parts):
                server = path_parts[i + 1]
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

    # Create DataFrame
    # type: ignore
    flows_df = spark.createDataFrame(flows)

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
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
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
