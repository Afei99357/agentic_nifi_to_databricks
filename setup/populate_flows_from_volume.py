"""
Populate nifi_flows table by scanning XML files in Unity Catalog volume.

Usage:
  1. Update VOLUME_PATH variable below to point to your XML files
  2. Run in Databricks notebook

Expected volume structure:
  /Volumes/main/default/nifi_xmls/
    ├── prod/
    │   ├── nifi_flow1.xml
    │   └── nifi_flow2.xml
    ├── thailand/
    │   └── nifi_flow_3.xml
    └── ...
"""

from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
import xml.etree.ElementTree as ET
from datetime import datetime
import re

# Configuration
VOLUME_PATH = "/Volumes/main/default/nifi_xmls"
TABLE_NAME = "main.default.nifi_flows"

# Initialize
spark = SparkSession.builder.getOrCreate()
w = WorkspaceClient()

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

def extract_flow_info(xml_path):
    """Extract flow information from NiFi XML file."""
    try:
        # Read XML content
        content = w.files.download(xml_path).contents.read().decode('utf-8')
        root = ET.fromstring(content)

        # Extract flow name (from root processGroup name attribute or filename)
        flow_name = root.get('name')
        if not flow_name:
            # Fall back to filename without extension
            flow_name = xml_path.split('/')[-1].replace('.xml', '')

        # Extract server/environment from path (e.g., "prod", "thailand")
        path_parts = xml_path.split('/')
        server = 'unknown'
        for i, part in enumerate(path_parts):
            if part == 'nifi_xmls' and i + 1 < len(path_parts):
                server = path_parts[i + 1]
                break

        # Try to extract description from comment or position tag
        description = None
        for child in root:
            if child.tag == 'comment' or child.tag == 'description':
                description = child.text
                break

        # Generate flow_id from path (sanitized)
        flow_id = re.sub(r'[^a-z0-9_]', '_', xml_path.lower().replace('.xml', '').split('/')[-1])

        return {
            'flow_id': flow_id,
            'flow_name': flow_name,
            'server': server,
            'nifi_xml_path': xml_path,
            'description': description,
            'priority': None,  # Can be set manually later
            'owner': None,     # Can be set manually later
            'status': 'NOT_STARTED'
        }

    except Exception as e:
        print(f"Error parsing {xml_path}: {e}")
        return None

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
        flow_info = extract_flow_info(xml_path)
        if flow_info:
            flows.append(flow_info)

    print(f"Successfully parsed {len(flows)} flows")

    # Create DataFrame
    flows_df = spark.createDataFrame(flows)

    # Show preview
    print("\nPreview of flows to be inserted:")
    flows_df.select('flow_id', 'flow_name', 'server', 'nifi_xml_path').show(5, truncate=False)

    # Insert into Delta table (append mode to avoid duplicates if run multiple times)
    # Use merge to handle duplicates based on flow_id
    flows_df.createOrReplaceTempView("new_flows")

    spark.sql(f"""
        MERGE INTO {TABLE_NAME} AS target
        USING new_flows AS source
        ON target.flow_id = source.flow_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"\n✅ Successfully populated {len(flows)} flows into {TABLE_NAME}")

    # Verify
    count = spark.sql(f"SELECT COUNT(*) FROM {TABLE_NAME}").collect()[0][0]
    print(f"Total flows in table: {count}")

# Run the population
if __name__ == "__main__":
    populate_flows_table()

# Verify the results
print("\n--- Final Verification ---")
spark.sql(f"""
    SELECT
        server,
        COUNT(*) as flow_count,
        SUM(CASE WHEN status = 'NOT_STARTED' THEN 1 ELSE 0 END) as not_started
    FROM {TABLE_NAME}
    GROUP BY server
    ORDER BY flow_count DESC
""").show()
