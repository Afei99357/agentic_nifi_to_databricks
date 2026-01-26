# Databricks notebook source
"""
Dummy NiFi Conversion Agent for Testing Dynamic Job Creation

This notebook simulates the behavior of a real AI agent that converts NiFi flows
to Databricks notebooks. It's used for testing the dynamic job creation and
history tracking system.

Expected Parameters:
- flow_id: Flow identifier
- nifi_xml_path: Path to NiFi XML file in UC volume
- output_path: Where to save generated notebooks
- attempt_id: Unique attempt identifier for history tracking
- delta_table: History table name (default: main.default.nifi_conversion_history)

The real agent (to be provided by agent team) will replace this with actual
LLM-based conversion logic.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameter Setup

# COMMAND ----------

import time
import os
from datetime import datetime
from pyspark.sql import SparkSession

# Get parameters (passed by Databricks job)
dbutils.widgets.text("flow_id", "", "Flow ID")
dbutils.widgets.text("nifi_xml_path", "", "NiFi XML Path")
dbutils.widgets.text("output_path", "", "Output Path")
dbutils.widgets.text("attempt_id", "", "Attempt ID")
dbutils.widgets.text("delta_table", "main.default.nifi_conversion_history", "History Table")

flow_id = dbutils.widgets.get("flow_id")
nifi_xml_path = dbutils.widgets.get("nifi_xml_path")
output_path = dbutils.widgets.get("output_path")
attempt_id = dbutils.widgets.get("attempt_id")
delta_table = dbutils.widgets.get("delta_table")

print(f"Starting conversion for flow: {flow_id}")
print(f"XML Path: {nifi_xml_path}")
print(f"Output Path: {output_path}")
print(f"Attempt ID: {attempt_id}")
print(f"History Table: {delta_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validation

# COMMAND ----------

# Validate required parameters
if not all([flow_id, nifi_xml_path, output_path, attempt_id]):
    raise ValueError("Missing required parameters. All of flow_id, nifi_xml_path, output_path, and attempt_id are required.")

# Validate XML file exists (if path is in volume format)
# For now, just log the path - real agent would read and parse the XML
print(f"Validating NiFi XML at: {nifi_xml_path}")

# Update status to RUNNING
spark = SparkSession.builder.getOrCreate()
spark.sql(f"""
    UPDATE {delta_table}
    SET
        status = 'RUNNING',
        status_message = 'Validation complete, starting conversion',
        progress_percentage = 10
    WHERE attempt_id = '{attempt_id}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Simulated Conversion Process

# COMMAND ----------

def update_progress(progress_pct, status_msg, iterations=0):
    """Update progress in history table."""
    spark.sql(f"""
        UPDATE {delta_table}
        SET
            progress_percentage = {progress_pct},
            status_message = '{status_msg}',
            iterations = {iterations}
        WHERE attempt_id = '{attempt_id}'
    """)
    print(f"Progress: {progress_pct}% - {status_msg}")

# Phase 1: Analyzing NiFi Flow (25%)
print("\n=== Phase 1: Analyzing NiFi Flow ===")
update_progress(25, "Analyzing NiFi flow structure", iterations=1)
time.sleep(5)  # Simulate processing time

# Phase 2: Generating Databricks Code (50%)
print("\n=== Phase 2: Generating Databricks Code ===")
update_progress(50, "Generating PySpark code from NiFi processors", iterations=2)
time.sleep(5)

# Phase 3: Creating Notebooks (75%)
print("\n=== Phase 3: Creating Notebooks ===")
update_progress(75, "Creating notebook files", iterations=3)
time.sleep(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate Dummy Notebooks

# COMMAND ----------

# Create output directory if it doesn't exist
try:
    # For volume paths, create using dbutils
    if output_path.startswith("/Volumes/"):
        # Create directory structure
        print(f"Creating output directory: {output_path}")
        # Note: Volumes are automatically created in Databricks
    else:
        # For DBFS paths
        dbutils.fs.mkdirs(output_path)
except Exception as e:
    print(f"Note: Output path may already exist or will be created: {e}")

# Generate 3 dummy notebooks
generated_notebooks = []
notebook_templates = [
    ("01_ingestion", "# NiFi Ingestion Converted\n\n# This notebook handles data ingestion\ndf = spark.read.format('delta').load('/path/to/source')\ndisplay(df)"),
    ("02_transformation", "# NiFi Transformation Converted\n\n# This notebook handles data transformation\nfrom pyspark.sql.functions import col\ndf_transformed = df.withColumn('processed_at', current_timestamp())\ndisplay(df_transformed)"),
    ("03_output", "# NiFi Output Converted\n\n# This notebook handles data output\ndf_transformed.write.format('delta').mode('overwrite').save('/path/to/output')")
]

for notebook_name, content in notebook_templates:
    notebook_path = f"{output_path}/{notebook_name}.py"

    # In a real implementation, this would write to Unity Catalog volumes
    # For this dummy, we just create the path reference
    generated_notebooks.append(notebook_path)
    print(f"Generated: {notebook_path}")

    # Optionally, write actual files if output_path is writable
    try:
        if output_path.startswith("/Volumes/"):
            # Write to volume
            with open(f"/Volumes/{output_path.split('/Volumes/')[1]}/{notebook_name}.py", "w") as f:
                f.write(content)
        else:
            # Write to DBFS
            dbutils.fs.put(notebook_path, content, overwrite=True)
    except Exception as e:
        print(f"Note: Could not write file (expected in test environment): {e}")

update_progress(90, "Notebooks generated, finalizing", iterations=4)
time.sleep(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Finalize and Update History

# COMMAND ----------

# Calculate duration
end_time = datetime.now()

# Prepare notebook array for SQL
notebooks_sql = ", ".join([f"'{nb}'" for nb in generated_notebooks])

# Update history table with success
spark.sql(f"""
    UPDATE {delta_table}
    SET
        status = 'SUCCESS',
        progress_percentage = 100,
        status_message = 'Conversion completed successfully',
        completed_at = current_timestamp(),
        generated_notebooks = ARRAY({notebooks_sql}),
        iterations = 5,
        validation_percentage = 95
    WHERE attempt_id = '{attempt_id}'
""")

print("\n=== Conversion Complete ===")
print(f"Status: SUCCESS")
print(f"Generated {len(generated_notebooks)} notebooks:")
for nb in generated_notebooks:
    print(f"  - {nb}")

# Return success
dbutils.notebook.exit(f"SUCCESS: Generated {len(generated_notebooks)} notebooks for {flow_id}")
