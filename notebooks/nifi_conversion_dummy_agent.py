# Databricks notebook source
"""
Dummy NiFi Conversion Agent Notebook (Testing)

This is a placeholder notebook that simulates the NiFi-to-Databricks conversion process.
Used for testing dynamic job creation and history tracking before the real MLflow agent is ready.

Expected Parameters:
  - flow_id: Flow identifier
  - nifi_xml_path: Path to NiFi XML file in Unity Catalog volume
  - output_path: Where to save generated notebooks
  - attempt_id: Unique attempt identifier for history tracking
  - delta_table: History table name (e.g., "main.default.nifi_conversion_history")
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Parameters

# COMMAND ----------

# Get job parameters
dbutils.widgets.text("flow_id", "", "Flow ID")
dbutils.widgets.text("nifi_xml_path", "", "NiFi XML Path")
dbutils.widgets.text("output_path", "", "Output Path")
dbutils.widgets.text("attempt_id", "", "Attempt ID")
dbutils.widgets.text("delta_table", "main.default.nifi_conversion_history", "Delta Table")

flow_id = dbutils.widgets.get("flow_id")
nifi_xml_path = dbutils.widgets.get("nifi_xml_path")
output_path = dbutils.widgets.get("output_path")
attempt_id = dbutils.widgets.get("attempt_id")
delta_table = dbutils.widgets.get("delta_table")

print("=" * 60)
print("Dummy NiFi Conversion Agent Starting")
print("=" * 60)
print(f"Flow ID: {flow_id}")
print(f"NiFi XML Path: {nifi_xml_path}")
print(f"Output Path: {output_path}")
print(f"Attempt ID: {attempt_id}")
print(f"Delta Table: {delta_table}")
print("=" * 60)

# Validate required parameters
if not flow_id or not nifi_xml_path or not output_path or not attempt_id:
    raise ValueError("Missing required parameters. All of flow_id, nifi_xml_path, output_path, and attempt_id are required.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate XML File Exists

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import time

w = WorkspaceClient()

print(f"\n[Step 1/5] Validating XML file exists: {nifi_xml_path}")
try:
    # Try to download file to verify it exists
    file_content = w.files.download(nifi_xml_path).contents.read()
    file_size = len(file_content)
    print(f"✅ XML file exists ({file_size:,} bytes)")

    # Update history: RUNNING status
    spark.sql(f"""
        UPDATE {delta_table}
        SET
            status = 'RUNNING',
            progress_percentage = 10,
            status_message = 'Validated XML file exists'
        WHERE attempt_id = '{attempt_id}'
    """)

except Exception as e:
    print(f"❌ XML file not found or not accessible: {e}")
    # Update history: FAILED
    error_msg = str(e).replace("'", "''")
    spark.sql(f"""
        UPDATE {delta_table}
        SET
            status = 'FAILED',
            error_message = 'XML file not found: {error_msg}',
            completed_at = current_timestamp()
        WHERE attempt_id = '{attempt_id}'
    """)
    dbutils.notebook.exit(f"FAILED: XML file not found - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate XML Parsing

# COMMAND ----------

print(f"\n[Step 2/5] Parsing NiFi XML (simulated)")
time.sleep(5)  # Simulate parsing time

print("✅ Parsed NiFi flow definition")
print("  - Found 12 processors (simulated)")
print("  - Found 8 connections (simulated)")
print("  - Found 3 controller services (simulated)")

# Update progress
spark.sql(f"""
    UPDATE {delta_table}
    SET
        progress_percentage = 25,
        iterations = 1,
        status_message = 'Parsed NiFi XML successfully'
    WHERE attempt_id = '{attempt_id}'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulate Agent Conversion

# COMMAND ----------

print(f"\n[Step 3/5] Converting NiFi flow to Databricks notebooks (simulated)")

# Simulate multiple iterations
iterations = 3
for i in range(1, iterations + 1):
    print(f"\n  Iteration {i}/{iterations}:")
    print(f"    - Analyzing processor dependencies...")
    time.sleep(3)
    print(f"    - Generating PySpark code...")
    time.sleep(3)
    print(f"    - Validating generated code...")
    time.sleep(2)

    progress = 25 + (i * 20)
    validation = min(i * 30, 90)

    spark.sql(f"""
        UPDATE {delta_table}
        SET
            progress_percentage = {progress},
            iterations = {i},
            validation_percentage = {validation},
            status_message = 'Iteration {i}/{iterations} completed'
        WHERE attempt_id = '{attempt_id}'
    """)

print(f"\n✅ Conversion iterations complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Dummy Output Notebooks

# COMMAND ----------

print(f"\n[Step 4/5] Generating output notebooks")

# Create output directory if it doesn't exist
try:
    w.files.create_directory(output_path)
    print(f"✅ Created output directory: {output_path}")
except Exception as e:
    print(f"  Note: Directory may already exist: {e}")

# Generate dummy notebook files
generated_notebooks = []

notebook_templates = [
    {
        "name": f"{flow_id}_ingestion.py",
        "content": f"""# Databricks notebook source
# DUMMY NOTEBOOK - Generated by test agent
# Flow: {flow_id}
# Purpose: Data Ingestion

from pyspark.sql import SparkSession

# This is a placeholder notebook
print("Ingestion logic would go here")
"""
    },
    {
        "name": f"{flow_id}_transformation.py",
        "content": f"""# Databricks notebook source
# DUMMY NOTEBOOK - Generated by test agent
# Flow: {flow_id}
# Purpose: Data Transformation

from pyspark.sql.functions import col, when

# This is a placeholder notebook
print("Transformation logic would go here")
"""
    },
    {
        "name": f"{flow_id}_output.py",
        "content": f"""# Databricks notebook source
# DUMMY NOTEBOOK - Generated by test agent
# Flow: {flow_id}
# Purpose: Data Output

# This is a placeholder notebook
print("Output logic would go here")
"""
    }
]

for notebook in notebook_templates:
    notebook_path = f"{output_path}/{notebook['name']}"
    try:
        # Upload notebook content
        w.files.upload(notebook_path, notebook['content'].encode('utf-8'), overwrite=True)
        generated_notebooks.append(notebook_path)
        print(f"  ✅ Created: {notebook_path}")
    except Exception as e:
        print(f"  ⚠️ Warning: Could not create {notebook_path}: {e}")

print(f"\n✅ Generated {len(generated_notebooks)} notebooks")

# Update progress
spark.sql(f"""
    UPDATE {delta_table}
    SET
        progress_percentage = 90,
        status_message = 'Generated output notebooks'
    WHERE attempt_id = '{attempt_id}'
""")

time.sleep(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finalize and Mark Complete

# COMMAND ----------

print(f"\n[Step 5/5] Finalizing conversion")

# Prepare generated notebooks array for SQL
if generated_notebooks:
    notebooks_array = "ARRAY(" + ", ".join([f"'{nb}'" for nb in generated_notebooks]) + ")"
else:
    notebooks_array = "ARRAY()"

# Mark conversion as complete
spark.sql(f"""
    UPDATE {delta_table}
    SET
        status = 'SUCCESS',
        progress_percentage = 100,
        validation_percentage = 100,
        completed_at = current_timestamp(),
        generated_notebooks = {notebooks_array},
        status_message = 'Conversion completed successfully (dummy agent)'
    WHERE attempt_id = '{attempt_id}'
""")

print("=" * 60)
print("✅ DUMMY CONVERSION COMPLETE")
print("=" * 60)
print(f"Attempt ID: {attempt_id}")
print(f"Generated Notebooks: {len(generated_notebooks)}")
for nb in generated_notebooks:
    print(f"  - {nb}")
print("=" * 60)
print("\n⚠️ NOTE: This was a SIMULATED conversion using dummy agent")
print("Real conversions will use the MLflow agent application")
print("=" * 60)

# Exit successfully
dbutils.notebook.exit(f"SUCCESS: Generated {len(generated_notebooks)} notebooks for {flow_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Display final status (only runs if notebook doesn't exit above)
spark.sql(f"""
    SELECT
        attempt_id,
        flow_id,
        status,
        progress_percentage,
        iterations,
        validation_percentage,
        started_at,
        completed_at,
        CAST((unix_timestamp(completed_at) - unix_timestamp(started_at)) AS INT) as duration_seconds
    FROM {delta_table}
    WHERE attempt_id = '{attempt_id}'
""").show(truncate=False)
