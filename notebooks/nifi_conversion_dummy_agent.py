# Databricks notebook source
"""
NiFi Conversion Agent Notebook (Intent-Based Architecture)

This notebook converts NiFi flows to Databricks notebooks with intent-based folder structure.
It parses NiFi XML to extract processors, categorizes them by intent (ingestion, transformation,
routing, output, etc.), and generates test notebooks organized by intent.

Updated to use new migration_* schema following Cori's architecture:
  - "The App should never 'compute' progress. It should display the latest rows."
  - Agent writes progress to migration_iterations table (not direct SQL to old table)
  - Structured output with patches, iterations, and execution results

Expected Parameters:
  - run_id: Migration run identifier (format: "{flow_id}_run_{timestamp}")
  - flow_id: Flow UUID from XML <rootGroup id="...">
  - flow_name: Flow name (for display/folder name)
  - nifi_xml_path: Path to NiFi XML file in Unity Catalog volume
  - output_path: Where to save generated notebooks
  - catalog: Catalog name for migration tables (default: eliao)
  - schema: Schema name for migration tables (default: nifi_to_databricks)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Get Parameters

# COMMAND ----------

# Get job parameters
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("flow_id", "", "Flow ID")
dbutils.widgets.text("flow_name", "", "Flow Name")
dbutils.widgets.text("nifi_xml_path", "", "NiFi XML Path")
dbutils.widgets.text("output_path", "", "Output Path")
dbutils.widgets.text("catalog", "eliao", "Catalog")
dbutils.widgets.text("schema", "nifi_to_databricks", "Schema")
dbutils.widgets.text("iteration_num", "1", "Iteration Number")
dbutils.widgets.text("logs_path", "", "Logs Path (for iteration 2+)")
dbutils.widgets.text("previous_notebooks", "", "Previous Notebooks (comma-separated)")

run_id = dbutils.widgets.get("run_id")
flow_id = dbutils.widgets.get("flow_id")
flow_name = dbutils.widgets.get("flow_name")
nifi_xml_path = dbutils.widgets.get("nifi_xml_path")
output_path = dbutils.widgets.get("output_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
iteration_num = int(dbutils.widgets.get("iteration_num") or "1")
logs_path = dbutils.widgets.get("logs_path") or None
previous_notebooks_str = dbutils.widgets.get("previous_notebooks") or ""
previous_notebooks = [nb.strip() for nb in previous_notebooks_str.split(",") if nb.strip()]

print("=" * 60)
print("NiFi Conversion Agent Starting")
print("=" * 60)
print(f"Run ID: {run_id}")
print(f"Flow ID: {flow_id}")
print(f"Flow Name: {flow_name}")
print(f"Iteration: {iteration_num}")
print(f"Mode: {'GENERATION' if iteration_num == 1 else 'ERROR FIXING'}")
if logs_path:
    print(f"Logs Path: {logs_path}")
if previous_notebooks:
    print(f"Previous Notebooks: {len(previous_notebooks)} to fix")
print(f"NiFi XML Path: {nifi_xml_path}")
print(f"Output Path: {output_path}")
print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print("=" * 60)

# Validate required parameters
if not run_id or not flow_id or not flow_name:
    raise ValueError("Missing required parameters: run_id, flow_id, and flow_name are required.")

# For iteration 1, need nifi_xml_path
if iteration_num == 1 and not nifi_xml_path:
    raise ValueError("nifi_xml_path is required for iteration 1 (generation)")

# For iteration 2+, need logs_path
if iteration_num > 1 and not logs_path:
    raise ValueError("logs_path is required for iteration 2+ (error fixing)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState, ExecuteStatementRequest

w = WorkspaceClient()

# Processor intent mapping (categorize NiFi processors)
INTENT_MAPPING = {
    # Ingestion processors (source data)
    'GetFile': 'ingestion',
    'GetHTTP': 'ingestion',
    'GetSFTP': 'ingestion',
    'GetFTP': 'ingestion',
    'GetDatabase': 'ingestion',
    'ConsumeKafka': 'ingestion',
    'ConsumeKafka_2_0': 'ingestion',
    'ListS3': 'ingestion',
    'FetchS3Object': 'ingestion',
    'GetJMSQueue': 'ingestion',

    # Transformation processors
    'UpdateAttribute': 'transformation',
    'ExecuteStreamCommand': 'transformation',
    'ExecuteScript': 'transformation',
    'ExecuteProcess': 'transformation',
    'JoltTransformJSON': 'transformation',
    'ConvertRecord': 'transformation',
    'ConvertJSONToSQL': 'transformation',
    'ReplaceText': 'transformation',
    'ModifyBytes': 'transformation',
    'SplitText': 'transformation',
    'MergeContent': 'transformation',

    # Routing processors
    'RouteOnAttribute': 'routing',
    'RouteOnContent': 'routing',
    'DistributeLoad': 'routing',
    'RouteText': 'routing',

    # Flow control
    'ControlRate': 'flow_control',
    'Wait': 'flow_control',
    'Notify': 'flow_control',
    'MonitorActivity': 'flow_control',

    # Output processors (write data)
    'PutFile': 'output',
    'PutHDFS': 'output',
    'PutS3Object': 'output',
    'PutDatabase': 'output',
    'PutDatabaseRecord': 'output',
    'PutKafka': 'output',
    'PutEmail': 'output',
    'PutFTP': 'output',
    'PutSFTP': 'output',

    # Monitoring/debugging
    'LogMessage': 'monitoring',
    'LogAttribute': 'monitoring'
}

def categorize_processor(processor_type: str) -> str:
    """
    Categorize processor by type.

    Args:
        processor_type: Full class name like 'org.apache.nifi.processors.standard.RouteOnAttribute'

    Returns:
        Intent category: 'ingestion', 'transformation', 'routing', 'output', etc.
        Returns 'other' if not recognized.
    """
    class_name = processor_type.split('.')[-1]  # Extract 'RouteOnAttribute'
    return INTENT_MAPPING.get(class_name, 'other')

def add_iteration(iteration_num: int, step: str, step_status: str, summary: str, error: str = None):
    """Write iteration to migration_iterations table."""
    error_sql = f"'{error.replace(chr(39), chr(39)*2)}'" if error else 'NULL'
    summary_sql = f"'{summary.replace(chr(39), chr(39)*2)}'" if summary else 'NULL'

    sql = f"""
        INSERT INTO {catalog}.{schema}.migration_iterations
        (run_id, iteration_num, step, step_status, ts, summary, error)
        VALUES (
            '{run_id}',
            {iteration_num},
            '{step}',
            '{step_status}',
            current_timestamp(),
            {summary_sql},
            {error_sql}
        )
    """

    try:
        spark.sql(sql)
    except Exception as e:
        print(f"Warning: Could not write iteration to DB: {e}")

def add_patch(iteration_num: int, artifact_ref: str, patch_summary: str):
    """Write patch (generated notebook) to migration_patches table."""
    summary_sql = f"'{patch_summary.replace(chr(39), chr(39)*2)}'" if patch_summary else 'NULL'

    sql = f"""
        INSERT INTO {catalog}.{schema}.migration_patches
        (run_id, iteration_num, artifact_ref, patch_summary, ts)
        VALUES (
            '{run_id}',
            {iteration_num},
            '{artifact_ref}',
            {summary_sql},
            current_timestamp()
        )
    """

    try:
        spark.sql(sql)
    except Exception as e:
        print(f"Warning: Could not write patch to DB: {e}")

def generate_test_notebook(flow_name: str, processor_name: str, processor_type: str, intent: str) -> str:
    """
    Generate a simple test notebook that prints flow and processor info.

    This is for testing purposes - real agent will generate actual conversion code.
    """
    safe_proc_name = processor_name.replace("-", "_").replace(" ", "_")

    return f'''# Databricks notebook source
"""
Generated Test Notebook
Flow: {flow_name}
Processor: {processor_name}
Type: {processor_type}
Intent: {intent}

This is a test notebook generated by the dummy agent.
Real implementation will contain actual NiFi processor conversion logic.
"""

# COMMAND ----------

def process_{safe_proc_name}():
    """
    Test function for {processor_name} processor.
    """
    print("=" * 60)
    print(f"Flow Name: {flow_name}")
    print(f"Processor: {processor_name}")
    print(f"Type: {processor_type}")
    print(f"Intent Category: {intent}")
    print("=" * 60)
    print()
    print("✅ Test notebook executed successfully")
    print("📝 Real implementation will process data here")
    return True

# COMMAND ----------

# Execute the processor function
result = process_{safe_proc_name}()
print(f"Result: {{result}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook represents the conversion of NiFi processor **{processor_name}** to Databricks.
# MAGIC
# MAGIC **Next Steps for Real Implementation:**
# MAGIC - Parse processor configuration from XML
# MAGIC - Convert NiFi properties to PySpark code
# MAGIC - Handle input/output connections
# MAGIC - Implement error handling and validation
'''

def create_intent_folders(base_output_path: str, flow_name: str, intents: list) -> dict:
    """
    Create folder structure based on processor intents.

    Args:
        base_output_path: Base UC volume path
        flow_name: Flow name (creates subfolder)
        intents: List of unique intent categories found in flow

    Returns:
        dict mapping intent -> folder path
    """
    flow_path = f"{base_output_path}/{flow_name}"

    # Create flow root folder
    try:
        w.files.create_directory(flow_path)
        print(f"✅ Created flow folder: {flow_path}")
    except Exception as e:
        print(f"  Note: Flow folder may exist: {e}")

    # Create intent subfolders
    intent_paths = {}
    for intent in intents:
        intent_path = f"{flow_path}/{intent}"
        try:
            w.files.create_directory(intent_path)
            intent_paths[intent] = intent_path
            print(f"  ✅ Created folder: {intent_path}")
        except Exception as e:
            print(f"  ⚠️  Folder may exist: {intent_path}")
            intent_paths[intent] = intent_path

    return intent_paths

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conditional Logic: Generation vs Error Fixing

# COMMAND ----------

# Check if this is iteration 1 (generation) or iteration 2+ (error fixing)
if iteration_num == 1:
    print("\n🔧 MODE: GENERATION (Iteration 1)")
    print("Task: Generate notebooks from NiFi XML")
else:
    print(f"\n🔧 MODE: ERROR FIXING (Iteration {iteration_num})")
    print("Task: Analyze execution logs and fix errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Parse NiFi XML or Read Logs

# COMMAND ----------

import xml.etree.ElementTree as ET
import json

# Determine starting iteration step number based on mode
step_iteration_num = 1

if iteration_num > 1:
    # ITERATION 2+: Read and analyze logs
    print(f"\n[Step 1/4] Reading execution logs from: {logs_path}")

    try:
        # Read log files from UC volume
        log_files = []
        try:
            files = w.files.list_directory_contents(logs_path)
            log_files = [f.path for f in files if f.path.endswith('.log')]
        except Exception as e:
            print(f"⚠️  Could not list log files: {e}")
            # For dummy agent, simulate no logs found
            log_files = []

        if not log_files:
            print("⚠️  No log files found. This is a test - simulating error analysis.")

            # Dummy agent: Simulate that we found errors but can't fix them
            # Real agent would use LLM to analyze logs and determine fixability

            # For now, mark as NEEDS_HUMAN on iteration 2+
            if iteration_num >= 2:
                print("❌ Dummy agent cannot analyze logs. Real implementation would:")
                print("   1. Read log files from UC volume")
                print("   2. Parse error messages and stack traces")
                print("   3. Use LLM to categorize error types")
                print("   4. Determine if errors are fixable")
                print("   5. Generate fixes or return NEEDS_HUMAN")

                add_iteration(step_iteration_num, 'read_logs', 'COMPLETED', 'Read logs (dummy - no real analysis)')

                # Exit with NEEDS_HUMAN
                result = {
                    "status": "NEEDS_HUMAN",
                    "iteration_num": iteration_num,
                    "reason": "dummy_agent_limitation",
                    "error_details": "Dummy agent cannot analyze logs or fix errors. Real implementation needed.",
                    "next_action": "HUMAN_REVIEW"
                }

                dbutils.notebook.exit(json.dumps(result))

        # If we had logs, we'd analyze them here
        print(f"✅ Found {len(log_files)} log files")
        add_iteration(step_iteration_num, 'read_logs', 'COMPLETED', f'Read {len(log_files)} log files')

    except Exception as e:
        error_msg = f"Failed to read logs: {str(e)}"
        print(f"❌ {error_msg}")
        add_iteration(step_iteration_num, 'read_logs', 'FAILED', None, error_msg)
        dbutils.notebook.exit(json.dumps({
            "status": "NEEDS_HUMAN",
            "iteration_num": iteration_num,
            "reason": "log_read_failed",
            "error_details": error_msg,
            "next_action": "HUMAN_REVIEW"
        }))

else:
    # ITERATION 1: Parse NiFi XML
    print(f"\n[Step 1/4] Parsing NiFi XML: {nifi_xml_path}")

    try:
        # Download and parse XML file
        xml_content = w.files.download(nifi_xml_path).contents.read().decode('utf-8')
        root = ET.fromstring(xml_content)

        # Extract processors
        processors = []
        for proc_elem in root.findall('.//processor'):
            proc_id = proc_elem.find('id')
            proc_name = proc_elem.find('name')
            proc_type = proc_elem.find('class')

            if proc_id is not None and proc_name is not None and proc_type is not None:
                proc_info = {
                    'id': proc_id.text,
                    'name': proc_name.text,
                    'type': proc_type.text,
                    'intent': categorize_processor(proc_type.text)
                }
                processors.append(proc_info)

        print(f"✅ Found {len(processors)} processors")

        # Show breakdown by intent
        intent_counts = {}
        for proc in processors:
            intent = proc['intent']
            intent_counts[intent] = intent_counts.get(intent, 0) + 1

        print(f"\nProcessor breakdown by intent:")
        for intent, count in sorted(intent_counts.items()):
            print(f"  - {intent}: {count} processors")

        add_iteration(step_iteration_num, 'parse_xml', 'COMPLETED', f'Parsed {len(processors)} processors from XML')

    except Exception as e:
        error_msg = f"Failed to parse XML: {str(e)}"
        print(f"❌ {error_msg}")
        add_iteration(step_iteration_num, 'parse_xml', 'FAILED', None, error_msg)
        dbutils.notebook.exit(json.dumps({
            "status": "FAILED",
            "iteration_num": iteration_num,
            "error": error_msg
        }))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Intent-Based Folder Structure (Generation Only)

# COMMAND ----------

step_iteration_num = 2

if iteration_num == 1:
    # Only create folders during generation
    print(f"\n[Step 2/4] Creating intent-based folder structure")

    try:
        # Get unique intents
        intents = list(set([p['intent'] for p in processors]))
        print(f"Creating folders for {len(intents)} intents: {intents}")

        # Create folders
        intent_paths = create_intent_folders(output_path, flow_name, intents)

        add_iteration(step_iteration_num, 'create_folders', 'COMPLETED', f'Created {len(intents)} intent folders')

    except Exception as e:
        error_msg = f"Failed to create folders: {str(e)}"
        print(f"❌ {error_msg}")
        add_iteration(step_iteration_num, 'create_folders', 'FAILED', None, error_msg)
        dbutils.notebook.exit(json.dumps({
            "status": "FAILED",
            "iteration_num": iteration_num,
            "error": error_msg
        }))
else:
    print(f"\n[Step 2/4] Skipping folder creation (error fixing mode)")
    # In error fixing mode, folders already exist
    # We would load the existing intent structure here
    intents = ['ingestion', 'transformation', 'routing', 'output', 'other']  # Placeholder
    intent_paths = {intent: f"{output_path}/{flow_name}/{intent}" for intent in intents}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Generate or Fix Notebooks

# COMMAND ----------

step_iteration_num = 3
generated_notebooks = []

if iteration_num == 1:
    # GENERATION MODE
    print(f"\n[Step 3/4] Generating test notebooks")

    try:
        for processor in processors:
            intent = processor['intent']
            proc_name = processor['name'].replace(' ', '_').replace('/', '_').replace('\\', '_')

            # Generate notebook content
            notebook_content = generate_test_notebook(
                flow_name=flow_name,
                processor_name=processor['name'],
                processor_type=processor['type'],
                intent=intent
            )

            # Save to intent folder
            notebook_path = f"{intent_paths[intent]}/{proc_name}.py"
            w.files.upload(notebook_path, notebook_content.encode('utf-8'), overwrite=True)
            generated_notebooks.append(notebook_path)
            print(f"  ✅ Generated: {notebook_path}")

            # Record as patch
            add_patch(iteration_num, notebook_path, f"Generated test notebook for {processor['name']} ({intent})")

        print(f"\n✅ Generated {len(generated_notebooks)} notebooks")
        add_iteration(step_iteration_num, 'generate_notebooks', 'COMPLETED', f'Generated {len(generated_notebooks)} test notebooks')

    except Exception as e:
        error_msg = f"Failed to generate notebooks: {str(e)}"
        print(f"❌ {error_msg}")
        add_iteration(step_iteration_num, 'generate_notebooks', 'FAILED', None, error_msg)
        dbutils.notebook.exit(json.dumps({
            "status": "FAILED",
            "iteration_num": iteration_num,
            "error": error_msg
        }))

else:
    # ERROR FIXING MODE
    print(f"\n[Step 3/4] Fixing notebooks based on error analysis")

    # Dummy agent: We already determined in Step 1 that we can't fix errors
    # Real agent would:
    # 1. Analyze which notebooks failed
    # 2. Use LLM to generate fixes
    # 3. Update the notebooks
    # 4. Record patches

    print("⚠️  Dummy agent cannot fix notebooks. Exiting with NEEDS_HUMAN.")
    add_iteration(step_iteration_num, 'fix_notebooks', 'COMPLETED', 'Dummy agent cannot fix - needs real implementation')

    dbutils.notebook.exit(json.dumps({
        "status": "NEEDS_HUMAN",
        "iteration_num": iteration_num,
        "reason": "dummy_agent_limitation",
        "error_details": "Dummy agent cannot fix errors. Real LLM-based agent implementation needed.",
        "next_action": "HUMAN_REVIEW"
    }))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Finalize and Return Structured Output

# COMMAND ----------

step_iteration_num = 4

if iteration_num == 1:
    # GENERATION MODE - Return success with notebooks to execute
    print(f"\n[Step 4/4] Finalizing generation")

    try:
        add_iteration(step_iteration_num, 'finalize', 'COMPLETED', 'Notebook generation completed successfully')

        print("=" * 60)
        print("✅ GENERATION COMPLETE")
        print("=" * 60)
        print(f"Run ID: {run_id}")
        print(f"Flow ID: {flow_id}")
        print(f"Iteration: {iteration_num}")
        print(f"Generated Notebooks: {len(generated_notebooks)}")
        print(f"Output Location: {output_path}/{flow_name}")
        print("=" * 60)
        print("\n⚠️ NOTE: This was a TEST conversion using dummy agent")
        print("Real conversions will generate functional PySpark code")
        print("=" * 60)

        # Return structured output for Flask orchestrator
        result = {
            "status": "SUCCESS",
            "iteration_num": iteration_num,
            "notebooks_generated": generated_notebooks,
            "next_action": "EXECUTE_NOTEBOOKS"
        }

        dbutils.notebook.exit(json.dumps(result))

    except Exception as e:
        error_msg = f"Failed to finalize: {str(e)}"
        print(f"❌ {error_msg}")
        add_iteration(step_iteration_num, 'finalize', 'FAILED', None, error_msg)
        dbutils.notebook.exit(json.dumps({
            "status": "FAILED",
            "iteration_num": iteration_num,
            "error": error_msg
        }))

else:
    # ERROR FIXING MODE - Already exited in Step 3 with NEEDS_HUMAN
    # This code should not be reached for dummy agent
    print(f"\n[Step 4/4] This should not be reached in error fixing mode")
    dbutils.notebook.exit(json.dumps({
        "status": "NEEDS_HUMAN",
        "iteration_num": iteration_num,
        "reason": "unexpected_code_path",
        "next_action": "HUMAN_REVIEW"
    }))
