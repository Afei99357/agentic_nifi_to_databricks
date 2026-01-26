# NiFi Conversion Notebooks

## Dummy Agent Notebook for Testing

The `nifi_conversion_dummy_agent.py` file is a test notebook that simulates the NiFi-to-Databricks conversion process. It's used for testing dynamic job creation and history tracking before the real MLflow agent is ready.

### Upload to Databricks Workspace

**IMPORTANT:** This notebook must be uploaded to your Databricks workspace before the app can create conversion jobs.

**Upload Path:** `/Workspace/Shared/nifi_conversion_dummy_agent`

#### Option 1: Using Databricks UI

1. Log in to your Databricks workspace
2. Navigate to **Workspace** in the left sidebar
3. Click on **Shared** folder
4. Click **Create** → **Import**
5. Select `notebooks/nifi_conversion_dummy_agent.py` from your local machine
6. Verify the path shows: `/Workspace/Shared/nifi_conversion_dummy_agent`

#### Option 2: Using Databricks CLI

```bash
# Configure Databricks CLI (if not already done)
databricks configure --token

# Upload the notebook
databricks workspace import \
  notebooks/nifi_conversion_dummy_agent.py \
  /Workspace/Shared/nifi_conversion_dummy_agent \
  --language PYTHON \
  --format SOURCE \
  --overwrite
```

#### Option 3: Using VS Code Databricks Extension

1. Install the **Databricks** extension in VS Code
2. Connect to your workspace
3. Right-click on `notebooks/nifi_conversion_dummy_agent.py`
4. Select **Upload to Databricks**
5. Choose destination: `/Workspace/Shared/nifi_conversion_dummy_agent`

### Verification

After uploading, verify the notebook exists:

```bash
# Using Databricks CLI
databricks workspace ls /Workspace/Shared/

# Should show:
# nifi_conversion_dummy_agent
```

Or visit in Databricks UI:
- Navigate to **Workspace** → **Shared**
- You should see `nifi_conversion_dummy_agent` listed

### What the Dummy Agent Does

When a conversion job runs, this notebook will:

1. **Validate XML File** - Checks that the NiFi XML file exists in the Unity Catalog volume
2. **Simulate Parsing** - Pretends to parse the NiFi flow definition (5 seconds)
3. **Simulate Conversion** - Runs 3 iterations of "converting" processors to PySpark (24 seconds)
4. **Generate Dummy Notebooks** - Creates 3 placeholder notebooks:
   - `{flow_id}_ingestion.py`
   - `{flow_id}_transformation.py`
   - `{flow_id}_output.py`
5. **Update History Table** - Tracks progress and marks conversion as SUCCESS

**Total simulation time:** ~35 seconds

### Expected Parameters

The notebook expects these parameters (automatically passed by the app):

- `flow_id` - Flow identifier
- `nifi_xml_path` - Path to NiFi XML file (e.g., `/Volumes/main/default/nifi_files/prod/flow1.xml`)
- `output_path` - Where to save notebooks (e.g., `/Volumes/main/default/nifi_notebooks/flow1`)
- `attempt_id` - Unique attempt ID for history tracking
- `delta_table` - History table name (default: `main.default.nifi_conversion_history`)

### Transition to Real Agent

This dummy notebook is a **temporary placeholder**. When the real MLflow agent application is ready:

1. The agent team will deploy the MLflow app to Databricks
2. Update `services/dynamic_job_service.py` to change from `notebook_task` to the appropriate MLflow task type
3. Remove or deprecate this dummy notebook
4. Update `app.yaml` to remove `AGENT_NOTEBOOK_PATH` (no longer needed)

The app is designed to make this transition seamless - only the job configuration needs to change, not the overall flow.
