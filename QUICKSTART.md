# Quick Start Guide

Get the NiFi-to-Databricks converter running in 5 minutes.

## Prerequisites

- Python 3.9+
- Databricks workspace access
- Unity Catalog enabled

## 1. Install Dependencies

```bash
pip install -r requirements.txt
```

## 2. Set Databricks Credentials

```bash
export DATABRICKS_HOST="https://your-workspace.databricks.com"
export DATABRICKS_TOKEN="dapi..."
```

## 3. Run the Application

```bash
flask --app app.py run --debug
```

## 4. Access the Application

Open your browser to: http://localhost:5000

## 5. Test the Workflow

### Step 1: Select a NiFi Flow
1. Enter catalog: `main`
2. Enter schema: `default`
3. Click "Load Volumes"
4. Select a volume and browse for XML files
5. Click "Convert to Notebooks"

### Step 2: Review Conversion
1. Watch the progress bar
2. Preview generated notebooks
3. Download notebooks if desired
4. Click "Proceed to Deploy"

### Step 3: Deploy & Run
1. Enter a job name
2. Review the execution plan
3. Click "Create & Run Now"

### Step 4: Monitor Execution
1. Watch real-time status updates
2. View individual task progress
3. Check task logs if needed
4. Wait for completion

## Troubleshooting

### "ModuleNotFoundError: No module named 'flask'"
```bash
pip install -r requirements.txt
```

### "Failed to initialize services"
Check your Databricks credentials:
```bash
echo $DATABRICKS_HOST
echo $DATABRICKS_TOKEN
```

### "Failed to list volumes"
Create test volumes in Unity Catalog:
```sql
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS main.default;
CREATE VOLUME IF NOT EXISTS main.default.nifi_input;
```

## Sample Data

### Create a Test NiFi XML

Save this as `sample_flow.xml` in `/Volumes/main/default/nifi_input/`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<flowController>
  <processGroup>
    <id>test-flow-1</id>
    <name>Sample Flow 1</name>
    <processor>
      <id>proc-1</id>
      <name>GetFile</name>
      <type>org.apache.nifi.processors.standard.GetFile</type>
    </processor>
  </processGroup>
</flowController>
```

## Next Steps

- Review [README.md](README.md) for full documentation
- Check [DEPLOYMENT.md](DEPLOYMENT.md) for production deployment
- Read [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) for technical details
- Review [DESIGN_PLAN.md](DESIGN_PLAN.md) for architecture

## Common Commands

```bash
# Run with debug mode
flask --app app.py run --debug

# Run on specific port
flask --app app.py run --port 8080

# Run accessible from network
flask --app app.py run --host 0.0.0.0

# Check Python syntax
python3 -m py_compile app.py

# Verify all Python files
python3 -m py_compile models/*.py services/*.py utils/*.py
```

## Development Workflow

1. Make code changes
2. Flask auto-reloads in debug mode
3. Test in browser
4. Check terminal for errors
5. Iterate

## Getting Help

- Check the error message in terminal
- Review browser console (F12)
- Check [DEPLOYMENT.md](DEPLOYMENT.md) troubleshooting section
- Contact the team (see README for contacts)

## Quick Reference

### Project Structure
```
app.py              # Main Flask app
models/             # Data models
services/           # Business logic
utils/              # Utilities
templates/          # HTML templates
static/             # CSS/JS assets
```

### Key Files
- `app.py` - Flask routes and API endpoints
- `services/agent_service.py` - Conversion logic (needs integration)
- `services/job_deployment.py` - Databricks job management
- `templates/step*.html` - UI for each step

### Important URLs
- `/` - Landing page
- `/step1` - File selection
- `/step2/<job_id>` - Review conversion
- `/step3/<job_id>` - Deploy configuration
- `/step3b/<run_id>` - Monitor execution
- `/api/*` - REST API endpoints

## Ready to Go!

You should now have a working NiFi-to-Databricks converter. Start by selecting a NiFi XML file and watching it convert to notebooks!

For production deployment, see [DEPLOYMENT.md](DEPLOYMENT.md).
