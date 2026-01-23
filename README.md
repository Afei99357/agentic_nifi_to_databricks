# NiFi-to-Databricks Conversion Flask App

A Flask web application that converts Apache NiFi flows to Databricks notebooks and executes them in parallel using AI-powered automation.

## Features

- **Step 1: Select NiFi Flow** - Browse Unity Catalog volumes and select NiFi XML files
- **Step 2: Review Conversion** - AI agent converts flows to notebooks with real-time progress
- **Step 3: Deploy & Run** - Create Databricks jobs with parallel task execution
- **Step 4: Monitor** - Real-time job monitoring with task status and logs

## Architecture

- **One XML file** → **Multiple independent flows**
- **Each independent flow** → **One notebook** (1:1 mapping)
- **Parallel execution** via single Databricks job with multiple tasks
- **Real-time monitoring** of job execution status

## Project Structure

```
agentic_nifi_accelerator_app/
├── app.py                      # Main Flask application
├── app.yaml                    # Databricks app configuration
├── requirements.txt            # Python dependencies
├── static/
│   ├── css/styles.css         # Application styles
│   └── js/app.js              # JavaScript utilities
├── templates/
│   ├── base.html              # Base template
│   ├── index.html             # Landing page
│   ├── step1_select.html      # File selection
│   ├── step2_review.html      # Review conversion
│   ├── step3_deploy.html      # Deployment config
│   ├── step3b_monitor.html    # Execution monitoring
│   └── components/
│       └── stepper.html       # Step indicator component
├── services/
│   ├── unity_catalog.py       # UC volume operations
│   ├── agent_service.py       # Agent invocation & monitoring
│   ├── notebook_service.py    # Notebook management
│   ├── nifi_parser.py         # XML parsing & validation
│   └── job_deployment.py      # Databricks job management
├── models/
│   ├── conversion_job.py      # Job tracking model
│   └── nifi_flow.py           # NiFi flow representation
└── utils/
    └── databricks_client.py   # Databricks SDK wrapper
```

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure Databricks authentication (one of):
   - Set environment variables: `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
   - Use Databricks CLI configuration
   - Use Azure CLI authentication

3. Run locally:
```bash
flask --app app.py run
```

4. Or deploy to Databricks:
```bash
# Upload files to Databricks workspace
# Create Databricks app using app.yaml
```

## Usage

1. **Select NiFi Flow**
   - Navigate to Step 1
   - Enter catalog and schema name
   - Load volumes and select XML file
   - Preview file to validate

2. **Review Conversion**
   - Monitor AI agent conversion progress
   - Preview generated notebooks
   - Download individual or all notebooks

3. **Deploy & Run**
   - Configure job name
   - Select cluster settings
   - Create job and run immediately or later

4. **Monitor Execution**
   - Watch real-time task status
   - View task logs
   - Track overall progress
   - Cancel running jobs

## API Endpoints

### File Operations
- `GET /api/volumes` - List Unity Catalog volumes
- `GET /api/files` - List files in volume path
- `POST /api/file/preview` - Preview XML file

### Conversion
- `POST /api/convert` - Start conversion
- `GET /api/job/<job_id>/status` - Get conversion status
- `GET /api/job/<job_id>/notebooks` - Get generated notebooks

### Downloads
- `GET /api/job/<job_id>/download/<notebook_name>` - Download notebook
- `GET /api/job/<job_id>/download-all` - Download all as ZIP

### Deployment & Monitoring
- `POST /api/job/<job_id>/deploy-and-run` - Deploy and run job
- `GET /api/run/<run_id>/status` - Get job run status
- `GET /api/run/<run_id>/tasks` - Get task statuses
- `GET /api/run/<run_id>/task/<task_key>/logs` - Get task logs
- `POST /api/run/<run_id>/cancel` - Cancel job run

## Agent Integration

The agent service (`services/agent_service.py`) is currently a placeholder implementation. To integrate with the actual agentic conversion system:

1. Update `start_conversion()` to invoke your agent API
2. Implement `get_conversion_status()` to poll agent progress
3. Implement `get_conversion_results()` to retrieve generated notebooks
4. Update the agent integration documentation with actual API details

## Configuration

### Environment Variables
- `DATABRICKS_HOST` - Databricks workspace URL
- `DATABRICKS_TOKEN` - Personal access token
- `SECRET_KEY` - Flask secret key (default: 'dev-secret-key')

### Unity Catalog Paths
Notebooks are saved to: `/Volumes/catalog/schema/nifi_notebooks/{source_filename}/`

## Development

### Running Tests
```bash
# TODO: Add test suite
pytest
```

### Code Style
```bash
# Format code
black .

# Lint code
flake8 .
```

## Troubleshooting

### Common Issues

1. **"Failed to initialize services"**
   - Check Databricks authentication configuration
   - Verify network connectivity to Databricks workspace

2. **"Failed to list volumes"**
   - Verify catalog and schema names
   - Check Unity Catalog permissions

3. **"Conversion failed"**
   - Check agent service logs
   - Verify NiFi XML is valid

4. **"Failed to create job"**
   - Verify cluster configuration
   - Check Databricks Jobs API permissions

## Next Steps

1. Integrate with actual agentic conversion system
2. Add user authentication
3. Implement job history and persistence
4. Add error recovery and retry logic
5. Enhance monitoring with metrics and alerts
6. Add support for existing cluster selection
7. Implement batch conversion for multiple files

## Team

- **Eric** - Project Lead
- **Cori** - Product Management
- **Alex & Alexandru** - Agent Integration
- **Bill & John** - Architecture

## License

Copyright 2026 Databricks. See LICENSE file for details.
