# Deployment Guide

## Prerequisites

1. Databricks workspace with Unity Catalog enabled
2. Python 3.9+ environment
3. Databricks CLI configured (optional)

## Local Development

### 1. Set up Python environment

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Databricks Authentication

Set environment variables:

```bash
export DATABRICKS_HOST="https://your-workspace.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"
```

Or use Databricks CLI:

```bash
databricks configure --token
```

### 3. Run the Application

```bash
flask --app app.py run --debug
```

Access at: http://localhost:5000

## Databricks Deployment

### Option 1: Databricks Apps (Recommended)

1. Upload project files to Databricks workspace:

```bash
# Using Databricks CLI
databricks workspace mkdirs /Users/your-email@company.com/nifi-converter-app
databricks workspace import-dir . /Users/your-email@company.com/nifi-converter-app
```

2. Create a Databricks app:
   - Go to Databricks workspace
   - Navigate to Apps
   - Click "Create App"
   - Select the uploaded directory
   - The `app.yaml` will be used automatically

3. Start the app and access the provided URL

### Option 2: Databricks Notebook

1. Upload the project as a repo:
   - Go to Repos in Databricks
   - Add your git repository
   - Or upload files directly

2. Create a notebook cell:

```python
%pip install -r requirements.txt

import sys
sys.path.append('/Workspace/path/to/app')

from app import flask_app
flask_app.run(host='0.0.0.0', port=8080)
```

### Option 3: Databricks Job

1. Package the application:

```bash
zip -r nifi-converter-app.zip . -x "*.git*" -x "*__pycache__*" -x "*.pyc"
```

2. Upload to DBFS:

```bash
databricks fs cp nifi-converter-app.zip dbfs:/apps/nifi-converter-app.zip
```

3. Create a job:
   - Use Python Wheel task type
   - Point to the uploaded ZIP
   - Set entry point to `app:flask_app`

## Unity Catalog Setup

### 1. Create Catalog and Schema

```sql
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS main.default;
```

### 2. Create Volume for NiFi Files

```sql
CREATE VOLUME IF NOT EXISTS main.default.nifi_input;
CREATE VOLUME IF NOT EXISTS main.default.nifi_notebooks;
```

### 3. Upload Sample NiFi XML

Upload your NiFi XML files to:
`/Volumes/main/default/nifi_input/`

## Configuration

### Environment Variables

Create a `.env` file (for local development):

```bash
DATABRICKS_HOST=https://your-workspace.databricks.com
DATABRICKS_TOKEN=dapi...
SECRET_KEY=your-secure-secret-key
```

### app.yaml Configuration

The `app.yaml` file configures the Flask app for Databricks Apps:

```yaml
command: [
  "flask",
  "--app",
  "app.py",
  "run",
  "--host",
  "0.0.0.0",
  "--port",
  "8080"
]
```

## Permissions

### Required Databricks Permissions

1. **Unity Catalog:**
   - Read access to input volumes
   - Write access to output volumes
   - USE CATALOG and USE SCHEMA permissions

2. **Jobs API:**
   - Create and manage jobs
   - View job runs
   - Cancel job runs

3. **Workspace:**
   - Read/write access to workspace folder
   - Execute notebooks

### Granting Permissions

```sql
-- Grant catalog permissions
GRANT USE CATALOG ON CATALOG main TO `user@company.com`;
GRANT USE SCHEMA ON SCHEMA main.default TO `user@company.com`;

-- Grant volume permissions
GRANT READ VOLUME ON VOLUME main.default.nifi_input TO `user@company.com`;
GRANT WRITE VOLUME ON VOLUME main.default.nifi_notebooks TO `user@company.com`;
```

## Health Checks

### Verify Installation

```bash
# Test Flask app
curl http://localhost:5000/

# Test API endpoints
curl http://localhost:5000/api/volumes?catalog=main&schema=default
```

### Common Issues

1. **ModuleNotFoundError: No module named 'flask'**
   - Install dependencies: `pip install -r requirements.txt`

2. **databricks.sdk.errors.platform.PermissionDenied**
   - Verify Unity Catalog permissions
   - Check Databricks token is valid

3. **Connection refused**
   - Verify DATABRICKS_HOST environment variable
   - Check network connectivity

## Monitoring

### Application Logs

```bash
# Local development
tail -f flask.log

# Databricks Apps
# View logs in the Apps UI console
```

### Performance Metrics

Monitor:
- API response times
- Conversion job duration
- Databricks job execution time
- Error rates

## Scaling

### Horizontal Scaling

For high traffic, deploy multiple app instances:

1. Use Databricks Apps with auto-scaling
2. Implement session affinity if needed
3. Consider external load balancer

### Database Scaling

Currently using in-memory job storage. For production:

1. Replace with Delta table storage
2. Implement connection pooling
3. Add caching layer (Redis)

## Security

### Best Practices

1. **Never commit secrets:**
   - Use environment variables
   - Use Databricks secrets
   - Rotate tokens regularly

2. **Input validation:**
   - Validate XML files
   - Sanitize file paths
   - Check file sizes

3. **Access control:**
   - Implement user authentication
   - Use Databricks SSO
   - Audit access logs

### Secrets Management

```python
# Use Databricks secrets
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
token = w.dbutils.secrets.get(scope="app-secrets", key="api-token")
```

## Backup and Recovery

### Backup Strategy

1. **Code:**
   - Version control (Git)
   - Regular commits

2. **Data:**
   - Unity Catalog volumes are persistent
   - Delta tables have time travel

3. **Configuration:**
   - Document environment variables
   - Store app.yaml in version control

### Disaster Recovery

1. Redeploy from Git repository
2. Restore Unity Catalog volumes from backup
3. Recreate Databricks app configuration

## Maintenance

### Regular Tasks

1. **Weekly:**
   - Review error logs
   - Check disk usage
   - Monitor job failures

2. **Monthly:**
   - Update dependencies
   - Review security patches
   - Archive old job data

3. **Quarterly:**
   - Performance optimization
   - Capacity planning
   - Security audit

## Support

For issues or questions:

1. Check the README.md
2. Review DESIGN_PLAN.md
3. Contact the team (see README for contacts)
4. Open an issue in the project repository

## Next Steps After Deployment

1. Test the complete workflow with sample data
2. Integrate with actual agent service
3. Set up monitoring and alerts
4. Train users on the interface
5. Gather feedback and iterate
