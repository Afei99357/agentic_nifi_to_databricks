# NiFi-to-Databricks Conversion Flask App - Design Plan

## Meeting Summary

### Key Participants
- **Eric Liao** (You) - Working on UI/first draft visualization
- **Cori Hendon** - Technical lead, infrastructure concerns
- **Alexandru Muresan** - Model endpoint testing and code generation
- **Alex Sisu** - Development team
- **Bill Tucker & John Urban** - Team members

### Key Technical Points from Meeting
1. **Model Endpoint Infrastructure**
   - Testing provision throughput endpoints for Claude models
   - Concern about API rate limits beyond provision throughput
   - Context window/token limit testing needed
   - Goal: Generate Databricks notebooks using AI models

2. **Code Generation Capability**
   - Testing if models can generate executable Databricks notebooks
   - Focus on NiFi-specific code generation (not just generic notebooks)
   - Need to measure if models "know enough about Databricks"

3. **Team Agreement**
   - Eric to create first visual draft for team review
   - Parallel workstreams established
   - Need something visual to clarify requirements

---

## Application Purpose

**Primary Goal**: Migrate and convert existing Apache NiFi flows to Databricks notebooks and jobs

**User Workflow**: Users have NiFi XML flow files → Agent processes them → Generates Databricks notebooks → Users review and deploy

### Key Architecture (Confirmed with Alex)
- **One XML file** can contain **multiple independent flows**
- **Each independent flow** generates **exactly one notebook**
- All notebooks are **independent** (no dependencies between them)
- Notebooks can run **in parallel** (order doesn't matter)
- Generated notebooks are saved to **Unity Catalog volume**
- System should **automatically execute notebooks in parallel** via Databricks job

---

## Flask Application Architecture (Databricks-Hosted)

### Project Structure
```
agentic_nifi_accelerator_app/
├── app.py                      # Main Flask application
├── app.yaml                    # Databricks app configuration
├── requirements.txt            # Python dependencies
├── static/                     # CSS, JS, images
│   ├── css/
│   │   └── styles.css
│   └── js/
│       └── app.js
├── templates/                  # Jinja2 HTML templates
│   ├── base.html              # Base template with common layout
│   ├── index.html             # Landing/dashboard page
│   ├── step1_select.html      # Step 1: Select NiFi flow
│   ├── step2_review.html      # Step 2: Review conversion
│   └── components/            # Reusable UI components
│       ├── header.html
│       ├── stepper.html       # Multi-step wizard progress bar
│       └── file_browser.html  # Unity Catalog volume browser
├── services/                   # Business logic layer
│   ├── __init__.py
│   ├── unity_catalog.py       # UC volume interactions
│   ├── agent_service.py       # Agent invocation & monitoring
│   ├── notebook_service.py    # Notebook generation & management
│   ├── nifi_parser.py         # NiFi XML parsing utilities
│   └── job_deployment.py      # Databricks job creation & execution
├── models/                     # Data models
│   ├── __init__.py
│   ├── conversion_job.py      # Job tracking model
│   └── nifi_flow.py           # NiFi flow representation
└── utils/                      # Helper utilities
    ├── __init__.py
    └── databricks_client.py   # Databricks API wrapper
```

---

## UI/UX Design - Multi-Step Wizard

### Design Principles
- **Clean, professional interface** suitable for data engineers
- **Progress visibility** - Always show current step and overall progress
- **Error handling** - Clear feedback when things go wrong
- **Responsive design** - Works on desktop (primary) and tablets

### Step 1: Select NiFi Flow
**Purpose**: Browse and select NiFi XML files from Unity Catalog volumes

**UI Components**:
```
┌─────────────────────────────────────────────────────────┐
│  NiFi to Databricks Converter         [User: Eric]  [⚙] │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ●──────○──────○                                        │
│  Select  Review Deploy                                   │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │ Step 1: Select NiFi Flow                       │    │
│  └────────────────────────────────────────────────┘    │
│                                                          │
│  Unity Catalog Volume Path:                             │
│  ┌─────────────────────────────────────────────┐       │
│  │ /Volumes/catalog/schema/nifi_flows/     [📁] │       │
│  └─────────────────────────────────────────────┘       │
│                                                          │
│  Available NiFi Flow Files:                             │
│  ┌─────────────────────────────────────────────────┐   │
│  │ ○ customer_data_pipeline.xml      45 KB  ↻ 2d  │   │
│  │ ○ log_processing_flow.xml         32 KB  ↻ 1w  │   │
│  │ ○ kafka_to_delta_flow.xml        128 KB  ↻ 3d  │   │
│  │ ○ api_ingestion_flow.xml          67 KB  ↻ 5d  │   │
│  └─────────────────────────────────────────────────┘   │
│                                                          │
│  Selected: customer_data_pipeline.xml                   │
│                                                          │
│  [Preview XML] [Cancel] [Next: Start Conversion →]      │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

**Features**:
- Unity Catalog volume path selector with autocomplete
- File list with metadata (size, last modified)
- Radio button selection (single file at a time initially)
- XML preview modal (optional, click "Preview XML")
- Validation: Must select a file to proceed

---

### Step 2: Review Conversion Results
**Purpose**: Monitor agent conversion progress and review generated notebooks

**UI Components - Processing State**:
```
┌─────────────────────────────────────────────────────────┐
│  NiFi to Databricks Converter         [User: Eric]  [⚙] │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ●──────●──────○                                        │
│  Select  Review Deploy                                   │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │ Step 2: Review Conversion                      │    │
│  └────────────────────────────────────────────────┘    │
│                                                          │
│  Converting: customer_data_pipeline.xml                 │
│                                                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │  🔄 Agent Processing...                          │   │
│  │                                                  │   │
│  │  [████████████░░░░░░░░░░] 60%                   │   │
│  │                                                  │   │
│  │  Status: Analyzing NiFi processors               │   │
│  │  Elapsed: 0:02:34                                │   │
│  │                                                  │   │
│  │  ✓ Parsed NiFi XML structure                     │   │
│  │  ✓ Identified 12 processors                      │   │
│  │  ✓ Mapped 8 connections                          │   │
│  │  ⏳ Generating notebook for data ingestion...    │   │
│  │  ⏳ Pending: transformation logic                │   │
│  │  ⏳ Pending: output stage                        │   │
│  └─────────────────────────────────────────────────┘   │
│                                                          │
│  [View Logs] [Cancel Conversion]                        │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

**UI Components - Completion State**:
```
┌─────────────────────────────────────────────────────────┐
│  NiFi to Databricks Converter         [User: Eric]  [⚙] │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ●──────●──────○                                        │
│  Select  Review Deploy                                   │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │ Step 2: Review Conversion                      │    │
│  └────────────────────────────────────────────────┘    │
│                                                          │
│  ✓ Conversion Complete!                                 │
│                                                          │
│  Source: customer_data_pipeline.xml                     │
│  Generated: 3 notebooks | Duration: 3m 45s              │
│                                                          │
│  Generated Notebooks:                                   │
│  ℹ️ Each notebook represents one independent flow      │
│  💡 All notebooks will run in parallel                 │
│                                                          │
│  ┌─────────────────────────────────────────────────┐   │
│  │ 📓 customer_ingestion_flow.py                    │   │
│  │    Independent Flow #1                           │   │
│  │    - Processes customer data pipeline            │   │
│  │    - Kafka → Transformation → Delta              │   │
│  │    [Preview Code] [Download]                     │   │
│  ├─────────────────────────────────────────────────┤   │
│  │ 📓 log_processing_flow.py                        │   │
│  │    Independent Flow #2                           │   │
│  │    - Processes application logs                  │   │
│  │    - S3 → Parsing → Analytics tables             │   │
│  │    [Preview Code] [Download]                     │   │
│  ├─────────────────────────────────────────────────┤   │
│  │ 📓 api_integration_flow.py                       │   │
│  │    Independent Flow #3                           │   │
│  │    - REST API data collection                    │   │
│  │    - API → Enrichment → Data warehouse           │   │
│  │    [Preview Code] [Download]                     │   │
│  └─────────────────────────────────────────────────┘   │
│                                                          │
│  ⚠ Manual Review Needed:                                │
│  • Custom NiFi processors may require manual migration  │
│  • Review API credentials and secrets configuration     │
│                                                          │
│  [← Back] [Download All] [Next: Deploy (TBD) →]         │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

**Features**:
- Real-time progress indicator with percentage
- Status messages showing current agent activity
- Checklist of completed/pending steps
- Notebook cards with:
  - File name and icon
  - Brief description of what the notebook does
  - Preview and download buttons
- Warning/info section for manual steps
- Code preview modal (syntax-highlighted Python/SQL)

---

### Step 3: Deploy & Run
**Purpose**: Create Databricks job with parallel tasks and execute all notebooks

**Architecture Decision (Based on Alex's Input)**:
- Create **one Databricks job** with **multiple parallel tasks**
- Each task runs one notebook (one independent flow)
- All tasks run **concurrently** (no task dependencies)
- Job automatically triggers on conversion completion (optional)

**UI Components**:
```
┌─────────────────────────────────────────────────────────┐
│  NiFi to Databricks Converter         [User: Eric]  [⚙] │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ●──────●──────●                                        │
│  Select  Review Deploy                                   │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │ Step 3: Deploy & Run Notebooks                 │    │
│  └────────────────────────────────────────────────┘    │
│                                                          │
│  Job Configuration:                                     │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Job Name: customer_data_pipeline_conversion     │   │
│  │                                                  │   │
│  │ Execution Mode: ● Parallel (All at once)        │   │
│  │                                                  │   │
│  │ Cluster: [Serverless Compute ▼]                 │   │
│  │                                                  │   │
│  │ Save Location:                                   │   │
│  │ /Volumes/catalog/schema/nifi_notebooks/         │   │
│  │ customer_data_pipeline/                          │   │
│  └─────────────────────────────────────────────────┘   │
│                                                          │
│  Databricks Job Structure:                              │
│  ┌─────────────────────────────────────────────────┐   │
│  │ Job: customer_data_pipeline_conversion          │   │
│  │                                                  │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐      │   │
│  │  │ Task 1   │  │ Task 2   │  │ Task 3   │      │   │
│  │  │ customer │  │ log      │  │ api      │      │   │
│  │  │ flow     │  │ flow     │  │ flow     │      │   │
│  │  └──────────┘  └──────────┘  └──────────┘      │   │
│  │      ↓              ↓              ↓            │   │
│  │  (Run in Parallel - No Dependencies)            │   │
│  └─────────────────────────────────────────────────┘   │
│                                                          │
│  [ ] Auto-run after deployment                          │
│  [ ] Set up scheduling (optional)                       │
│                                                          │
│  [← Back] [Create Job & Deploy] [Create & Run Now]     │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

**Features**:
- Automatically generate job configuration
- All tasks configured to run in parallel
- Save notebooks to structured UC volume path
- Option to run immediately or just create job
- Optional scheduling configuration

---

### Step 3b: Monitor Job Execution (Real-time)
**Purpose**: Monitor parallel notebook execution status in real-time

**UI Components - Execution Monitoring**:
```
┌─────────────────────────────────────────────────────────┐
│  NiFi to Databricks Converter         [User: Eric]  [⚙] │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ●──────●──────●                                        │
│  Select  Review Deploy                                   │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │ Monitoring Job Execution                       │    │
│  └────────────────────────────────────────────────┘    │
│                                                          │
│  Job: customer_data_pipeline_conversion                 │
│  Run ID: 12345 | Started: 2m 15s ago                    │
│                                                          │
│  Overall Status: 🔄 RUNNING (2/3 completed)             │
│  [█████████████████░░░░░] 67%                           │
│                                                          │
│  Task Execution Status:                                 │
│  ┌─────────────────────────────────────────────────┐   │
│  │ ✅ Task 1: customer_ingestion_flow.py            │   │
│  │    Status: SUCCESS | Duration: 1m 45s            │   │
│  │    [View Logs] [View Output]                     │   │
│  ├─────────────────────────────────────────────────┤   │
│  │ ✅ Task 2: log_processing_flow.py                │   │
│  │    Status: SUCCESS | Duration: 2m 10s            │   │
│  │    [View Logs] [View Output]                     │   │
│  ├─────────────────────────────────────────────────┤   │
│  │ 🔄 Task 3: api_integration_flow.py               │   │
│  │    Status: RUNNING | Elapsed: 0m 30s             │   │
│  │    [View Logs] [Cancel Task]                     │   │
│  └─────────────────────────────────────────────────┘   │
│                                                          │
│  Auto-refresh: ● On (every 5s) [Pause]                  │
│                                                          │
│  [← Back to Review] [View Job in Databricks]            │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

**Task Status Icons & Colors**:
- ⏳ **PENDING** - Task queued, not started (Gray)
- 🔄 **RUNNING** - Currently executing (Blue)
- ✅ **SUCCESS** - Completed successfully (Green)
- ❌ **FAILED** - Execution failed with error (Red)
- ⚠️ **TIMEOUT** - Task exceeded time limit (Orange)
- 🚫 **CANCELLED** - Manually cancelled by user (Gray)

**Features**:
- Real-time status updates via polling (every 5 seconds)
- Progress bar showing overall completion
- Individual task status with duration/elapsed time
- Links to view Databricks logs and outputs
- Option to cancel running tasks
- Notification when all tasks complete
- Direct link to Databricks job UI

---

## Backend Services & Integration Points

### 1. Unity Catalog Service (`services/unity_catalog.py`)
**Responsibilities**:
- List volumes and files in Unity Catalog
- Read NiFi XML files from volumes
- Validate file access permissions
- Cache volume listings for performance

**Key Methods**:
```python
list_volumes(catalog: str, schema: str) -> List[Volume]
list_files(volume_path: str) -> List[FileMetadata]
read_nifi_xml(file_path: str) -> str
validate_xml_structure(xml_content: str) -> ValidationResult
```

**Databricks Integration**:
- Use Databricks SDK or REST API
- Authenticate using app service principal or user token
- Access pattern: `/Volumes/{catalog}/{schema}/{volume}/{path}`

---

### 2. Agent Service (`services/agent_service.py`)
**Responsibilities**:
- Invoke the agentic conversion system (developed by coworkers)
- Monitor conversion job progress
- Handle agent errors and retries
- Store conversion job metadata

**Key Methods**:
```python
start_conversion(nifi_xml: str, job_id: str) -> ConversionJob
get_conversion_status(job_id: str) -> JobStatus
get_conversion_results(job_id: str) -> List[Notebook]
cancel_conversion(job_id: str) -> bool
```

**Agent Behavior (Confirmed with Alex)**:
- Agent parses XML and identifies multiple independent flows
- Each independent flow → one notebook
- All flows are independent (no execution order)
- Notebooks saved to Unity Catalog volume

**Integration Points** (To Be Determined):
- How is the agent invoked? (REST API, Databricks notebook execution, MLflow model?)
- Where does the agent run? (Serverless compute, existing cluster?)
- How are results returned? (Unity Catalog volume, Delta table, API response?)
- What progress updates are available?
- How does agent identify and separate independent flows?

**Proposed Flow**:
```
Flask App → Agent Endpoint/Notebook
            ↓
         Agent parses NiFi XML
            ↓
         Identifies N independent flows
            ↓
         Generates N notebooks (1 per flow)
            ↓
         Saves to UC Volume: /Volumes/.../flow_name/*.py
            ↓
         Updates job status table
            ↓
Flask App ← Polls for completion
            ↓
         Retrieves N notebooks
            ↓
         Creates Databricks job with N parallel tasks
```

---

### 3. Notebook Service (`services/notebook_service.py`)
**Responsibilities**:
- Retrieve generated notebooks from agent output
- Parse notebook metadata (title, description)
- Generate download bundles (zip archives)
- Format notebooks for preview (syntax highlighting)
- Save notebooks to Unity Catalog volume

**Key Methods**:
```python
get_generated_notebooks(job_id: str) -> List[Notebook]
save_notebooks_to_volume(notebooks: List[Notebook], volume_path: str) -> bool
get_notebook_preview(notebook_path: str) -> NotebookPreview
download_notebook(notebook_path: str) -> bytes
download_all_notebooks(job_id: str) -> bytes  # ZIP archive
```

**Storage Pattern**:
```
/Volumes/catalog/schema/nifi_notebooks/
  └── {source_xml_filename}/
      ├── flow_1.py          # Independent flow #1
      ├── flow_2.py          # Independent flow #2
      └── flow_3.py          # Independent flow #3
```

---

### 4. NiFi Parser (`services/nifi_parser.py`)
**Responsibilities**:
- Parse NiFi XML structure for preview
- Extract processor names and types
- Identify flow connections and dependencies
- Validate NiFi XML schema

**Key Methods**:
```python
parse_flow(xml_content: str) -> NiFiFlow
extract_processors(flow: NiFiFlow) -> List[Processor]
get_flow_statistics(flow: NiFiFlow) -> FlowStats
```

---

### 5. Job Deployment Service (`services/job_deployment.py`)
**Responsibilities**:
- Create Databricks jobs with parallel task configuration
- Deploy notebooks to workspace or reference from UC volume
- Configure compute resources (serverless or cluster)
- Trigger job runs and monitor execution

**Key Methods**:
```python
create_parallel_job(job_name: str, notebooks: List[Notebook], cluster_config: dict) -> JobId
add_parallel_tasks(job_id: str, notebooks: List[Notebook]) -> bool
run_job(job_id: str) -> RunId
get_job_run_status(run_id: str) -> JobRunStatus
get_task_statuses(run_id: str) -> List[TaskStatus]
cancel_job_run(run_id: str) -> bool
get_task_logs(run_id: str, task_key: str) -> str
get_task_output(run_id: str, task_key: str) -> dict
```

**JobRunStatus Model**:
```python
class JobRunStatus:
    run_id: str
    state: str  # 'PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED'
    start_time: datetime
    end_time: datetime | None
    duration_seconds: int
    tasks: List[TaskStatus]

class TaskStatus:
    task_key: str
    notebook_path: str
    state: str  # 'PENDING', 'RUNNING', 'SUCCESS', 'FAILED', 'TIMEOUT', 'CANCELLED'
    start_time: datetime | None
    end_time: datetime | None
    duration_seconds: int | None
    error_message: str | None
```

**Databricks Job Configuration**:
```json
{
  "name": "nifi_conversion_{source_filename}",
  "tasks": [
    {
      "task_key": "flow_1",
      "notebook_task": {
        "notebook_path": "/Volumes/catalog/schema/nifi_notebooks/flow_1.py"
      },
      "new_cluster": { "spark_version": "...", "node_type_id": "..." }
    },
    {
      "task_key": "flow_2",
      "notebook_task": {
        "notebook_path": "/Volumes/catalog/schema/nifi_notebooks/flow_2.py"
      },
      "new_cluster": { "spark_version": "...", "node_type_id": "..." }
    }
  ],
  "max_concurrent_runs": 1
}
```

**Note**: No `depends_on` fields between tasks = parallel execution

---

## Data Models

### ConversionJob (`models/conversion_job.py`)
```python
class ConversionJob:
    job_id: str
    user_id: str
    source_file: str
    source_volume_path: str
    status: str  # 'pending', 'processing', 'completed', 'failed'
    progress_percentage: int
    status_message: str
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    generated_notebooks: List[str]
    error_message: str | None
    warnings: List[str]
```

**Storage**:
- Option 1: In-memory (dict) - Simple, lost on restart
- Option 2: Delta table in Unity Catalog - Persistent, queryable
- Option 3: SQLite database - Middle ground

---

### NiFiFlow (`models/nifi_flow.py`)
```python
class NiFiFlow:
    flow_id: str
    name: str
    processors: List[Processor]
    connections: List[Connection]
    process_groups: List[ProcessGroup]

class Processor:
    id: str
    name: str
    type: str
    config: dict

class Connection:
    source_id: str
    destination_id: str
    relationships: List[str]
```

---

## Flask Application Structure (`app.py`)

### Core Routes
```python
# Main pages
@flask_app.route('/')
def index()
    """Dashboard/landing page"""

@flask_app.route('/step1')
def step1_select()
    """Step 1: Select NiFi flow from UC volume"""

@flask_app.route('/step2/<job_id>')
def step2_review(job_id)
    """Step 2: Review conversion results"""

# API endpoints for AJAX calls
@flask_app.route('/api/volumes')
def api_list_volumes()
    """Return JSON list of available volumes"""

@flask_app.route('/api/files')
def api_list_files()
    """Return JSON list of files in volume"""

@flask_app.route('/api/convert', methods=['POST'])
def api_start_conversion()
    """Start conversion job, return job_id"""

@flask_app.route('/api/job/<job_id>/status')
def api_job_status(job_id)
    """Return current job status (polled by frontend)"""

@flask_app.route('/api/job/<job_id>/notebooks')
def api_job_notebooks(job_id)
    """Return list of generated notebooks"""

@flask_app.route('/api/notebook/<notebook_id>/preview')
def api_notebook_preview(notebook_id)
    """Return notebook code for preview modal"""

@flask_app.route('/api/notebook/<notebook_id>/download')
def api_notebook_download(notebook_id)
    """Download single notebook file"""

@flask_app.route('/api/job/<job_id>/download-all')
def api_download_all(job_id)
    """Download all notebooks as ZIP"""

@flask_app.route('/api/job/<job_id>/deploy', methods=['POST'])
def api_deploy_job(job_id)
    """Create Databricks job with parallel tasks"""

@flask_app.route('/api/job/<job_id>/deploy-and-run', methods=['POST'])
def api_deploy_and_run(job_id)
    """Create Databricks job and execute immediately"""

@flask_app.route('/api/run/<run_id>/status')
def api_run_status(run_id)
    """Get current status of job run and all tasks (polled by frontend)"""

@flask_app.route('/api/run/<run_id>/tasks')
def api_task_statuses(run_id)
    """Get detailed status of all parallel tasks"""

@flask_app.route('/api/run/<run_id>/task/<task_key>/logs')
def api_task_logs(run_id, task_key)
    """Get execution logs for specific task"""

@flask_app.route('/api/run/<run_id>/cancel', methods=['POST'])
def api_cancel_run(run_id)
    """Cancel running job"""
```

### Frontend JavaScript
- Use vanilla JavaScript or lightweight library (Alpine.js, htmx)
- Polling mechanisms:
  - Agent conversion status (every 2-3 seconds) for Step 2
  - Job execution status (every 5 seconds) for Step 3b monitoring
- Progress bar animation
- Modal dialogs for previews and logs
- File downloads via blob URLs
- Auto-refresh toggle for monitoring view
- Real-time task status updates with color-coded indicators

---

## Key Technical Decisions & Questions

### ✅ Decided
1. **Framework**: Flask (per user preference over Streamlit)
2. **Hosting**: Databricks Apps platform
3. **Input Source**: Unity Catalog volumes (not file upload)
4. **UI Pattern**: Multi-step wizard (3 steps)
5. **Step 1**: Select NiFi XML file from UC volume
6. **Step 2**: Review agent-generated notebooks
7. **Conversion Pattern** (Confirmed with Alex):
   - One XML file → multiple independent flows
   - Each independent flow → one notebook
   - All notebooks are independent (no dependencies)
8. **Step 3 (Deployment)**:
   - Create single Databricks job with multiple parallel tasks
   - Each task runs one notebook (one flow)
   - No task dependencies (all run in parallel)
   - Notebooks saved to UC volume in structured path

### ❓ To Be Discussed with Team
1. **Agent Integration**:
   - How is the agentic conversion system invoked?
   - What's the API contract between Flask app and agent?
   - Where do agents store generated notebooks?
   - How does the app monitor agent progress?
   - How does agent identify and separate independent flows within XML?

3. **Data Persistence**:
   - Where to store conversion job metadata? (Delta table vs in-memory vs SQLite)
   - Should conversion history be preserved?
   - Multi-user considerations?

4. **Authentication & Authorization**:
   - How are users authenticated? (Databricks SSO handled automatically?)
   - What permissions are needed to access Unity Catalog volumes?
   - Service principal setup for app?

5. **Error Handling**:
   - What if agent fails mid-conversion?
   - Retry mechanism?
   - How to handle unsupported NiFi processors?
   - Validation before conversion starts?
   - What if some parallel tasks fail while others succeed?
   - Should failed tasks be retryable individually?

---

## Implementation Phases

### Phase 1: Basic Flask App Structure ✓ (Template Provided)
- [x] Basic Flask app with routing
- [x] Databricks app.yaml configuration
- [x] Hello world endpoint working

### Phase 2: UI Templates & Frontend
- [ ] Create base template with common layout
- [ ] Implement multi-step wizard progress indicator
- [ ] Build Step 1 UI (file selection)
- [ ] Build Step 2 UI (review/results)
- [ ] Add CSS styling (clean, professional theme)
- [ ] Implement JavaScript for interactivity and AJAX

### Phase 3: Unity Catalog Integration
- [ ] Implement `unity_catalog.py` service
- [ ] Connect to Databricks SDK
- [ ] List volumes and files
- [ ] Read XML file contents
- [ ] Test file permissions and access

### Phase 4: Agent Integration (Requires Team Input)
- [ ] Define agent API contract with team
- [ ] Implement `agent_service.py`
- [ ] Test agent invocation
- [ ] Implement job status polling
- [ ] Handle agent results retrieval

### Phase 5: Notebook Management
- [ ] Implement `notebook_service.py`
- [ ] Parse generated notebooks
- [ ] Create preview functionality
- [ ] Implement download (single & ZIP)
- [ ] Add syntax highlighting for code preview

### Phase 6: Job Tracking & State Management
- [ ] Choose persistence strategy (in-memory, Delta table, SQLite)
- [ ] Implement `ConversionJob` model
- [ ] Create job creation and tracking logic
- [ ] Add job history view (optional)

### Phase 7: Polish & Error Handling
- [ ] Add comprehensive error handling
- [ ] Implement user-friendly error messages
- [ ] Add loading states and animations
- [ ] Testing with real NiFi XML files
- [ ] Performance optimization

### Phase 8: Job Deployment & Monitoring
- [ ] Implement `job_deployment.py` service
- [ ] Create Databricks job via Jobs API
- [ ] Configure parallel tasks (no dependencies)
- [ ] Build Step 3 UI (deployment configuration)
- [ ] Implement job execution trigger
- [ ] Build Step 3b UI (execution monitoring)
- [ ] Implement real-time status polling (every 5 seconds)
- [ ] Add task status display with icons/colors
- [ ] Implement task log viewer
- [ ] Test job creation and execution
- [ ] Test monitoring with multiple parallel tasks
- [ ] Handle partial failures (some tasks succeed, some fail)

---

## Dependencies (`requirements.txt`)

```txt
flask>=3.0.0
pandas>=2.0.0
databricks-sdk>=0.20.0
pyyaml>=6.0
```

**Additional (as needed)**:
```txt
lxml>=4.9.0           # For NiFi XML parsing
jinja2>=3.1.0         # Template engine (included with Flask)
werkzeug>=3.0.0       # WSGI utilities (included with Flask)
pygments>=2.15.0      # For code syntax highlighting
```

---

## Verification & Testing Plan

### Manual Testing Steps
1. **Deploy Flask app to Databricks**:
   - Upload files to Databricks workspace
   - Create Databricks app from workspace files
   - Verify app.yaml configuration
   - Access app URL and confirm landing page loads

2. **Test Step 1 - File Selection**:
   - Navigate to Step 1
   - Verify Unity Catalog volume browser displays volumes
   - Select a test NiFi XML file
   - Confirm file selection UI updates correctly
   - Click "Preview XML" to verify XML content displays

3. **Test Step 2 - Conversion & Review**:
   - Click "Next: Start Conversion"
   - Verify conversion job starts
   - Monitor progress bar and status updates
   - Wait for completion
   - Verify generated notebooks are displayed
   - Test "Preview Code" modal for each notebook
   - Test "Download" for individual notebooks
   - Test "Download All" for ZIP archive

4. **Test Step 3 - Deployment & Execution Monitoring**:
   - Configure job name and cluster settings
   - Click "Create & Run Now"
   - Verify Databricks job is created with parallel tasks
   - Confirm redirect to monitoring view
   - Verify auto-refresh polling (every 5 seconds)
   - Monitor task status updates in real-time
   - Verify status icons change correctly (PENDING → RUNNING → SUCCESS/FAILED)
   - Test "View Logs" for individual tasks
   - Test overall progress bar calculation
   - Wait for all parallel tasks to complete
   - Verify completion notification
   - Test "View Job in Databricks" link

5. **Test Error Scenarios**:
   - Invalid XML file selection
   - Agent conversion failure
   - Network interruption during polling
   - Cancel conversion mid-process
   - Access denied to Unity Catalog volume

6. **Test Cross-Browser Compatibility**:
   - Chrome/Edge (primary)
   - Firefox
   - Safari (if accessible)

### Integration Testing
1. Verify Unity Catalog SDK connection
2. Test agent API integration (once contract is defined)
3. Verify notebook retrieval from agent output
4. Test file download functionality

### User Acceptance Testing
1. Share with team members (Bill, John, Alex, Alexandru, Cori)
2. Gather feedback on UI/UX
3. Verify it meets workflow expectations
4. Iterate based on feedback

---

## Open Questions for Eric

1. **Design Preferences**: Any specific color scheme or branding guidelines? (Default: Clean blue/white professional theme)

2. **Volume Access**: Do you already have test NiFi XML files in a Unity Catalog volume? What's the path?

3. **Agent Status**: Is the agentic conversion system already working? Can you provide:
   - How to invoke it (API endpoint, notebook path, etc.)
   - Expected input format
   - Expected output location
   - Status/progress mechanism

4. **User Authentication**: Will multiple users access this app, or just your team? Any specific permission requirements?

5. **Timeline**: When do you need the first visual draft ready for team review?

---

## Next Steps

1. **Eric**: Review this design plan and provide feedback
2. **Eric**: Coordinate with team (Alexandru, Alex, Cori) on agent integration details
3. **Implementation**: Start with Phase 2 (UI templates) since Flask foundation exists
4. **Iteration**: Build incrementally, review with team at each phase
5. **Team Discussion**: Schedule meeting to discuss Step 3 (deployment) requirements

---

## Summary

This Flask app will provide a user-friendly web interface for converting NiFi flows to Databricks notebooks, hosted on the Databricks platform. The multi-step wizard guides users through:

1. **Selecting** NiFi XML files from Unity Catalog volumes
2. **Reviewing** agent-generated Databricks notebooks
3. **Deploying** (future) notebooks as Databricks jobs/workflows

The architecture leverages the Databricks Flask app template structure, integrates with Unity Catalog for file storage, and coordinates with the agentic conversion system to transform NiFi workflows into production-ready Databricks notebooks.
