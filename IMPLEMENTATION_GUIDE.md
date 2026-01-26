# Dynamic Job Creation & Conversion History Tracking - Implementation Guide

**Implementation Date:** 2026-01-26
**Status:** ✅ Complete

---

## Overview

This implementation replaces the old static job ID pattern with **dynamic job creation**, where each flow conversion creates a new Databricks job. It also adds **complete history tracking** for all conversion attempts, enabling users to:

- View all previous conversion attempts for a flow
- Compare failed attempts to identify patterns
- Track successful vs failed conversions
- Maintain a complete audit trail

---

## Changes Summary

### New Files Created

1. **`setup/create_history_table.sql`** - Creates the `nifi_conversion_history` Delta table
2. **`setup/update_flows_table_schema.sql`** - Adds new columns to `nifi_flows` table
3. **`setup/migrate_existing_data.sql`** - Migrates existing conversion data to new schema
4. **`notebooks/nifi_conversion_dummy_agent.py`** - Dummy agent notebook for testing
5. **`services/dynamic_job_service.py`** - Service for creating dynamic Databricks jobs

### Modified Files

1. **`services/delta_service.py`** - Added history tracking methods
2. **`services/flow_service.py`** - Updated to use dynamic job creation
3. **`models/nifi_flow_record.py`** - Added new history-related fields
4. **`app.py`** - Added history API endpoint
5. **`templates/dashboard.html`** - Added history view UI
6. **`app.yaml`** - Removed old job ID, added agent notebook path

---

## Database Schema Changes

### New Table: `nifi_conversion_history`

Tracks all conversion attempts:

```sql
CREATE TABLE main.default.nifi_conversion_history (
  attempt_id STRING PRIMARY KEY,
  flow_id STRING NOT NULL,
  databricks_job_id STRING NOT NULL,
  databricks_run_id STRING,
  job_name STRING,
  status STRING NOT NULL,  -- CREATING, RUNNING, SUCCESS, FAILED, CANCELLED
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  duration_seconds INT,
  progress_percentage INT,
  iterations INT,
  validation_percentage INT,
  generated_notebooks ARRAY<STRING>,
  error_message STRING,
  status_message STRING,
  attempt_number INT,
  triggered_by STRING
);
```

### Updated Table: `nifi_flows`

Added new columns:

- `current_attempt_id` - Reference to current attempt
- `total_attempts` - Total number of attempts
- `successful_conversions` - Count of successful conversions
- `last_attempt_at` - Timestamp of most recent attempt
- `first_attempt_at` - Timestamp of first attempt

---

## Deployment Steps

### Step 1: Database Setup

Run these SQL scripts **in order** in Databricks SQL Editor or notebook:

```sql
-- 1. Create history table
%run /Workspace/Repos/your-repo/setup/create_history_table.sql

-- 2. Update flows table schema
%run /Workspace/Repos/your-repo/setup/update_flows_table_schema.sql

-- 3. Migrate existing data (if you have existing flows)
%run /Workspace/Repos/your-repo/setup/migrate_existing_data.sql
```

**Verification:**

```sql
-- Verify history table
DESCRIBE TABLE main.default.nifi_conversion_history;

-- Verify flows table has new columns
DESCRIBE TABLE main.default.nifi_flows;

-- Check migrated data
SELECT flow_id, total_attempts, successful_conversions
FROM main.default.nifi_flows
WHERE total_attempts > 0;
```

### Step 2: Upload Dummy Agent Notebook

Upload the dummy agent notebook to Databricks Workspace:

```bash
# Using Databricks CLI
databricks workspace import \
  notebooks/nifi_conversion_dummy_agent.py \
  /Workspace/Shared/nifi_conversion_dummy_agent \
  --language PYTHON
```

**Or manually:**

1. Go to Databricks Workspace
2. Navigate to `/Workspace/Shared/`
3. Click "Create" → "Notebook"
4. Name it `nifi_conversion_dummy_agent`
5. Copy/paste the content from `notebooks/nifi_conversion_dummy_agent.py`

### Step 3: Deploy Updated App

Deploy the updated application to Databricks Apps:

```bash
# Using Databricks CLI
databricks apps deploy nifi-accelerator \
  --source-code-path /home/eric/Projects/agentic_nifi_accelerator_app
```

**Or using the Databricks UI:**

1. Go to Apps in Databricks
2. Find your NiFi Accelerator app
3. Click "Update"
4. Upload the updated code

### Step 4: Verify Deployment

1. **Check app is running:**
   - Go to Databricks Apps
   - Open NiFi Accelerator app
   - Dashboard should load without errors

2. **Test single flow conversion:**
   - Select a flow
   - Click "Start Conversion"
   - Verify new job is created in Jobs UI
   - Check history table for new attempt record

3. **Test history view:**
   - Click on a flow with attempts
   - Click "View All Attempts"
   - Verify history table displays correctly

---

## How It Works

### Conversion Flow (Before → After)

**Before (Static Job):**
```
User clicks "Start"
  → FlowService.start_conversion()
  → Uses NIFI_CONVERSION_JOB_ID (pre-existing job)
  → jobs.run_now(job_id, params)
  → Updates nifi_flows table
  → Previous attempt data overwritten ❌
```

**After (Dynamic Job Creation):**
```
User clicks "Start"
  → FlowService.start_conversion()
  → Creates attempt record in history table
  → DynamicJobService.create_and_run_job()
    → Creates NEW Databricks job for this flow
    → Passes attempt_id to agent notebook
  → Updates history table with job_id and run_id
  → Agent updates progress in history table
  → All attempts preserved ✅
```

### History Tracking

Each conversion attempt creates a new record:

```python
# Attempt 1 (Failed)
{
  "attempt_id": "flow123_attempt_1706234567",
  "attempt_number": 1,
  "status": "FAILED",
  "error_message": "Invalid XML structure"
}

# Attempt 2 (Success)
{
  "attempt_id": "flow123_attempt_1706234890",
  "attempt_number": 2,
  "status": "SUCCESS",
  "generated_notebooks": [...]
}
```

---

## API Changes

### New Endpoint

**`GET /api/flows/<flow_id>/history`**

Get conversion history for a flow.

**Query Parameters:**
- `limit` (optional, default: 20) - Max number of attempts to return

**Response:**
```json
{
  "success": true,
  "flow_id": "flow123",
  "history": [
    {
      "attempt_id": "flow123_attempt_1706234890",
      "attempt_number": 2,
      "status": "SUCCESS",
      "started_at": "2026-01-26T10:30:00",
      "completed_at": "2026-01-26T10:45:00",
      "duration_seconds": 900,
      "progress_percentage": 100,
      "generated_notebooks": [...]
    },
    {
      "attempt_id": "flow123_attempt_1706234567",
      "attempt_number": 1,
      "status": "FAILED",
      "error_message": "Invalid XML structure"
    }
  ],
  "count": 2
}
```

### Modified Endpoint

**`POST /api/flows/<flow_id>/start`**

Now returns additional fields:

```json
{
  "success": true,
  "job_id": "123456789",      // NEW: Databricks job ID
  "run_id": "987654321",
  "attempt_id": "flow123_attempt_1706234890",  // NEW
  "flow_id": "flow123"
}
```

---

## UI Changes

### Dashboard Updates

1. **Flow Details Modal** now shows:
   - Total attempts count
   - Successful conversions count
   - First/last attempt timestamps
   - "View All Attempts" button (if `total_attempts > 0`)

2. **History Table** displays:
   - Attempt number
   - Status badge
   - Start/completion timestamps
   - Duration
   - Progress percentage
   - Error message (truncated)

---

## Testing Guide

### Test Case 1: First Conversion

**Steps:**
1. Select a flow with status "NOT_STARTED"
2. Click "Start Conversion"
3. Wait for completion

**Expected Results:**
- ✅ New Databricks job created (check Jobs UI)
- ✅ History table has 1 record with `attempt_number = 1`
- ✅ Flow shows `total_attempts = 1`
- ✅ Flow status updates to "DONE" on success

**Verification SQL:**
```sql
SELECT h.*
FROM main.default.nifi_conversion_history h
JOIN main.default.nifi_flows f ON h.flow_id = f.flow_id
WHERE f.flow_name = 'YourFlowName'
ORDER BY h.started_at DESC;
```

### Test Case 2: Restart After Failure

**Steps:**
1. Select a flow with status "NEEDS_ATTENTION" (failed)
2. Click "Start Conversion" again
3. Wait for completion

**Expected Results:**
- ✅ New job created (different `job_id` from first attempt)
- ✅ History table has 2 records with `attempt_number = 1, 2`
- ✅ Flow shows `total_attempts = 2`
- ✅ Both attempts visible in history view
- ✅ Previous attempt data preserved

**Verification SQL:**
```sql
SELECT
  attempt_number,
  status,
  databricks_job_id,
  started_at,
  error_message
FROM main.default.nifi_conversion_history
WHERE flow_id = 'flow123'
ORDER BY attempt_number;
```

### Test Case 3: Bulk Conversions

**Steps:**
1. Select 3 flows
2. Click "Launch Selected Conversions"
3. Monitor progress

**Expected Results:**
- ✅ 3 separate Databricks jobs created
- ✅ 3 history records created
- ✅ All jobs run in parallel
- ✅ Dashboard updates all 3 in real-time

### Test Case 4: View History

**Steps:**
1. Open flow details for a flow with `total_attempts > 1`
2. Click "View All Attempts (N)"
3. Verify history table displays

**Expected Results:**
- ✅ Table shows all attempts
- ✅ Most recent attempt at top
- ✅ Status badges colored correctly
- ✅ Duration calculated properly
- ✅ Error messages displayed (truncated to 50 chars)

---

## Troubleshooting

### Issue: "Conversion job not configured"

**Cause:** Old code still looking for `NIFI_CONVERSION_JOB_ID`

**Fix:**
```bash
# Verify app.yaml has correct config
grep "AGENT_NOTEBOOK_PATH" app.yaml
# Should show: /Workspace/Shared/nifi_conversion_dummy_agent

# Redeploy app
databricks apps deploy nifi-accelerator --source-code-path .
```

### Issue: "Flow not found" when creating attempt

**Cause:** Flow doesn't exist in `nifi_flows` table

**Fix:**
```sql
-- Check if flow exists
SELECT * FROM main.default.nifi_flows WHERE flow_id = 'your_flow_id';

-- If not, populate flows from volume
%run /Workspace/Repos/your-repo/setup/populate_flows_from_volume.py
```

### Issue: History table not found

**Cause:** SQL setup scripts not run

**Fix:**
```sql
-- Run in order:
%run setup/create_history_table.sql
%run setup/update_flows_table_schema.sql
%run setup/migrate_existing_data.sql
```

### Issue: Agent notebook not found

**Cause:** Notebook path incorrect in app.yaml

**Fix:**
```bash
# Check notebook exists
databricks workspace list /Workspace/Shared/

# Upload if missing
databricks workspace import \
  notebooks/nifi_conversion_dummy_agent.py \
  /Workspace/Shared/nifi_conversion_dummy_agent \
  --language PYTHON

# Update app.yaml if path different
env:
  - name: AGENT_NOTEBOOK_PATH
    value: "/Workspace/Shared/nifi_conversion_dummy_agent"
```

### Issue: Job creation fails with "Invalid cluster config"

**Cause:** Cluster settings in `dynamic_job_service.py` may not match your workspace

**Fix:**
Edit `services/dynamic_job_service.py`:

```python
"new_cluster": {
    "spark_version": "13.3.x-scala2.12",  # Update to available version
    "node_type_id": "i3.xlarge",          # Update to available node type
    "num_workers": 2
}
```

Check available versions:
```bash
databricks clusters spark-versions
databricks clusters list-node-types
```

---

## Migration from Old System

If you have existing flows with conversion data:

### Before Migration

**Backup existing data:**
```sql
CREATE TABLE main.default.nifi_flows_backup AS
SELECT * FROM main.default.nifi_flows;
```

### After Migration

**Verify migration:**
```sql
-- Check all flows with conversions were migrated
SELECT
  f.flow_id,
  f.flow_name,
  f.total_attempts,
  h.status,
  h.databricks_run_id
FROM main.default.nifi_flows f
LEFT JOIN main.default.nifi_conversion_history h
  ON f.current_attempt_id = h.attempt_id
WHERE f.total_attempts > 0;
```

---

## Next Steps

### Replace Dummy Agent with Real Agent

When the agent team provides the real conversion notebook:

1. Update `AGENT_NOTEBOOK_PATH` in `app.yaml`:
   ```yaml
   - name: AGENT_NOTEBOOK_PATH
     value: "/Workspace/Shared/nifi_conversion_real_agent"
   ```

2. Ensure real agent notebook:
   - Accepts same parameters: `flow_id`, `nifi_xml_path`, `output_path`, `attempt_id`, `delta_table`
   - Updates `nifi_conversion_history` table with progress
   - Sets final status: SUCCESS or FAILED
   - Saves generated notebooks to `output_path`

3. Redeploy app

### Optional: Job Cleanup

Implement retention policy for old jobs:

```python
# In flow_service.py, after successful conversion:
self.dynamic_job_service.cleanup_old_jobs(flow_id, keep_latest=3)
```

### Optional: Email Notifications

Add notifications when conversions complete:

```python
# In poll_flow_status(), when status becomes SUCCESS or FAILED:
if result_state == 'SUCCESS':
    notify_user(flow.owner, f"Conversion completed: {flow.flow_name}")
```

---

## Support

For issues or questions:

1. Check this guide's Troubleshooting section
2. Review logs in Databricks job runs
3. Query history table to diagnose conversion issues
4. Contact: [Your team/support contact]

---

## Appendix: Key Code Locations

| Component | File | Line Range |
|-----------|------|------------|
| Job creation | `services/dynamic_job_service.py` | 30-75 |
| History creation | `services/delta_service.py` | 129-188 |
| Conversion start | `services/flow_service.py` | 30-111 |
| Status polling | `services/flow_service.py` | 135-227 |
| History API | `app.py` | 138-152 |
| History UI | `templates/dashboard.html` | 690-741 |

---

**Implementation Complete! 🎉**

All tasks completed successfully. The system now supports:
- ✅ Dynamic job creation for each flow conversion
- ✅ Complete history tracking of all attempts
- ✅ UI for viewing conversion history
- ✅ Backward compatibility with existing flows
- ✅ Ready for real agent integration
