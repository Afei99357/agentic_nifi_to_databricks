"""
Service for creating and managing dynamic Databricks jobs for flow conversions.

This service creates a new Databricks job for each flow conversion request,
replacing the old pattern of using a pre-existing job ID.
"""

import os
import logging
from datetime import datetime
from typing import Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config


class DynamicJobService:
    """Service for creating dynamic Databricks jobs for flow conversions."""

    def __init__(self, agent_notebook_path: Optional[str] = None):
        """
        Initialize dynamic job service.

        Args:
            agent_notebook_path: Path to agent notebook in Workspace.
                                Defaults to env var AGENT_NOTEBOOK_PATH or
                                /Workspace/Shared/nifi_conversion_dummy_agent
        """
        self.client = WorkspaceClient()
        self.agent_notebook_path = (
            agent_notebook_path
            or os.getenv("AGENT_NOTEBOOK_PATH", "/Workspace/Shared/nifi_conversion_dummy_agent")
        )
        self.logger = logging.getLogger(__name__)

    def create_conversion_job(
        self,
        flow_id: str,
        nifi_xml_path: str,
        attempt_id: str,
        flow_name: Optional[str] = None
    ) -> dict:
        """
        Create a new Databricks job for converting a NiFi flow.

        Args:
            flow_id: Flow identifier
            nifi_xml_path: Path to NiFi XML file in Unity Catalog volume
            attempt_id: Unique attempt identifier for history tracking
            flow_name: Optional flow name for job naming

        Returns:
            dict with job_id, job_name, flow_id, attempt_id

        Raises:
            Exception: If job creation fails
        """
        timestamp = int(datetime.now().timestamp())
        display_name = flow_name or flow_id
        job_name = f"NiFi_Conversion_{display_name}_{timestamp}"

        # Prepare job configuration
        job_config = {
            "name": job_name,
            "tasks": [
                {
                    "task_key": f"convert_{flow_id}",
                    "notebook_task": {
                        "notebook_path": self.agent_notebook_path,
                        "base_parameters": {
                            "flow_id": flow_id,
                            "nifi_xml_path": nifi_xml_path,
                            "output_path": f"/Volumes/main/default/nifi_notebooks/{flow_id}",
                            "attempt_id": attempt_id,
                            "delta_table": "main.default.nifi_conversion_history"
                        }
                    },
                    "new_cluster": {
                        "spark_version": "13.3.x-scala2.12",
                        "node_type_id": "i3.xlarge",
                        "num_workers": 2,
                        "spark_conf": {
                            "spark.databricks.delta.preview.enabled": "true"
                        }
                    },
                    "timeout_seconds": 7200,  # 2 hours
                    "max_retries": 0
                }
            ],
            "max_concurrent_runs": 1,
            "timeout_seconds": 7200,
            "tags": {
                "flow_id": flow_id,
                "attempt_id": attempt_id,
                "purpose": "nifi_conversion",
                "created_by": "nifi_accelerator_app"
            }
        }

        try:
            job = self.client.jobs.create(**job_config)
            self.logger.info(f"Created job {job.job_id} for flow {flow_id} (attempt {attempt_id})")

            return {
                "job_id": str(job.job_id),
                "job_name": job_name,
                "flow_id": flow_id,
                "attempt_id": attempt_id
            }
        except Exception as e:
            self.logger.error(f"Failed to create job for flow {flow_id}: {e}")
            raise Exception(f"Job creation failed: {str(e)}")

    def run_job(self, job_id: str) -> str:
        """
        Run a Databricks job and return the run ID.

        Args:
            job_id: Databricks job ID (as string)

        Returns:
            run_id (string)

        Raises:
            Exception: If job run fails
        """
        try:
            run = self.client.jobs.run_now(job_id=int(job_id))
            run_id = str(run.run_id)
            self.logger.info(f"Started job {job_id}, run ID: {run_id}")
            return run_id
        except Exception as e:
            self.logger.error(f"Failed to run job {job_id}: {e}")
            raise Exception(f"Job run failed: {str(e)}")

    def create_and_run_job(
        self,
        flow_id: str,
        nifi_xml_path: str,
        attempt_id: str,
        flow_name: Optional[str] = None
    ) -> dict:
        """
        Create and immediately run a conversion job.

        Args:
            flow_id: Flow identifier
            nifi_xml_path: Path to NiFi XML file
            attempt_id: Unique attempt identifier
            flow_name: Optional flow name for job naming

        Returns:
            dict with job_id, run_id, attempt_id, flow_id

        Raises:
            Exception: If creation or execution fails
        """
        # Create the job
        job_info = self.create_conversion_job(
            flow_id=flow_id,
            nifi_xml_path=nifi_xml_path,
            attempt_id=attempt_id,
            flow_name=flow_name
        )

        # Run the job
        run_id = self.run_job(job_info['job_id'])

        return {
            "job_id": job_info['job_id'],
            "run_id": run_id,
            "attempt_id": attempt_id,
            "flow_id": flow_id,
            "job_name": job_info['job_name']
        }

    def delete_job(self, job_id: str):
        """
        Delete a Databricks job (cleanup after conversion).

        Args:
            job_id: Databricks job ID to delete

        Note:
            Use this carefully - only delete jobs after confirming they're complete
            and no longer needed. Consider implementing a retention policy instead.
        """
        try:
            self.client.jobs.delete(job_id=int(job_id))
            self.logger.info(f"Deleted job {job_id}")
        except Exception as e:
            self.logger.warning(f"Failed to delete job {job_id}: {e}")

    def get_job_info(self, job_id: str) -> dict:
        """
        Get information about a job.

        Args:
            job_id: Databricks job ID

        Returns:
            dict with job details
        """
        try:
            job = self.client.jobs.get(job_id=int(job_id))
            return {
                "job_id": str(job.job_id),
                "name": job.settings.name if job.settings else "Unknown",
                "created_time": job.created_time,
                "creator_user_name": job.creator_user_name
            }
        except Exception as e:
            self.logger.error(f"Failed to get job info for {job_id}: {e}")
            return {}

    def cleanup_old_jobs(self, flow_id: str, keep_latest: int = 3):
        """
        Clean up old conversion jobs for a flow, keeping only the N most recent.

        Args:
            flow_id: Flow identifier
            keep_latest: Number of recent jobs to keep (default: 3)

        Note:
            This is optional - you may want to keep all jobs for audit trail.
            Jobs can be manually deleted from Databricks UI if needed.
        """
        try:
            # List all jobs with the flow_id tag
            jobs = self.client.jobs.list()
            flow_jobs = [
                job for job in jobs
                if job.settings and job.settings.tags
                and job.settings.tags.get("flow_id") == flow_id
            ]

            # Sort by creation time (newest first)
            flow_jobs.sort(key=lambda j: j.created_time or 0, reverse=True)

            # Delete old jobs
            for job in flow_jobs[keep_latest:]:
                self.logger.info(f"Cleaning up old job {job.job_id} for flow {flow_id}")
                self.delete_job(str(job.job_id))

        except Exception as e:
            self.logger.warning(f"Failed to cleanup old jobs for flow {flow_id}: {e}")
