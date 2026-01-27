"""
Service for creating and managing dynamic Databricks jobs for flow conversions.

This service creates a new Databricks job for each flow conversion request,
replacing the old pattern of using a pre-existing job ID.
"""

import os
import logging
from datetime import datetime
from typing import Optional, List, Dict
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
import config as app_config


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
        flow_name: str,
        nifi_xml_path: str,
        run_id: str,
        flow_id: Optional[str] = None,
        catalog: str = "eliao",
        schema: str = "nifi_to_databricks"
    ) -> dict:
        """
        Create a new Databricks job for converting a NiFi flow.

        Args:
            flow_name: Flow name (XML filename without .xml)
            nifi_xml_path: Path to NiFi XML file in Unity Catalog volume
            run_id: Migration run identifier (format: "{flow_id}_run_{timestamp}")
            flow_id: Flow UUID from XML (optional, for display/reference)
            catalog: Catalog name for migration tables (default: eliao)
            schema: Schema name for migration tables (default: nifi_to_databricks)

        Returns:
            dict with job_id, job_name, flow_name, run_id

        Raises:
            Exception: If job creation fails
        """
        timestamp = int(datetime.now().timestamp())
        job_name = f"NiFi_Conversion_{flow_name}_{timestamp}"

        # Prepare job configuration
        job_config = {
            "name": job_name,
            "tasks": [
                {
                    "task_key": f"convert_{flow_name}",
                    "notebook_task": {
                        "notebook_path": self.agent_notebook_path,
                        "base_parameters": {
                            "run_id": run_id,                                          # NEW: migration run ID
                            "flow_id": flow_id or "unknown",                           # NEW: UUID from XML
                            "flow_name": flow_name,                                    # Keep for display/folder name
                            "nifi_xml_path": nifi_xml_path,
                            "output_path": f"/Volumes/{catalog}/{schema}/nifi_notebooks/{flow_name}",
                            "catalog": catalog,                                        # NEW: for migration tables
                            "schema": schema                                           # NEW: for migration tables
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
                "flow_name": flow_name,
                "flow_id": flow_id or "unknown",
                "run_id": run_id,
                "purpose": "nifi_conversion",
                "created_by": "nifi_accelerator_app"
            }
        }

        try:
            job = self.client.jobs.create(**job_config)
            self.logger.info(f"Created job {job.job_id} for flow {flow_name} (run {run_id})")

            return {
                "job_id": str(job.job_id),
                "job_name": job_name,
                "flow_name": flow_name,
                "run_id": run_id
            }
        except Exception as e:
            self.logger.error(f"Failed to create job for flow {flow_name}: {e}")
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
        flow_name: str,
        nifi_xml_path: str,
        run_id: str,
        flow_id: Optional[str] = None,
        catalog: str = "eliao",
        schema: str = "nifi_to_databricks"
    ) -> dict:
        """
        Create and immediately run a conversion job.

        Args:
            flow_name: Flow name (XML filename without .xml)
            nifi_xml_path: Path to NiFi XML file
            run_id: Migration run identifier
            flow_id: Flow UUID from XML (optional)
            catalog: Catalog name for migration tables
            schema: Schema name for migration tables

        Returns:
            dict with job_id, databricks_run_id, migration_run_id, flow_name

        Raises:
            Exception: If creation or execution fails
        """
        # Create the job
        job_info = self.create_conversion_job(
            flow_name=flow_name,
            nifi_xml_path=nifi_xml_path,
            run_id=run_id,
            flow_id=flow_id,
            catalog=catalog,
            schema=schema
        )

        # Run the job
        databricks_run_id = self.run_job(job_info['job_id'])

        return {
            "job_id": job_info['job_id'],
            "run_id": databricks_run_id,           # Databricks run ID
            "migration_run_id": run_id,            # Our migration run ID
            "flow_name": flow_name,
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

    def cleanup_old_jobs(self, flow_name: str, keep_latest: int = 3):
        """
        Clean up old conversion jobs for a flow, keeping only the N most recent.

        Args:
            flow_name: Flow identifier
            keep_latest: Number of recent jobs to keep (default: 3)

        Note:
            This is optional - you may want to keep all jobs for audit trail.
            Jobs can be manually deleted from Databricks UI if needed.
        """
        try:
            # List all jobs with the flow_name tag
            jobs = self.client.jobs.list()
            flow_jobs = [
                job for job in jobs
                if job.settings and job.settings.tags
                and job.settings.tags.get("flow_name") == flow_name
            ]

            # Sort by creation time (newest first)
            flow_jobs.sort(key=lambda j: j.created_time or 0, reverse=True)

            # Delete old jobs
            for job in flow_jobs[keep_latest:]:
                self.logger.info(f"Cleaning up old job {job.job_id} for flow {flow_name}")
                self.delete_job(str(job.job_id))

        except Exception as e:
            self.logger.warning(f"Failed to cleanup old jobs for flow {flow_name}: {e}")

    def create_execution_job(
        self,
        flow_name: str,
        run_id: str,
        iteration_num: int,
        notebook_paths: List[str],
        logs_path: str,
        catalog: str = "eliao",
        schema: str = "nifi_to_databricks"
    ) -> dict:
        """
        Create a Databricks job to execute generated notebooks and capture logs.

        Args:
            flow_name: Flow name for display
            run_id: Migration run identifier
            iteration_num: Current iteration number
            notebook_paths: List of notebook paths to execute
            logs_path: UC volume path to write execution logs
            catalog: Catalog name
            schema: Schema name

        Returns:
            dict with job_id, job_name, run_id

        Raises:
            Exception: If job creation fails
        """
        timestamp = int(datetime.now().timestamp())
        job_name = f"Execute_{flow_name}_iter{iteration_num}_{timestamp}"

        # Create tasks to run notebooks in parallel
        tasks = []
        for i, notebook_path in enumerate(notebook_paths):
            # Extract notebook name from path
            notebook_name = notebook_path.split('/')[-1].replace('.py', '')
            log_file_path = f"{logs_path}/{notebook_name}.log"

            tasks.append({
                "task_key": f"execute_{notebook_name}_{i}",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": {
                        "log_output_path": log_file_path
                    }
                },
                "new_cluster": {
                    "spark_version": app_config.DEFAULT_SPARK_VERSION,
                    "node_type_id": app_config.DEFAULT_NODE_TYPE,
                    "num_workers": app_config.DEFAULT_NUM_WORKERS
                },
                "timeout_seconds": app_config.EXECUTION_TIMEOUT_SECONDS,
                "max_retries": 0
            })

        # Prepare job configuration
        job_config = {
            "name": job_name,
            "tasks": tasks,
            "max_concurrent_runs": 1,
            "timeout_seconds": app_config.EXECUTION_TIMEOUT_SECONDS,
            "tags": {
                "flow_name": flow_name,
                "run_id": run_id,
                "iteration_num": str(iteration_num),
                "purpose": "notebook_execution",
                "created_by": "nifi_accelerator_app"
            }
        }

        try:
            job = self.client.jobs.create(**job_config)
            self.logger.info(f"Created execution job {job.job_id} for {flow_name} iteration {iteration_num}")

            return {
                "job_id": str(job.job_id),
                "job_name": job_name,
                "run_id": run_id,
                "iteration_num": iteration_num
            }
        except Exception as e:
            self.logger.error(f"Failed to create execution job: {e}")
            raise Exception(f"Execution job creation failed: {str(e)}")

    def create_and_run_agent_job(
        self,
        flow_name: str,
        nifi_xml_path: str,
        run_id: str,
        flow_id: Optional[str] = None,
        iteration_num: int = 1,
        logs_path: Optional[str] = None,
        previous_notebooks: Optional[List[str]] = None,
        catalog: str = "eliao",
        schema: str = "nifi_to_databricks"
    ) -> dict:
        """
        Create and run an agent job for code generation or error fixing.

        Args:
            flow_name: Flow name
            nifi_xml_path: Path to NiFi XML file
            run_id: Migration run identifier
            flow_id: Flow UUID from XML
            iteration_num: Iteration number (1 = generation, 2+ = fixing)
            logs_path: Path to execution logs (None for iteration 1, required for iteration 2+)
            previous_notebooks: List of notebook paths to fix (for iteration 2+)
            catalog: Catalog name
            schema: Schema name

        Returns:
            dict with job_id, run_id, iteration_num

        Raises:
            Exception: If creation or execution fails
        """
        timestamp = int(datetime.now().timestamp())
        job_name = f"Agent_{flow_name}_{run_id}_iter{iteration_num}_{timestamp}"

        # Build base parameters
        base_parameters = {
            "run_id": run_id,
            "flow_id": flow_id or "unknown",
            "flow_name": flow_name,
            "nifi_xml_path": nifi_xml_path,
            "output_path": f"/Volumes/{catalog}/{schema}/nifi_notebooks/{flow_name}",
            "catalog": catalog,
            "schema": schema,
            "iteration_num": str(iteration_num)
        }

        # Add iteration-specific parameters
        if logs_path:
            base_parameters["logs_path"] = logs_path
        if previous_notebooks:
            base_parameters["previous_notebooks"] = ",".join(previous_notebooks)

        # Prepare job configuration
        job_config = {
            "name": job_name,
            "tasks": [
                {
                    "task_key": f"agent_iter{iteration_num}",
                    "notebook_task": {
                        "notebook_path": self.agent_notebook_path,
                        "base_parameters": base_parameters
                    },
                    "new_cluster": {
                        "spark_version": app_config.DEFAULT_SPARK_VERSION,
                        "node_type_id": app_config.DEFAULT_NODE_TYPE,
                        "num_workers": app_config.DEFAULT_NUM_WORKERS,
                        "spark_conf": {
                            "spark.databricks.delta.preview.enabled": "true"
                        }
                    },
                    "timeout_seconds": app_config.AGENT_TIMEOUT_SECONDS,
                    "max_retries": 0
                }
            ],
            "max_concurrent_runs": 1,
            "timeout_seconds": app_config.AGENT_TIMEOUT_SECONDS,
            "tags": {
                "flow_name": flow_name,
                "flow_id": flow_id or "unknown",
                "run_id": run_id,
                "iteration_num": str(iteration_num),
                "purpose": "agent_conversion",
                "created_by": "nifi_accelerator_app"
            }
        }

        try:
            # Create the job
            job = self.client.jobs.create(**job_config)
            self.logger.info(f"Created agent job {job.job_id} for iteration {iteration_num}")

            # Run the job
            databricks_run_id = self.run_job(str(job.job_id))

            return {
                "job_id": str(job.job_id),
                "run_id": databricks_run_id,
                "migration_run_id": run_id,
                "flow_name": flow_name,
                "iteration_num": iteration_num,
                "job_name": job_name
            }
        except Exception as e:
            self.logger.error(f"Failed to create/run agent job: {e}")
            raise Exception(f"Agent job creation/execution failed: {str(e)}")
