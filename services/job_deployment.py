from typing import List, Optional, Dict
from datetime import datetime
from models.nifi_flow import Notebook
from models.conversion_job import JobRunStatus, TaskStatus
from utils.databricks_client import DatabricksClientWrapper


class JobDeploymentService:
    """Service for creating and monitoring Databricks jobs."""

    def __init__(self, databricks_client: DatabricksClientWrapper):
        self.client = databricks_client

    def create_parallel_job(
        self,
        job_name: str,
        notebooks: List[Notebook],
        cluster_config: Optional[Dict] = None
    ) -> str:
        """
        Create a Databricks job with parallel tasks (one per notebook).

        Args:
            job_name: Name for the job
            notebooks: List of notebooks to execute
            cluster_config: Optional cluster configuration

        Returns:
            Job ID
        """
        if not notebooks:
            raise Exception("No notebooks provided for job creation")

        # Default cluster configuration
        if cluster_config is None:
            cluster_config = {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2,
                "spark_conf": {
                    "spark.databricks.delta.preview.enabled": "true"
                }
            }

        # Build task list - all tasks are independent (no dependencies)
        tasks = []
        for idx, notebook in enumerate(notebooks):
            task = {
                "task_key": f"task_{idx}_{notebook.flow_id}",
                "notebook_task": {
                    "notebook_path": notebook.volume_path,
                    "source": "WORKSPACE"
                },
                "new_cluster": cluster_config,
                "timeout_seconds": 3600,
                "max_retries": 0
            }
            tasks.append(task)

        # Create job configuration
        job_config = {
            "name": job_name,
            "tasks": tasks,
            "format": "MULTI_TASK",
            "max_concurrent_runs": 1
        }

        try:
            job_id = self.client.create_job(job_config)
            return job_id
        except Exception as e:
            raise Exception(f"Failed to create job: {str(e)}")

    def run_job(self, job_id: str) -> str:
        """
        Run a Databricks job.

        Args:
            job_id: The job ID to run

        Returns:
            Run ID
        """
        try:
            run_id = self.client.run_job(job_id)
            return run_id
        except Exception as e:
            raise Exception(f"Failed to run job: {str(e)}")

    def get_job_run_status(self, run_id: str) -> JobRunStatus:
        """
        Get the status of a job run.

        Args:
            run_id: The run ID

        Returns:
            JobRunStatus object with overall status and task details
        """
        try:
            run = self.client.get_run_status(run_id)

            # Extract overall run information
            state = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "UNKNOWN"
            start_time = datetime.fromtimestamp(run.start_time / 1000) if run.start_time else datetime.now()
            end_time = datetime.fromtimestamp(run.end_time / 1000) if run.end_time else None

            duration_seconds = 0
            if run.end_time and run.start_time:
                duration_seconds = (run.end_time - run.start_time) // 1000

            # Extract task statuses
            task_statuses = []
            if run.tasks:
                for task in run.tasks:
                    task_state = task.state.life_cycle_state.value if task.state and task.state.life_cycle_state else "UNKNOWN"
                    task_start = datetime.fromtimestamp(task.start_time / 1000) if task.start_time else None
                    task_end = datetime.fromtimestamp(task.end_time / 1000) if task.end_time else None

                    task_duration = None
                    if task.end_time and task.start_time:
                        task_duration = (task.end_time - task.start_time) // 1000

                    error_message = None
                    if task.state and task.state.state_message:
                        error_message = task.state.state_message

                    notebook_path = ""
                    if task.notebook_task and task.notebook_task.notebook_path:
                        notebook_path = task.notebook_task.notebook_path

                    task_status = TaskStatus(
                        task_key=task.task_key,
                        notebook_path=notebook_path,
                        state=task_state,
                        start_time=task_start,
                        end_time=task_end,
                        duration_seconds=task_duration,
                        error_message=error_message
                    )
                    task_statuses.append(task_status)

            return JobRunStatus(
                run_id=str(run_id),
                state=state,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration_seconds,
                tasks=task_statuses
            )
        except Exception as e:
            raise Exception(f"Failed to get job run status: {str(e)}")

    def get_task_statuses(self, run_id: str) -> List[TaskStatus]:
        """
        Get the status of all tasks in a job run.

        Args:
            run_id: The run ID

        Returns:
            List of TaskStatus objects
        """
        job_status = self.get_job_run_status(run_id)
        return job_status.tasks

    def get_task_logs(self, run_id: str, task_key: str) -> str:
        """
        Get logs for a specific task.

        Args:
            run_id: The run ID
            task_key: The task key

        Returns:
            Task logs as string
        """
        # TODO: Implement log retrieval from Databricks API
        # For now, return a placeholder
        return f"Logs for task {task_key} in run {run_id}\n\nLog retrieval not yet implemented."

    def cancel_job_run(self, run_id: str) -> bool:
        """
        Cancel a running job.

        Args:
            run_id: The run ID to cancel

        Returns:
            True if successful
        """
        try:
            self.client.cancel_run(run_id)
            return True
        except Exception as e:
            raise Exception(f"Failed to cancel job run: {str(e)}")

    def create_and_run_job(
        self,
        job_name: str,
        notebooks: List[Notebook],
        cluster_config: Optional[Dict] = None
    ) -> tuple[str, str]:
        """
        Create a job and run it immediately.

        Args:
            job_name: Name for the job
            notebooks: List of notebooks to execute
            cluster_config: Optional cluster configuration

        Returns:
            Tuple of (job_id, run_id)
        """
        job_id = self.create_parallel_job(job_name, notebooks, cluster_config)
        run_id = self.run_job(job_id)
        return job_id, run_id
