from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, workspace
import os


class DatabricksClientWrapper:
    """Wrapper around Databricks SDK for easier access to common operations."""

    def __init__(self):
        self.client = WorkspaceClient()

    def get_workspace_client(self) -> WorkspaceClient:
        """Get the underlying workspace client."""
        return self.client

    def get_current_user(self) -> str:
        """Get the current user's email or ID."""
        try:
            current_user = self.client.current_user.me()
            return current_user.user_name or current_user.id
        except Exception:
            return "unknown_user"

    def list_volumes(self, catalog: str, schema: str):
        """List volumes in a catalog and schema."""
        try:
            volumes = self.client.volumes.list(catalog_name=catalog, schema_name=schema)
            return list(volumes)
        except Exception as e:
            raise Exception(f"Failed to list volumes: {str(e)}")

    def read_file(self, file_path: str) -> bytes:
        """Read a file from Unity Catalog volume."""
        try:
            with open(file_path, 'rb') as f:
                return f.read()
        except Exception as e:
            raise Exception(f"Failed to read file {file_path}: {str(e)}")

    def write_file(self, file_path: str, content: bytes):
        """Write a file to Unity Catalog volume."""
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'wb') as f:
                f.write(content)
        except Exception as e:
            raise Exception(f"Failed to write file {file_path}: {str(e)}")

    def create_job(self, job_config: dict) -> str:
        """Create a Databricks job and return job ID."""
        try:
            job = self.client.jobs.create(**job_config)
            return str(job.job_id)
        except Exception as e:
            raise Exception(f"Failed to create job: {str(e)}")

    def run_job(self, job_id: str) -> str:
        """Run a Databricks job and return run ID."""
        try:
            run = self.client.jobs.run_now(job_id=int(job_id))
            return str(run.run_id)
        except Exception as e:
            raise Exception(f"Failed to run job: {str(e)}")

    def get_run_status(self, run_id: str):
        """Get the status of a job run."""
        try:
            run = self.client.jobs.get_run(run_id=int(run_id))
            return run
        except Exception as e:
            raise Exception(f"Failed to get run status: {str(e)}")

    def cancel_run(self, run_id: str):
        """Cancel a job run."""
        try:
            self.client.jobs.cancel_run(run_id=int(run_id))
        except Exception as e:
            raise Exception(f"Failed to cancel run: {str(e)}")
