"""
Service for interacting with the new migration_* Delta tables.
Uses Databricks SQL Connector following Cori's proposed architecture.

Philosophy: "The App should never 'compute' progress. It should display
            the latest rows and statuses written by the job."
"""

import os
from typing import List, Optional
from datetime import datetime
from databricks import sql
from databricks.sdk.core import Config
from models.migration_flow import MigrationFlow
from models.migration_run import MigrationRun
from models.migration_iteration import MigrationIteration
from models.migration_patch import MigrationPatch
from models.migration_sast_result import MigrationSastResult
from models.migration_exec_result import MigrationExecResult
from models.migration_human_request import MigrationHumanRequest


class MigrationService:
    """Interface to read/write the migration_* Delta tables via SQL Warehouse."""

    def __init__(self):
        """Initialize with SQL Warehouse configuration using Config class."""
        import logging

        # Use Config class (official Databricks Apps pattern)
        self.cfg = Config()

        # Table names
        self.schema = "eliao.nifi_to_databricks"
        self.flows_table = f"{self.schema}.migration_flows"
        self.runs_table = f"{self.schema}.migration_runs"
        self.iterations_table = f"{self.schema}.migration_iterations"
        self.patches_table = f"{self.schema}.migration_patches"
        self.sast_table = f"{self.schema}.migration_sast_results"
        self.exec_table = f"{self.schema}.migration_exec_results"
        self.human_table = f"{self.schema}.migration_human_requests"

        # Connection will be created lazily
        self._connection = None

        warehouse_id = getattr(self.cfg, 'warehouse_id', 'NOT_SET')
        logging.info(f"MigrationService configured: schema={self.schema}, warehouse={warehouse_id}")

    def _get_connection(self):
        """Get or create SQL connection using exact Databricks Dash template pattern."""
        import logging

        if self._connection is None or not self._connection.open:
            logging.info(f"Connecting to SQL Warehouse: host={self.cfg.host}, warehouse={self.cfg.warehouse_id}")

            try:
                self._connection = sql.connect(
                    server_hostname=self.cfg.host,
                    http_path=f"/sql/1.0/warehouses/{self.cfg.warehouse_id}",
                    credentials_provider=lambda: self.cfg.authenticate
                )
                logging.info("✓ SQL Warehouse connection established")
            except Exception as e:
                logging.error(f"Connection failed: {type(e).__name__}: {str(e)}")
                import traceback
                logging.error(f"Traceback: {traceback.format_exc()}")
                raise

        return self._connection

    def _row_to_dict(self, row, description):
        """Convert SQL result row to dictionary."""
        return {col[0]: row[i] for i, col in enumerate(description)}

    # ===== MIGRATION_FLOWS METHODS =====

    def list_all_flows(self) -> List[MigrationFlow]:
        """Get all flows from migration_flows table."""
        import logging
        import traceback
        try:
            logging.info(f"Querying {self.flows_table}")
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(f"SELECT * FROM {self.flows_table}")
            rows = cursor.fetchall()
            description = cursor.description
            cursor.close()

            records = [MigrationFlow.from_dict(self._row_to_dict(row, description))
                      for row in rows]

            logging.info(f"Successfully fetched {len(records)} flows")
            return records
        except Exception as e:
            logging.error(f"Error in list_all_flows: {type(e).__name__}: {str(e)}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            raise Exception(f"Failed to read flows from Delta table: {str(e)}")

    def get_flow(self, flow_id: str) -> Optional[MigrationFlow]:
        """Get a single flow by flow_id."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT * FROM {self.flows_table} WHERE flow_id = ?",
                (flow_id,)
            )

            row = cursor.fetchone()
            description = cursor.description
            cursor.close()

            if row:
                return MigrationFlow.from_dict(self._row_to_dict(row, description))
            return None
        except Exception as e:
            raise Exception(f"Failed to get flow {flow_id}: {str(e)}")

    def get_flow_by_name(self, flow_name: str) -> Optional[MigrationFlow]:
        """Get a single flow by flow_name."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT * FROM {self.flows_table} WHERE flow_name = ?",
                (flow_name,)
            )

            row = cursor.fetchone()
            description = cursor.description
            cursor.close()

            if row:
                return MigrationFlow.from_dict(self._row_to_dict(row, description))
            return None
        except Exception as e:
            raise Exception(f"Failed to get flow by name {flow_name}: {str(e)}")

    def update_flow_status(self, flow_id: str, migration_status: str, current_run_id: Optional[str] = None):
        """
        Update flow status and optionally current_run_id.

        Args:
            flow_id: Flow identifier
            migration_status: New status (NOT_STARTED | RUNNING | NEEDS_HUMAN | FAILED | DONE | STOPPED)
            current_run_id: Optional run_id to link
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            if current_run_id:
                cursor.execute(f"""
                    UPDATE {self.flows_table}
                    SET migration_status = ?,
                        current_run_id = ?,
                        last_update_ts = current_timestamp()
                    WHERE flow_id = ?
                """, (migration_status, current_run_id, flow_id))
            else:
                cursor.execute(f"""
                    UPDATE {self.flows_table}
                    SET migration_status = ?,
                        last_update_ts = current_timestamp()
                    WHERE flow_id = ?
                """, (migration_status, flow_id))

            cursor.close()
        except Exception as e:
            raise Exception(f"Failed to update flow {flow_id}: {str(e)}")

    def insert_flow(self, flow: MigrationFlow):
        """Insert a new flow into migration_flows table."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(f"""
                INSERT INTO {self.flows_table}
                (flow_id, flow_name, migration_status, current_run_id, last_update_ts,
                 server, nifi_xml_path, description, priority, owner)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                flow.flow_id,
                flow.flow_name,
                flow.migration_status,
                flow.current_run_id,
                flow.last_update_ts or datetime.now(),
                flow.server,
                flow.nifi_xml_path,
                flow.description,
                flow.priority,
                flow.owner
            ))

            cursor.close()
        except Exception as e:
            raise Exception(f"Failed to insert flow {flow.flow_id}: {str(e)}")

    # ===== MIGRATION_RUNS METHODS =====

    def create_run(self, flow_id: str, job_run_id: Optional[str] = None) -> str:
        """
        Create a new migration run.

        Args:
            flow_id: Flow identifier
            job_run_id: Optional Databricks run ID

        Returns:
            run_id (format: "{flow_id}_run_{timestamp}")
        """
        timestamp = int(datetime.now().timestamp())
        run_id = f"{flow_id}_run_{timestamp}"

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(f"""
                INSERT INTO {self.runs_table}
                (run_id, flow_id, job_run_id, start_ts, status)
                VALUES (?, ?, ?, current_timestamp(), 'CREATING')
            """, (run_id, flow_id, job_run_id))

            cursor.close()
            return run_id
        except Exception as e:
            raise Exception(f"Failed to create run: {str(e)}")

    def get_run(self, run_id: str) -> Optional[MigrationRun]:
        """Get a migration run by run_id."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT * FROM {self.runs_table} WHERE run_id = ?",
                (run_id,)
            )

            row = cursor.fetchone()
            description = cursor.description
            cursor.close()

            if row:
                return MigrationRun.from_dict(self._row_to_dict(row, description))
            return None
        except Exception as e:
            raise Exception(f"Failed to get run {run_id}: {str(e)}")

    def update_run_status(
        self,
        run_id: str,
        status: str,
        job_run_id: Optional[str] = None,
        end_ts: Optional[datetime] = None
    ):
        """
        Update migration run status.

        Args:
            run_id: Run identifier
            status: New status (CREATING | QUEUED | RUNNING | SUCCESS | FAILED | CANCELLED)
            job_run_id: Optional Databricks run ID
            end_ts: Optional end timestamp (set when status is terminal)
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            if job_run_id and end_ts:
                cursor.execute(f"""
                    UPDATE {self.runs_table}
                    SET status = ?, job_run_id = ?, end_ts = ?
                    WHERE run_id = ?
                """, (status, job_run_id, end_ts, run_id))
            elif job_run_id:
                cursor.execute(f"""
                    UPDATE {self.runs_table}
                    SET status = ?, job_run_id = ?
                    WHERE run_id = ?
                """, (status, job_run_id, run_id))
            elif end_ts:
                cursor.execute(f"""
                    UPDATE {self.runs_table}
                    SET status = ?, end_ts = ?
                    WHERE run_id = ?
                """, (status, end_ts, run_id))
            else:
                cursor.execute(f"""
                    UPDATE {self.runs_table}
                    SET status = ?
                    WHERE run_id = ?
                """, (status, run_id))

            cursor.close()
        except Exception as e:
            raise Exception(f"Failed to update run {run_id}: {str(e)}")

    def get_flow_runs(self, flow_id: str, limit: int = 10) -> List[MigrationRun]:
        """
        Get migration runs for a flow.

        Args:
            flow_id: Flow identifier
            limit: Maximum number of runs to return

        Returns:
            List of runs, sorted by start_ts DESC
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT *
                FROM {self.runs_table}
                WHERE flow_id = ?
                ORDER BY start_ts DESC
                LIMIT ?
            """, (flow_id, limit))

            rows = cursor.fetchall()
            description = cursor.description
            cursor.close()

            return [MigrationRun.from_dict(self._row_to_dict(row, description))
                   for row in rows]
        except Exception as e:
            raise Exception(f"Failed to get runs for flow {flow_id}: {str(e)}")

    # ===== MIGRATION_ITERATIONS METHODS =====

    def add_iteration(
        self,
        run_id: str,
        iteration_num: int,
        step: str,
        step_status: str,
        summary: Optional[str] = None,
        error: Optional[str] = None
    ):
        """
        Add a new iteration step.

        Args:
            run_id: Run identifier
            iteration_num: Iteration number (sequential)
            step: Step name ("parse_xml", "generate_code", "validate")
            step_status: Status (RUNNING | COMPLETED | FAILED)
            summary: Optional human-readable summary
            error: Optional error message if FAILED
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(f"""
                INSERT INTO {self.iterations_table}
                (run_id, iteration_num, step, step_status, ts, summary, error)
                VALUES (?, ?, ?, ?, current_timestamp(), ?, ?)
            """, (run_id, iteration_num, step, step_status, summary, error))

            cursor.close()
        except Exception as e:
            raise Exception(f"Failed to add iteration: {str(e)}")

    def get_latest_iteration(self, run_id: str) -> Optional[MigrationIteration]:
        """Get the latest iteration for a run."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT *
                FROM {self.iterations_table}
                WHERE run_id = ?
                ORDER BY iteration_num DESC
                LIMIT 1
            """, (run_id,))

            row = cursor.fetchone()
            description = cursor.description
            cursor.close()

            if row:
                return MigrationIteration.from_dict(self._row_to_dict(row, description))
            return None
        except Exception as e:
            raise Exception(f"Failed to get latest iteration for run {run_id}: {str(e)}")

    def get_run_iterations(self, run_id: str) -> List[MigrationIteration]:
        """Get all iterations for a run, ordered by iteration_num ASC."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT *
                FROM {self.iterations_table}
                WHERE run_id = ?
                ORDER BY iteration_num ASC
            """, (run_id,))

            rows = cursor.fetchall()
            description = cursor.description
            cursor.close()

            return [MigrationIteration.from_dict(self._row_to_dict(row, description))
                   for row in rows]
        except Exception as e:
            raise Exception(f"Failed to get iterations for run {run_id}: {str(e)}")

    # ===== MIGRATION_PATCHES METHODS =====

    def add_patch(
        self,
        run_id: str,
        iteration_num: int,
        artifact_ref: str,
        patch_summary: Optional[str] = None,
        diff_ref: Optional[str] = None
    ):
        """Add a code patch record."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(f"""
                INSERT INTO {self.patches_table}
                (run_id, iteration_num, artifact_ref, patch_summary, diff_ref, ts)
                VALUES (?, ?, ?, ?, ?, current_timestamp())
            """, (run_id, iteration_num, artifact_ref, patch_summary, diff_ref))

            cursor.close()
        except Exception as e:
            raise Exception(f"Failed to add patch: {str(e)}")

    def get_run_patches(self, run_id: str) -> List[MigrationPatch]:
        """Get all patches for a run."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT *
                FROM {self.patches_table}
                WHERE run_id = ?
                ORDER BY iteration_num ASC, artifact_ref ASC
            """, (run_id,))

            rows = cursor.fetchall()
            description = cursor.description
            cursor.close()

            return [MigrationPatch.from_dict(self._row_to_dict(row, description))
                   for row in rows]
        except Exception as e:
            raise Exception(f"Failed to get patches for run {run_id}: {str(e)}")

    # ===== MIGRATION_SAST_RESULTS METHODS =====

    def add_sast_result(
        self,
        run_id: str,
        iteration_num: int,
        artifact_ref: str,
        scan_status: str,
        findings_json: Optional[str] = None
    ):
        """Add a SAST scan result."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(f"""
                INSERT INTO {self.sast_table}
                (run_id, iteration_num, artifact_ref, scan_status, findings_json, ts)
                VALUES (?, ?, ?, ?, ?, current_timestamp())
            """, (run_id, iteration_num, artifact_ref, scan_status, findings_json))

            cursor.close()
        except Exception as e:
            raise Exception(f"Failed to add SAST result: {str(e)}")

    def get_run_sast_results(self, run_id: str) -> List[MigrationSastResult]:
        """Get all SAST results for a run."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT *
                FROM {self.sast_table}
                WHERE run_id = ?
                ORDER BY iteration_num ASC, artifact_ref ASC
            """, (run_id,))

            rows = cursor.fetchall()
            description = cursor.description
            cursor.close()

            return [MigrationSastResult.from_dict(self._row_to_dict(row, description))
                   for row in rows]
        except Exception as e:
            raise Exception(f"Failed to get SAST results for run {run_id}: {str(e)}")

    # ===== MIGRATION_EXEC_RESULTS METHODS =====

    def add_exec_result(
        self,
        run_id: str,
        iteration_num: int,
        job_type: str,
        status: str,
        runtime_s: Optional[int] = None,
        error: Optional[str] = None,
        logs_ref: Optional[str] = None
    ):
        """Add a job execution result."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(f"""
                INSERT INTO {self.exec_table}
                (run_id, iteration_num, job_type, status, runtime_s, error, logs_ref, ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, current_timestamp())
            """, (run_id, iteration_num, job_type, status, runtime_s, error, logs_ref))

            cursor.close()
        except Exception as e:
            raise Exception(f"Failed to add exec result: {str(e)}")

    def get_run_exec_results(self, run_id: str) -> List[MigrationExecResult]:
        """Get all execution results for a run."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT *
                FROM {self.exec_table}
                WHERE run_id = ?
                ORDER BY iteration_num ASC, job_type ASC
            """, (run_id,))

            rows = cursor.fetchall()
            description = cursor.description
            cursor.close()

            return [MigrationExecResult.from_dict(self._row_to_dict(row, description))
                   for row in rows]
        except Exception as e:
            raise Exception(f"Failed to get exec results for run {run_id}: {str(e)}")

    # ===== MIGRATION_HUMAN_REQUESTS METHODS =====

    def create_human_request(
        self,
        run_id: str,
        iteration_num: int,
        reason: str,
        instructions: Optional[str] = None,
        status: str = "PENDING"
    ):
        """Create a human intervention request."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(f"""
                INSERT INTO {self.human_table}
                (run_id, iteration_num, reason, instructions, status, ts)
                VALUES (?, ?, ?, ?, ?, current_timestamp())
            """, (run_id, iteration_num, reason, instructions, status))

            cursor.close()
        except Exception as e:
            raise Exception(f"Failed to create human request: {str(e)}")

    def get_run_human_requests(self, run_id: str) -> List[MigrationHumanRequest]:
        """Get all human requests for a run."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT *
                FROM {self.human_table}
                WHERE run_id = ?
                ORDER BY iteration_num ASC
            """, (run_id,))

            rows = cursor.fetchall()
            description = cursor.description
            cursor.close()

            return [MigrationHumanRequest.from_dict(self._row_to_dict(row, description))
                   for row in rows]
        except Exception as e:
            raise Exception(f"Failed to get human requests for run {run_id}: {str(e)}")

    def update_human_request_status(self, run_id: str, iteration_num: int, status: str):
        """Update a human request status (PENDING | RESOLVED | IGNORED)."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(f"""
                UPDATE {self.human_table}
                SET status = ?
                WHERE run_id = ? AND iteration_num = ?
            """, (status, run_id, iteration_num))

            cursor.close()
        except Exception as e:
            raise Exception(f"Failed to update human request: {str(e)}")

    # ===== UTILITY METHODS =====

    def close(self):
        """Close connection."""
        if self._connection and self._connection.open:
            self._connection.close()
