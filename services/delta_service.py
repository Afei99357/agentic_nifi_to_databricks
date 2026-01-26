"""
Service for interacting with the nifi_flows Delta table.
"""

import os
from typing import List, Optional
from models.nifi_flow_record import NiFiFlowRecord


class DeltaService:
    """Interface to read/write the nifi_flows Delta table."""

    def __init__(self, spark=None):
        """
        Initialize Delta service.

        Args:
            spark: SparkSession instance. If None, will be created.
        """
        if spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.builder.getOrCreate()
        else:
            self.spark = spark

        self.table_name = os.getenv("DELTA_TABLE_NAME", "main.default.nifi_flows")

    def list_all_flows(self) -> List[NiFiFlowRecord]:
        """Get all flows from Delta table."""
        try:
            df = self.spark.read.table(self.table_name)
            rows = df.collect()
            return [NiFiFlowRecord.from_row(row) for row in rows]
        except Exception as e:
            raise Exception(f"Failed to read flows from Delta table: {str(e)}")

    def get_flow(self, flow_id: str) -> Optional[NiFiFlowRecord]:
        """Get a single flow by ID."""
        try:
            df = self.spark.read.table(self.table_name)
            rows = df.filter(df.flow_id == flow_id).collect()
            return NiFiFlowRecord.from_row(rows[0]) if rows else None
        except Exception as e:
            raise Exception(f"Failed to get flow {flow_id}: {str(e)}")

    def update_flow_status(self, flow_id: str, updates: dict):
        """
        Update flow fields (status, run_id, progress, etc.).

        Args:
            flow_id: Flow identifier
            updates: Dictionary of fields to update
        """
        try:
            # Build SET clause
            set_clauses = []
            for key, value in updates.items():
                if value is None:
                    set_clauses.append(f"{key} = NULL")
                elif isinstance(value, str):
                    # Escape single quotes in strings
                    escaped_value = value.replace("'", "''")
                    set_clauses.append(f"{key} = '{escaped_value}'")
                elif isinstance(value, (int, float)):
                    set_clauses.append(f"{key} = {value}")
                elif isinstance(value, list):
                    # Handle arrays
                    array_str = ", ".join([f"'{item}'" for item in value])
                    set_clauses.append(f"{key} = ARRAY({array_str})")
                else:
                    # Default string conversion
                    set_clauses.append(f"{key} = '{str(value)}'")

            set_clause = ", ".join(set_clauses)

            # Execute update
            sql = f"""
                UPDATE {self.table_name}
                SET {set_clause}, last_updated = current_timestamp()
                WHERE flow_id = '{flow_id}'
            """
            self.spark.sql(sql)

        except Exception as e:
            raise Exception(f"Failed to update flow {flow_id}: {str(e)}")

    def bulk_update_status(self, flow_ids: List[str], status: str):
        """Update status for multiple flows."""
        try:
            ids_str = "', '".join(flow_ids)
            sql = f"""
                UPDATE {self.table_name}
                SET status = '{status}', last_updated = current_timestamp()
                WHERE flow_id IN ('{ids_str}')
            """
            self.spark.sql(sql)
        except Exception as e:
            raise Exception(f"Failed to bulk update flows: {str(e)}")

    def search_flows(self, query: str = None, status: str = None, server: str = None) -> List[NiFiFlowRecord]:
        """
        Search flows with filters.

        Args:
            query: Search term for flow name
            status: Filter by status
            server: Filter by server

        Returns:
            List of matching flows
        """
        try:
            df = self.spark.read.table(self.table_name)

            if query:
                df = df.filter(df.flow_name.contains(query))
            if status:
                df = df.filter(df.status == status)
            if server:
                df = df.filter(df.server == server)

            rows = df.collect()
            return [NiFiFlowRecord.from_row(row) for row in rows]
        except Exception as e:
            raise Exception(f"Failed to search flows: {str(e)}")

    # ===== HISTORY TRACKING METHODS =====

    def create_conversion_attempt(
        self,
        flow_id: str,
        job_id: str,
        job_name: str
    ) -> str:
        """
        Create a new conversion attempt record in history table.

        Args:
            flow_id: Flow identifier
            job_id: Databricks job ID (can be "PENDING" initially)
            job_name: Name of the Databricks job

        Returns:
            attempt_id (string)
        """
        from datetime import datetime

        timestamp = int(datetime.now().timestamp())
        attempt_id = f"{flow_id}_attempt_{timestamp}"

        # Get current attempt count
        flow = self.get_flow(flow_id)
        if not flow:
            raise Exception(f"Flow {flow_id} not found")

        attempt_number = (flow.total_attempts or 0) + 1

        # Insert into history table
        self.spark.sql(f"""
            INSERT INTO main.default.nifi_conversion_history
            (attempt_id, flow_id, databricks_job_id, job_name, status,
             started_at, attempt_number, triggered_by)
            VALUES (
                '{attempt_id}',
                '{flow_id}',
                '{job_id}',
                '{job_name}',
                'CREATING',
                current_timestamp(),
                {attempt_number},
                'user'
            )
        """)

        # Update flows table
        self.spark.sql(f"""
            UPDATE {self.table_name}
            SET
                current_attempt_id = '{attempt_id}',
                total_attempts = {attempt_number},
                last_attempt_at = current_timestamp(),
                first_attempt_at = COALESCE(first_attempt_at, current_timestamp()),
                status = 'CONVERTING',
                last_updated = current_timestamp()
            WHERE flow_id = '{flow_id}'
        """)

        return attempt_id

    def update_attempt_status(self, attempt_id: str, updates: dict):
        """
        Update a specific conversion attempt in history table.

        Args:
            attempt_id: Attempt identifier
            updates: Dictionary of fields to update
        """
        try:
            # Build SET clause
            set_clauses = []
            for key, value in updates.items():
                if value is None:
                    set_clauses.append(f"{key} = NULL")
                elif isinstance(value, str):
                    # Escape single quotes in strings
                    escaped_value = value.replace("'", "''")
                    set_clauses.append(f"{key} = '{escaped_value}'")
                elif isinstance(value, (int, float)):
                    set_clauses.append(f"{key} = {value}")
                elif isinstance(value, list):
                    # Handle arrays
                    array_str = ", ".join([f"'{item}'" for item in value])
                    set_clauses.append(f"{key} = ARRAY({array_str})")
                else:
                    # Default string conversion
                    set_clauses.append(f"{key} = '{str(value)}'")

            set_clause = ", ".join(set_clauses)

            # Execute update on history table
            sql = f"""
                UPDATE main.default.nifi_conversion_history
                SET {set_clause}
                WHERE attempt_id = '{attempt_id}'
            """
            self.spark.sql(sql)

        except Exception as e:
            raise Exception(f"Failed to update attempt {attempt_id}: {str(e)}")

    def get_flow_history(self, flow_id: str, limit: int = 10) -> list:
        """
        Get conversion history for a flow.

        Args:
            flow_id: Flow identifier
            limit: Maximum number of attempts to return

        Returns:
            List of attempt dictionaries, sorted by started_at DESC
        """
        try:
            df = self.spark.sql(f"""
                SELECT *
                FROM main.default.nifi_conversion_history
                WHERE flow_id = '{flow_id}'
                ORDER BY started_at DESC
                LIMIT {limit}
            """)
            return [row.asDict() for row in df.collect()]
        except Exception as e:
            raise Exception(f"Failed to get history for flow {flow_id}: {str(e)}")

    def get_current_attempt(self, flow_id: str) -> Optional[dict]:
        """
        Get the current attempt for a flow.

        Args:
            flow_id: Flow identifier

        Returns:
            Attempt dictionary or None if no current attempt
        """
        try:
            flow = self.get_flow(flow_id)
            if not flow or not flow.current_attempt_id:
                return None

            df = self.spark.sql(f"""
                SELECT *
                FROM main.default.nifi_conversion_history
                WHERE attempt_id = '{flow.current_attempt_id}'
            """)
            rows = df.collect()
            return rows[0].asDict() if rows else None
        except Exception as e:
            raise Exception(f"Failed to get current attempt for flow {flow_id}: {str(e)}")
