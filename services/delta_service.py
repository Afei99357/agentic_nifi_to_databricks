"""
Service for interacting with the nifi_flows Delta table.
Uses Databricks SQL Connector for containerized app environments.
"""

import os
from typing import List, Optional
from databricks import sql
from databricks.sdk.core import Config
from models.nifi_flow_record import NiFiFlowRecord


class DeltaService:
    """Interface to read/write the nifi_flows Delta table via SQL Warehouse."""

    def __init__(self):
        """Initialize with SQL Warehouse configuration using Config class."""
        import logging

        # Use Config class (official Databricks Apps pattern)
        # Config automatically loads warehouse_id from DATABRICKS_WAREHOUSE_ID env var
        self.cfg = Config()

        self.table_name = os.getenv("DELTA_TABLE_NAME", "eliao.nifi_to_databricks.nifi_flows")
        self.history_table_name = "eliao.nifi_to_databricks.nifi_conversion_history"

        # Connection will be created lazily
        self._connection = None

        # Log the warehouse ID that Config discovered
        warehouse_id = getattr(self.cfg, 'warehouse_id', 'NOT_SET')
        logging.info(f"DeltaService configured: table={self.table_name}, warehouse={warehouse_id}")

    def _get_connection(self):
        """Get or create SQL connection using official Databricks Apps pattern."""
        import logging
        from databricks.sdk import WorkspaceClient

        if self._connection is None or not self._connection.open:
            logging.info(f"Connecting to SQL Warehouse: host={self.cfg.host}, warehouse={self.cfg.warehouse_id}")

            try:
                # Get access token from WorkspaceClient
                ws = WorkspaceClient()
                # The auth header is in format "Bearer <token>"
                auth_header = ws.config.authenticate()

                # Extract token if it's a callable
                if callable(auth_header):
                    token_result = auth_header()
                    # Token result might be a dict with 'Authorization' key
                    if isinstance(token_result, dict) and 'Authorization' in token_result:
                        access_token = token_result['Authorization'].replace('Bearer ', '')
                    else:
                        access_token = str(token_result).replace('Bearer ', '')
                else:
                    access_token = str(auth_header).replace('Bearer ', '')

                logging.info(f"Got access token (length: {len(access_token)})")

                # Use access_token instead of credentials_provider
                self._connection = sql.connect(
                    server_hostname=self.cfg.host,
                    http_path=f"/sql/1.0/warehouses/{self.cfg.warehouse_id}",
                    access_token=access_token,
                    _socket_timeout=120,
                    _tls_no_verify=False
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

    def list_all_flows(self) -> List[NiFiFlowRecord]:
        """Get all flows from Delta table."""
        import logging
        import traceback
        try:
            logging.info(f"Connecting to SQL Warehouse to query {self.table_name}")
            conn = self._get_connection()
            logging.info("Connection established, creating cursor")
            cursor = conn.cursor()

            logging.info(f"Executing query: SELECT * FROM {self.table_name}")

            # Add query timeout to prevent hanging
            import time
            start_time = time.time()
            cursor.execute(f"SELECT * FROM {self.table_name}")
            query_time = time.time() - start_time
            logging.info(f"Query executed in {query_time:.2f} seconds")

            # Fetch all rows and convert to NiFiFlowRecord objects
            logging.info("Fetching rows...")
            rows = cursor.fetchall()
            fetch_time = time.time() - start_time
            logging.info(f"Rows fetched in {fetch_time:.2f} seconds total")
            description = cursor.description
            logging.info(f"Fetched {len(rows)} rows, closing cursor")
            cursor.close()

            logging.info(f"Converting rows to NiFiFlowRecord objects...")
            records = [NiFiFlowRecord.from_dict(self._row_to_dict(row, description))
                    for row in rows]

            logging.info(f"Successfully fetched {len(records)} flows from Delta table")
            return records
        except Exception as e:
            logging.error(f"Error in list_all_flows: {type(e).__name__}: {str(e)}")
            logging.error(f"Traceback: {traceback.format_exc()}")
            raise Exception(f"Failed to read flows from Delta table: {str(e)}")

    def get_flow(self, flow_name: str) -> Optional[NiFiFlowRecord]:
        """Get a single flow by name."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT * FROM {self.table_name} WHERE flow_name = ?",
                (flow_name,)
            )

            row = cursor.fetchone()
            description = cursor.description
            cursor.close()

            if row:
                return NiFiFlowRecord.from_dict(self._row_to_dict(row, description))
            return None
        except Exception as e:
            raise Exception(f"Failed to get flow {flow_name}: {str(e)}")

    def update_flow_status(self, flow_name: str, updates: dict):
        """
        Update flow fields (status, run_id, progress, etc.).

        Args:
            flow_name: Flow identifier
            updates: Dictionary of fields to update
        """
        try:
            # Build SET clause and values
            set_clauses = []
            values = []

            for key, value in updates.items():
                if value is None:
                    set_clauses.append(f"{key} = NULL")
                else:
                    set_clauses.append(f"{key} = ?")
                    values.append(value)

            # Add last_updated
            set_clauses.append("last_updated = current_timestamp()")

            set_clause = ", ".join(set_clauses)
            values.append(flow_name)

            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                f"UPDATE {self.table_name} SET {set_clause} WHERE flow_name = ?",
                values
            )
            cursor.close()
        except Exception as e:
            raise Exception(f"Failed to update flow {flow_name}: {str(e)}")

    def bulk_update_status(self, flow_names: List[str], status: str):
        """Update status for multiple flows."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Build parameterized query with placeholders
            placeholders = ", ".join(["?"] * len(flow_names))
            sql_query = f"""
                UPDATE {self.table_name}
                SET status = ?, last_updated = current_timestamp()
                WHERE flow_name IN ({placeholders})
            """

            # Parameters: status first, then all flow names
            params = [status] + flow_names
            cursor.execute(sql_query, params)
            cursor.close()
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
            conn = self._get_connection()
            cursor = conn.cursor()

            # Build WHERE clause
            where_clauses = []
            params = []

            if query:
                where_clauses.append("flow_name LIKE ?")
                params.append(f"%{query}%")
            if status:
                where_clauses.append("status = ?")
                params.append(status)
            if server:
                where_clauses.append("server = ?")
                params.append(server)

            where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
            sql_query = f"SELECT * FROM {self.table_name} WHERE {where_clause}"

            cursor.execute(sql_query, params)
            rows = cursor.fetchall()
            description = cursor.description
            cursor.close()

            return [NiFiFlowRecord.from_dict(self._row_to_dict(row, description))
                    for row in rows]
        except Exception as e:
            raise Exception(f"Failed to search flows: {str(e)}")

    # ===== HISTORY TRACKING METHODS =====

    def create_conversion_attempt(
        self,
        flow_name: str,
        job_id: str,
        job_name: str
    ) -> str:
        """
        Create a new conversion attempt record in history table.

        Args:
            flow_name: Flow identifier
            job_id: Databricks job ID (can be "PENDING" initially)
            job_name: Name of the Databricks job

        Returns:
            attempt_id (string)
        """
        from datetime import datetime

        timestamp = int(datetime.now().timestamp())
        attempt_id = f"{flow_name}_attempt_{timestamp}"

        # Get current attempt count
        flow = self.get_flow(flow_name)
        if not flow:
            raise Exception(f"Flow {flow_name} not found")

        attempt_number = (flow.total_attempts or 0) + 1

        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Insert into history table
            cursor.execute(f"""
                INSERT INTO {self.history_table_name}
                (attempt_id, flow_name, databricks_job_id, job_name, status,
                 started_at, attempt_number, triggered_by)
                VALUES (?, ?, ?, ?, 'CREATING', current_timestamp(), ?, 'user')
            """, (attempt_id, flow_name, job_id, job_name, attempt_number))

            # Update flows table
            cursor.execute(f"""
                UPDATE {self.table_name}
                SET
                    current_attempt_id = ?,
                    total_attempts = ?,
                    last_attempt_at = current_timestamp(),
                    first_attempt_at = COALESCE(first_attempt_at, current_timestamp()),
                    status = 'CONVERTING',
                    last_updated = current_timestamp()
                WHERE flow_name = ?
            """, (attempt_id, attempt_number, flow_name))

            cursor.close()
            return attempt_id
        except Exception as e:
            cursor.close()
            raise Exception(f"Failed to create attempt: {str(e)}")

    def update_attempt_status(self, attempt_id: str, updates: dict):
        """
        Update a specific conversion attempt in history table.

        Args:
            attempt_id: Attempt identifier
            updates: Dictionary of fields to update
        """
        try:
            set_clauses = []
            values = []

            for key, value in updates.items():
                if value is None:
                    set_clauses.append(f"{key} = NULL")
                elif isinstance(value, list):
                    # Handle arrays - convert to SQL array literal
                    array_str = ", ".join([f"'{item}'" for item in value])
                    set_clauses.append(f"{key} = ARRAY({array_str})")
                else:
                    set_clauses.append(f"{key} = ?")
                    values.append(value)

            set_clause = ", ".join(set_clauses)
            values.append(attempt_id)

            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                f"UPDATE {self.history_table_name} "
                f"SET {set_clause} WHERE attempt_id = ?",
                values
            )
            cursor.close()
        except Exception as e:
            raise Exception(f"Failed to update attempt {attempt_id}: {str(e)}")

    def get_flow_history(self, flow_name: str, limit: int = 10) -> list:
        """
        Get conversion history for a flow.

        Args:
            flow_name: Flow identifier
            limit: Maximum number of attempts to return

        Returns:
            List of attempt dictionaries, sorted by started_at DESC
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT *
                FROM {self.history_table_name}
                WHERE flow_name = ?
                ORDER BY started_at DESC
                LIMIT ?
            """, (flow_name, limit))

            rows = cursor.fetchall()
            description = cursor.description
            cursor.close()

            return [self._row_to_dict(row, description) for row in rows]
        except Exception as e:
            raise Exception(f"Failed to get history for flow {flow_name}: {str(e)}")

    def get_current_attempt(self, flow_name: str) -> Optional[dict]:
        """
        Get the current attempt for a flow.

        Args:
            flow_name: Flow identifier

        Returns:
            Attempt dictionary or None if no current attempt
        """
        try:
            flow = self.get_flow(flow_name)
            if not flow or not flow.current_attempt_id:
                return None

            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT *
                FROM {self.history_table_name}
                WHERE attempt_id = ?
            """, (flow.current_attempt_id,))

            row = cursor.fetchone()
            description = cursor.description
            cursor.close()

            return self._row_to_dict(row, description) if row else None
        except Exception as e:
            raise Exception(f"Failed to get current attempt for flow {flow_name}: {str(e)}")

    def close(self):
        """Close connection."""
        if self._connection and self._connection.open:
            self._connection.close()
