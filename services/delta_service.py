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
