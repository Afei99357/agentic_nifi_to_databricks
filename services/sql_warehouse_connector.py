"""
Standalone SQL Warehouse connector utility for Databricks Apps.

This module provides a reusable way to connect to Databricks SQL Warehouses
with proper error handling and logging.
"""

import os
import logging
from typing import Optional, Any
from databricks import sql
from databricks.sdk.core import Config


class SQLWarehouseConnector:
    """Handles SQL Warehouse connections for Databricks Apps."""

    def __init__(self, warehouse_id: Optional[str] = None, logger: Optional[logging.Logger] = None):
        """
        Initialize the SQL Warehouse connector.

        Args:
            warehouse_id: Optional warehouse ID. If not provided, uses DATABRICKS_WAREHOUSE_ID env var
            logger: Optional logger instance. If not provided, creates a new one
        """
        self.cfg = Config()

        # Set warehouse_id from parameter or config
        if warehouse_id:
            self.cfg.warehouse_id = warehouse_id
        elif not hasattr(self.cfg, 'warehouse_id') or not self.cfg.warehouse_id:
            # Try to get from environment
            env_warehouse = os.getenv('DATABRICKS_WAREHOUSE_ID')
            if env_warehouse:
                self.cfg.warehouse_id = env_warehouse

        if not self.cfg.warehouse_id:
            raise ValueError(
                "warehouse_id must be provided or set via DATABRICKS_WAREHOUSE_ID environment variable"
            )

        self.logger = logger or logging.getLogger(__name__)

    def get_connection(self) -> Any:
        """
        Create and return a SQL Warehouse connection.

        Returns:
            A databricks.sql connection object

        Raises:
            Exception: If connection fails
        """
        http_path = f"/sql/1.0/warehouses/{self.cfg.warehouse_id}"

        self.logger.info(f"Connecting to SQL Warehouse")
        self.logger.debug(f"  Host: {self.cfg.host}")
        self.logger.debug(f"  Warehouse ID: {self.cfg.warehouse_id}")
        self.logger.debug(f"  HTTP Path: {http_path}")

        try:
            # Exact pattern from Dash template (notebooks/dataapptmp/app.py)
            # Note: Use cfg.host directly, do NOT strip protocol
            conn = sql.connect(
                server_hostname=self.cfg.host,
                http_path=http_path,
                credentials_provider=lambda: self.cfg.authenticate
            )

            self.logger.info("SQL Warehouse connection established successfully")
            return conn

        except Exception as e:
            self.logger.error(f"Failed to connect to SQL Warehouse: {e}")
            self.logger.error(f"  Server: {self.cfg.host}")
            self.logger.error(f"  Warehouse: {self.cfg.warehouse_id}")
            raise

    def test_connection(self) -> bool:
        """
        Test the SQL Warehouse connection with a simple query.

        Returns:
            True if connection and query succeed, False otherwise
        """
        try:
            self.logger.info("Testing SQL Warehouse connection...")
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 as test")
                    result = cursor.fetchone()
                    self.logger.info(f"Connection test successful: {result}")
                    return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False

    def execute_query(self, query: str, fetch_all: bool = True) -> Any:
        """
        Execute a SQL query and return results.

        Args:
            query: SQL query string to execute
            fetch_all: If True, returns all rows. If False, returns cursor

        Returns:
            Query results (list of rows if fetch_all=True, cursor otherwise)

        Raises:
            Exception: If query execution fails
        """
        self.logger.debug(f"Executing query: {query[:100]}...")

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)

                    if fetch_all:
                        results = cursor.fetchall()
                        self.logger.debug(f"Query returned {len(results)} rows")
                        return results
                    else:
                        return cursor

        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            self.logger.error(f"  Query: {query[:200]}")
            raise

    def get_table_data(self, catalog: str, schema: str, table: str, limit: Optional[int] = None) -> list:
        """
        Fetch data from a specific table.

        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            limit: Optional row limit

        Returns:
            List of rows from the table
        """
        query = f"SELECT * FROM {catalog}.{schema}.{table}"
        if limit:
            query += f" LIMIT {limit}"

        self.logger.info(f"Fetching data from {catalog}.{schema}.{table}")
        return self.execute_query(query)
