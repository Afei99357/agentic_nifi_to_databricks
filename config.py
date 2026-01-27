"""
Configuration settings for NiFi to Databricks migration app.
"""

import os

# Maximum number of agent iterations before requiring human intervention
MAX_ITERATIONS = 5

# Unity Catalog configuration
DEFAULT_CATALOG = "eliao"
DEFAULT_SCHEMA = "nifi_to_databricks"

# UC Volume paths
LOGS_VOLUME_PATH = f"/Volumes/{DEFAULT_CATALOG}/{DEFAULT_SCHEMA}/execution_logs"
NOTEBOOKS_VOLUME_PATH = f"/Volumes/{DEFAULT_CATALOG}/{DEFAULT_SCHEMA}/nifi_notebooks"

# Execution timeouts (in seconds)
EXECUTION_TIMEOUT_SECONDS = 3600  # 1 hour per iteration
AGENT_TIMEOUT_SECONDS = 7200  # 2 hours for agent job

# Databricks cluster configuration
EXISTING_CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "")

# Validate cluster ID is set
if not EXISTING_CLUSTER_ID:
    raise ValueError("DATABRICKS_CLUSTER_ID environment variable must be set")

# Databricks job configuration (legacy - kept for reference)
DEFAULT_SPARK_VERSION = "13.3.x-scala2.12"
DEFAULT_NODE_TYPE = "i3.xlarge"
DEFAULT_NUM_WORKERS = 2
