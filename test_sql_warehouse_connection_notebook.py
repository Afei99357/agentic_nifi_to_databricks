# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Warehouse Connection Diagnostic Test
# MAGIC
# MAGIC This notebook tests SQL Warehouse connectivity with detailed diagnostics.
# MAGIC
# MAGIC **Test Sections:**
# MAGIC 1. Environment Check - Verify env vars and Config setup
# MAGIC 2. Config Class Inspection - Examine Config object and authenticate method
# MAGIC 3. Method 1 (Dash Template - RECOMMENDED) - Exact pattern from dataapptmp/app.py
# MAGIC 4. Method 2 (Stripped Protocol) - Alternative approach
# MAGIC 5. Method 3 (access_token) - For user-specific queries
# MAGIC 6. Real Table Query - Test actual table from application
# MAGIC 7. Diagnostics Summary - Review results and recommendations
# MAGIC
# MAGIC **Instructions:**
# MAGIC 1. Attach to a cluster
# MAGIC 2. Run all cells to diagnose connection issues

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Required Packages

# COMMAND ----------

# Install required packages (they will be available immediately after installation)
%pip install databricks-sql-connector databricks-sdk --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

import os
import logging
from databricks import sql
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print("="*80)
print("SQL WAREHOUSE CONNECTION DIAGNOSTIC TEST")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Environment Check
# MAGIC Verify that all required environment variables are set

# COMMAND ----------

print("="*80)
print("SECTION 1: Environment Check")
print("="*80)

# Check environment variables (for Databricks Apps)
env_vars = {
    'DATABRICKS_HOST': os.getenv('DATABRICKS_HOST'),
    'DATABRICKS_WAREHOUSE_ID': os.getenv('DATABRICKS_WAREHOUSE_ID'),
    'DATABRICKS_CLIENT_ID': os.getenv('DATABRICKS_CLIENT_ID'),
    'DATABRICKS_CLIENT_SECRET': '***' if os.getenv('DATABRICKS_CLIENT_SECRET') else None,
    'DATABRICKS_TOKEN': '***' if os.getenv('DATABRICKS_TOKEN') else None,
}

print("\nEnvironment Variables (Databricks Apps):")
for key, value in env_vars.items():
    status = "✓" if value else "✗"
    print(f"  {status} {key}: {value}")

# Get warehouse_id from environment or widget
warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID')

# For regular notebooks, get host from workspace context
try:
    workspace_host = spark.conf.get("spark.databricks.workspaceUrl")
    print(f"\n✓ Workspace Host (from Spark): {workspace_host}")
except:
    workspace_host = None
    print(f"\n✗ Could not get workspace host from Spark context")

# Set warehouse_id if not in environment (for notebook testing)
if not warehouse_id:
    print("\n⚠ DATABRICKS_WAREHOUSE_ID not set!")
    print("  For Databricks Apps: Set in app.yaml")
    print("  For notebook testing: Set warehouse_id below")

    # IMPORTANT: Set your warehouse ID here for notebook testing
    warehouse_id = "6197de40f3098d2b"  # TODO: Replace with your warehouse ID

    if warehouse_id:
        os.environ['DATABRICKS_WAREHOUSE_ID'] = warehouse_id
        print(f"\n✓ Using warehouse_id: {warehouse_id}")
    else:
        print("\n✗ No warehouse_id configured!")
else:
    print(f"\n✓ Using warehouse_id from environment: {warehouse_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Test Config Class
# MAGIC Examine the Config object and authentication method

# COMMAND ----------

print("="*80)
print("SECTION 2: Test Config Class")
print("="*80)

try:
    cfg = Config()
    print("\n✓ Config object created successfully")
    print(f"\nConfig attributes:")
    print(f"  host: {cfg.host}")
    print(f"  warehouse_id: {cfg.warehouse_id if hasattr(cfg, 'warehouse_id') else 'NOT SET'}")

    # Check if warehouse_id needs to be set manually
    if not hasattr(cfg, 'warehouse_id') or not cfg.warehouse_id:
        if warehouse_id:
            cfg.warehouse_id = warehouse_id
            print(f"  warehouse_id (manually set): {cfg.warehouse_id}")

    print(f"\nAuthentication method:")
    print(f"  cfg.authenticate type: {type(cfg.authenticate)}")

    # Try to understand what authenticate returns
    try:
        auth_result = cfg.authenticate()
        print(f"  cfg.authenticate() returns: {type(auth_result)}")
        if hasattr(auth_result, '__dict__'):
            print(f"  Attributes: {list(auth_result.__dict__.keys())}")
    except Exception as e:
        print(f"  ⚠ Could not call cfg.authenticate(): {e}")

except Exception as e:
    print(f"\n✗ Failed to create Config: {e}")
    import traceback
    traceback.print_exc()
    cfg = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Method 1 - Exact Dash Template (RECOMMENDED)
# MAGIC Uses `cfg.host` directly without stripping protocol

# COMMAND ----------

print("="*80)
print("SECTION 3: Test Connection - Method 1 (Exact Dash Template - RECOMMENDED)")
print("="*80)

if cfg:
    try:
        http_path = f"/sql/1.0/warehouses/{cfg.warehouse_id}"

        print(f"\nConnection parameters (from notebooks/dataapptmp/app.py):")
        print(f"  server_hostname: {cfg.host} (using cfg.host directly)")
        print(f"  http_path: {http_path}")
        print(f"  credentials_provider: lambda: cfg.authenticate")

        print("\nAttempting connection...")
        # This is the EXACT pattern from the Dash template
        conn = sql.connect(
            server_hostname=cfg.host,
            http_path=http_path,
            credentials_provider=lambda: cfg.authenticate
        )

        print("✓ Connection object created")
        print(f"  Connection: {conn}")
        print(f"  Connection open: {conn.open}")

        # Try to execute a simple query
        print("\nTesting query execution...")
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchall()
            print(f"✓ Query executed successfully")
            print(f"  Result: {result}")

        conn.close()
        print("✓ Method 1 (Dash Template) SUCCESSFUL")

    except Exception as e:
        print(f"\n✗ Method 1 FAILED: {e}")
        import traceback
        traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Method 2 - Stripped Protocol (Alternative)
# MAGIC Strips `https://` from hostname before connecting

# COMMAND ----------

print("="*80)
print("SECTION 4: Test Connection - Method 2 (Stripped Protocol - Alternative)")
print("="*80)

if cfg:
    try:
        # Strip protocol from hostname (alternative approach)
        server_hostname = cfg.host.replace("https://", "").replace("http://", "")
        http_path = f"/sql/1.0/warehouses/{cfg.warehouse_id}"

        print(f"\nConnection parameters:")
        print(f"  server_hostname: {server_hostname} (protocol stripped)")
        print(f"  http_path: {http_path}")
        print(f"  credentials_provider: lambda: cfg.authenticate")

        print("\nAttempting connection...")
        conn = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            credentials_provider=lambda: cfg.authenticate
        )

        print("✓ Connection object created")

        # Try to execute a simple query
        print("\nTesting query execution...")
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchall()
            print(f"✓ Query executed successfully")
            print(f"  Result: {result}")

        conn.close()
        print("✓ Method 2 (Stripped Protocol) SUCCESSFUL")

    except Exception as e:
        print(f"\n✗ Method 2 FAILED: {e}")
        import traceback
        traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Method 3 - Access Token
# MAGIC Extracts access token and uses it directly (for user-specific queries)

# COMMAND ----------

print("="*80)
print("SECTION 5: Test Connection - Method 3 (access_token)")
print("="*80)

if cfg:
    try:
        # Get token from WorkspaceClient
        print("\nGetting access token from WorkspaceClient...")
        w = WorkspaceClient()

        # Get the token using the authenticate method
        auth_result = cfg.authenticate()
        print(f"  Authentication result type: {type(auth_result)}")

        # Try to extract token
        if hasattr(auth_result, 'access_token'):
            access_token = auth_result.access_token
        elif hasattr(auth_result, 'token'):
            access_token = auth_result.token
        elif isinstance(auth_result, dict) and 'access_token' in auth_result:
            access_token = auth_result['access_token']
        else:
            access_token = str(auth_result)

        print(f"✓ Token extracted (length: {len(access_token)})")

        server_hostname = cfg.host.replace("https://", "").replace("http://", "")
        http_path = f"/sql/1.0/warehouses/{cfg.warehouse_id}"

        print(f"\nConnection parameters:")
        print(f"  server_hostname: {server_hostname}")
        print(f"  http_path: {http_path}")
        print(f"  access_token: {'*' * 20}")

        print("\nAttempting connection...")
        conn = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )

        print("✓ Connection object created")

        # Try to execute a simple query
        print("\nTesting query execution...")
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 as test")
            result = cursor.fetchall()
            print(f"✓ Query executed successfully")
            print(f"  Result: {result}")

        conn.close()
        print("✓ Method 3 (access_token) SUCCESSFUL")

    except Exception as e:
        print(f"\n✗ Method 3 FAILED: {e}")
        import traceback
        traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Test Query Execution (Real Table)
# MAGIC Query the actual application table to verify end-to-end connectivity

# COMMAND ----------

print("="*80)
print("SECTION 6: Test Query Execution (Real Table)")
print("="*80)

if cfg:
    try:
        # Use Dash template pattern (Method 1)
        print("\nConnecting to test real table query (using Dash template pattern)...")
        conn = sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
            credentials_provider=lambda: cfg.authenticate
        )

        # Test the actual table from the application
        test_query = "SELECT * FROM eliao.nifi_to_databricks.nifi_flows LIMIT 5"

        print(f"\nExecuting query:")
        print(f"  {test_query}")

        import time
        start_time = time.time()

        with conn.cursor() as cursor:
            cursor.execute(test_query)
            results = cursor.fetchall()

        elapsed = time.time() - start_time

        print(f"\n✓ Query executed successfully")
        print(f"  Rows returned: {len(results)}")
        print(f"  Execution time: {elapsed:.2f}s")

        if results:
            print(f"\nFirst row:")
            print(f"  {results[0]}")

        conn.close()

    except Exception as e:
        print(f"\n✗ Real table query FAILED: {e}")
        import traceback
        traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Diagnostics Summary
# MAGIC Review results and next steps

# COMMAND ----------

print("="*80)
print("SECTION 7: Diagnostics Summary")
print("="*80)

print("\nTest Results:")
print("  - Check the output above to see which methods succeeded")
print("  - Method 1 (Dash Template) is RECOMMENDED - uses cfg.host directly")
print("  - Method 2 (Stripped Protocol) is an alternative if Method 1 fails")
print("  - Method 3 (access_token) is for user-specific queries (see dataapptmp/app.py)")
print("  - If all methods failed: check permissions, warehouse status, or network")

print("\nRecommended Next Steps:")
print("  1. Review which connection method succeeded above")
print("  2. delta_service.py has been updated to use Method 1 (Dash Template)")
print("  3. Verify SQL Warehouse is running and accessible")
print("  4. Check that the app has proper permissions to the warehouse")

print("\n" + "="*80)
print("END OF DIAGNOSTIC TEST")
print("="*80)
