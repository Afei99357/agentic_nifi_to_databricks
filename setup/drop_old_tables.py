# Databricks notebook source
"""
Drop Old Tables (nifi_flows, nifi_conversion_history)

Run this ONLY if you want to start fresh without migrating old data.

WARNING: This will permanently delete:
- nifi_flows table and all flow metadata
- nifi_conversion_history table and all conversion history

Make sure you have backups if needed!
"""

# COMMAND ----------

CATALOG = "eliao"
SCHEMA = "nifi_to_databricks"

spark  # type: ignore

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚠️ Warning: This will delete data permanently!
# MAGIC
# MAGIC Uncomment the DROP statements below to proceed.

# COMMAND ----------

# Uncomment these lines to drop old tables:

# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.nifi_flows")
# print("✅ Dropped: nifi_flows")

# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.nifi_conversion_history")
# print("✅ Dropped: nifi_conversion_history")

print("⚠️ Tables NOT dropped - uncomment the lines above to proceed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Old Tables Are Gone

# COMMAND ----------

try:
    spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.nifi_flows").collect()
    print("❌ nifi_flows still exists")
except:
    print("✅ nifi_flows dropped")

try:
    spark.sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.nifi_conversion_history").collect()
    print("❌ nifi_conversion_history still exists")
except:
    print("✅ nifi_conversion_history dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Step
# MAGIC
# MAGIC Now run `setup/setup_complete.py` to create the new migration_* tables.
