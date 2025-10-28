# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Infrastructure Data
# MAGIC
# MAGIC Collects infrastructure data from Overpass API and saves to Delta Lake

# COMMAND ----------
# MAGIC %pip install pyarrow geopandas pyproj shapely scipy requests

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# Import configuration
import sys
import os
import getpass

# Auto-detect repository path (handles both nuar_mini_project and nuar-mini-project)
username = getpass.getuser()
possible_paths = [
    f'/Repos/{username}/nuar_mini_project',
    f'/Repos/{username}/nuar-mini-project',
    f'/Workspace/Repos/{username}/nuar_mini_project',
    f'/Workspace/Repos/{username}/nuar-mini-project',
    '/Repos/mnbabdullah765@yahoo.com/nuar_mini_project',
    '/Repos/mnbabdullah765@yahoo.com/nuar-mini-project',
    '/Workspace/Repos/mnbabdullah765@yahoo.com/nuar_mini_project',
    '/Workspace/Repos/mnbabdullah765@yahoo.com/nuar-mini-project',
]

repo_path = None
for path in possible_paths:
    if os.path.exists(path):
        repo_path = path
        break

if not repo_path:
    print("❌ Repository not found. Tried these paths:")
    for path in possible_paths:
        print(f"   {path}")
    print(f"\nCurrent directory: {os.getcwd()}")
    raise FileNotFoundError("Could not find repository. Check that Repos are synced from GitHub.")

print(f"✅ Repository found at: {repo_path}")
sys.path.append(repo_path)

from config.databricks_settings import *
from src.utils.api_client import OverpassClient
from src.bronze.ingestion import flatten_overpass_elements

# COMMAND ----------
print_config_summary()

# COMMAND ----------
# Query Overpass API
print("🔍 Querying Overpass API for Stockport infrastructure...")
client = OverpassClient(timeout=OVERPASS_TIMEOUT)
result = client.query_infrastructure(STOCKPORT_BBOX, STOCKPORT_INFRASTRUCTURE_TYPES)

# COMMAND ----------
# Process and save to Delta
if result and 'elements' in result:
    import pandas as pd
    from datetime import datetime

    elements = result['elements']
    print(f"📊 Retrieved {len(elements)} infrastructure elements")

    # Flatten data
    flattened = flatten_overpass_elements(elements)
    df_pd = pd.DataFrame(flattened)

    # Add metadata
    df_pd['ingestion_timestamp'] = datetime.now()
    df_pd['ingestion_date'] = datetime.now().date()
    df_pd['area'] = 'stockport'

    # Convert to Spark DataFrame
    df_spark = spark.createDataFrame(df_pd)

    # Write to Delta table
    df_spark.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("ingestion_date", "infrastructure_type") \
        .option("overwriteSchema", "true") \
        .saveAsTable(BRONZE_TABLES['infrastructure'])

    print(f"✅ Saved {len(df_pd)} records to {BRONZE_TABLES['infrastructure']}")
else:
    print("❌ No data retrieved from API")

# COMMAND ----------
# Verify data
display(spark.table(BRONZE_TABLES['infrastructure']).limit(10))

# COMMAND ----------
# Show summary statistics
spark.sql(f"""
    SELECT
        infrastructure_type,
        COUNT(*) as count,
        COUNT(DISTINCT id) as unique_elements,
        ingestion_date
    FROM {BRONZE_TABLES['infrastructure']}
    GROUP BY infrastructure_type, ingestion_date
    ORDER BY ingestion_date DESC, count DESC
""").display()
