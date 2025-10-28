# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Weather Transformation
# MAGIC
# MAGIC **Converted from**: `notebooks/07_silver_weather.py`
# MAGIC
# MAGIC Transforms weather data and adds quality checks

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup: Install Required Libraries

# COMMAND ----------
# Install required packages (only needed once per cluster, or use cluster libraries)
%pip install pyarrow geopandas pyproj shapely scipy python-dotenv requests

# COMMAND ----------
# Restart Python kernel to load new packages
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Import Configuration and Utilities

# COMMAND ----------
import sys
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
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

from src.silver.transformations import (
    transform_coordinates_to_bng,
    parse_date_fields,
    add_audit_columns,
    enforce_schema
)

print("="*80)
print("🥈 SILVER LAYER: Weather Transformation")
print("="*80)
print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
# ============================================================================

# Paths
if IS_DATABRICKS:
    # Use DBFS paths in Databricks
    BRONZE_DIR = f"{BRONZE_BASE_PATH}/stockport/weather"
    SILVER_DIR = f"{SILVER_BASE_PATH}/stockport/weather"

    # Create Silver directory
    dbutils.fs.mkdirs(SILVER_DIR)

    # Convert to local paths for Python operations
    BRONZE_DIR = BRONZE_DIR.replace('dbfs:', '/dbfs')
    SILVER_DIR = SILVER_DIR.replace('dbfs:', '/dbfs')
else:
    # Use local paths for development
    from pathlib import Path
    BRONZE_DIR = Path(BRONZE_BASE_PATH) / 'stockport' / 'weather'
    SILVER_DIR = Path(SILVER_BASE_PATH) / 'stockport' / 'weather'
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# LOAD & TRANSFORM
# ============================================================================

print("📥 Loading Bronze weather data...")
df_weather = pd.read_parquet(BRONZE_DIR / 'parquet' / 'weather_stockport')
print(f"   ✅ Loaded {len(df_weather)} weather snapshots\n")

print("🗺️  Adding BNG coordinates...")
df_weather = transform_coordinates_to_bng(df_weather, 'lat', 'lon')

print("\n📅 Parsing datetime...")
df_weather = parse_date_fields(df_weather, ['datetime'])

print("\n📋 Adding audit columns...")
df_weather = add_audit_columns(df_weather, 'openweather_api', 'silver')

# ============================================================================
# SAVE
# ============================================================================

print("\n💾 Saving Silver weather...")
output_path = SILVER_DIR / 'parquet' / 'weather_cleaned'

# Create parquet directory if it doesn't exist
output_path.parent.mkdir(parents=True, exist_ok=True)

df_weather.to_parquet(output_path, engine='pyarrow', compression='snappy', index=False)

print(f"✅ Saved {len(df_weather)} records to: {output_path}")
print(f"\n⏱️  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------
# Show data statistics
print("="*80)
print("Data Processing Complete! ✅")
print("="*80)

# COMMAND ----------
