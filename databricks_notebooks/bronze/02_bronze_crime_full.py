# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Crime Data
# MAGIC
# MAGIC **Converted from**: `notebooks/02_bronze_crime_full.py`
# MAGIC
# MAGIC Collects crime statistics from UK Police API

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
import json
from pathlib import Path
from datetime import datetime
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

# Import utilities
from src.utils.api_client import PoliceAPIClient, add_metadata
from src.bronze.ingestion import (
    flatten_crime_records,
    save_to_bronze_parquet,
    save_to_bronze_json,
    create_bronze_summary
)

print("="*80)
print("🥉 BRONZE LAYER: Stockport Crime Data Ingestion")
print("="*80)
print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ============================================================================
# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
# ============================================================================

# Load Stockport configuration
# Configuration loaded from databricks_settings
config = STOCKPORT_CONFIG

STOCKPORT = config['stockport']
BBOX = STOCKPORT['bounding_box']

print("📍 Target Area:")
print(f"   Name: {STOCKPORT['name']}")
print(f"   Council: {STOCKPORT['council']}")
print(f"   Bounding Box: {BBOX}")
print(f"   Area: {STOCKPORT['area_km2']} km²")
print()

# Grid configuration
GRID_SIZE = 4  # 4x4 = 16 query points for good coverage

print("🔍 Query Strategy:")
print(f"   Grid Size: {GRID_SIZE} x {GRID_SIZE} = {GRID_SIZE**2} points")
print(f"   Coverage: Full Stockport area")
print()

# Output directories
if IS_DATABRICKS:
    # Use DBFS path in Databricks
    BRONZE_DIR = f"{BRONZE_BASE_PATH}/stockport/crime"
    dbutils.fs.mkdirs(BRONZE_DIR)
    BRONZE_DIR = BRONZE_DIR.replace('dbfs:', '/dbfs')  # Convert to local path for Python operations
else:
    # Use local path for development
    from pathlib import Path
    BRONZE_DIR = Path(BRONZE_BASE_PATH) / 'stockport' / 'crime'
    os.makedirs(BRONZE_DIR, exist_ok=True)

print(f"💾 Output Directory: {BRONZE_DIR}")
print()

# ============================================================================
# DATA COLLECTION
# ============================================================================

print("="*80)
print("🔍 COLLECTING CRIME DATA")
print("="*80)
print()

# Initialize Police API client
client = PoliceAPIClient()

# Query using grid pattern
print(f"⏳ Querying UK Police API with {GRID_SIZE}x{GRID_SIZE} grid...")
print(f"   (This will make {GRID_SIZE**2} API calls with 1s delay between each)")
print()

crimes = client.query_crimes_grid(BBOX, grid_size=GRID_SIZE)

print()
print(f"✅ Successfully retrieved {len(crimes)} unique crime records")
print()

if len(crimes) == 0:
    print("⚠️  WARNING: No crime data found")
    print("   Possible reasons:")
    print("   - No recent crimes in this area")
    print("   - API rate limiting")
    print("   - Data not yet published for latest month")
    print()
    print("💡 This is normal - not all areas have crime data for every month")
    
    # Still create empty files for consistency
    crimes = []

# ============================================================================
# ANALYZE DATA
# ============================================================================

if crimes:
    print("="*80)
    print("📊 DATA ANALYSIS")
    print("="*80)
    print()
    
    # Count by category
    category_counts = {}
    for crime in crimes:
        category = crime.get('category', 'unknown')
        category_counts[category] = category_counts.get(category, 0) + 1
    
    print("Crime categories:")
    for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"   {category:30} {count:5}")
    print()
    
    # Count by month
    month_counts = {}
    for crime in crimes:
        month = crime.get('month', 'unknown')
        month_counts[month] = month_counts.get(month, 0) + 1
    
    print("Monthly distribution:")
    for month, count in sorted(month_counts.items(), reverse=True):
        print(f"   {month:10} {count:5}")
    print()
    
    # Check for outcome status
    with_outcome = sum(1 for c in crimes if c.get('outcome_status'))
    print(f"Crimes with outcome status: {with_outcome} ({with_outcome/len(crimes)*100:.1f}%)")
    print()

# ============================================================================
# SAVE RAW DATA (JSON)
# ============================================================================

print("="*80)
print("💾 SAVING RAW DATA")
print("="*80)
print()

print("1️⃣  Saving raw JSON response...")

# Add metadata
crimes_with_metadata = add_metadata(
    crimes,
    source='uk_police_api',
    area='stockport'
)

json_path = save_to_bronze_json(
    crimes_with_metadata,
    str(BRONZE_DIR / 'raw_json'),
    'police_crimes_full'
)

if json_path:
    print(f"   ✅ Raw JSON saved")
else:
    print(f"   ⚠️  Failed to save raw JSON")
print()

# ============================================================================
# FLATTEN AND SAVE AS PARQUET
# ============================================================================

if crimes:
    print("2️⃣  Flattening data structure...")
    
    flattened_data = flatten_crime_records(crimes)
    print(f"   ✅ Flattened {len(flattened_data)} records")
    print()
    
    # Add metadata columns
    for record in flattened_data:
        record['ingestion_timestamp'] = datetime.now().isoformat()
        record['ingestion_date'] = datetime.now().strftime('%Y-%m-%d')
        record['source_api'] = 'uk_police'
        record['area'] = 'stockport'
    
    print("3️⃣  Saving as Parquet...")
    
    parquet_path = # Save to local parquet first
parquet_path = save_to_bronze_parquet(
        flattened_data,
        str(BRONZE_DIR / 'parquet'),
        'crimes_stockport',
        partition_cols=['ingestion_date', 'month', 'category']
    )
    
    if parquet_path:
        print(f"   ✅ Parquet saved (partitioned by date, month, and category)")
    else:
        print(f"   ⚠️  Failed to save Parquet")
    print()
else:
    print("2️⃣  No data to flatten - skipping Parquet creation")
    parquet_path = None
    print()

# ============================================================================
# CREATE SUMMARY
# ============================================================================

print("4️⃣  Creating ingestion summary...")

create_bronze_summary(
    str(BRONZE_DIR),
    'crime',
    len(crimes),
    parquet_path or json_path
)

print()

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print("="*80)
print("✅ BRONZE LAYER INGESTION COMPLETE")
print("="*80)
print()
print(f"📊 Total Records: {len(crimes)}")
print(f"💾 Data Location: {BRONZE_DIR}")
print()
print("📁 Files Created:")
print(f"   1. Raw JSON: {BRONZE_DIR / 'raw_json'}")
if crimes:
    print(f"   2. Parquet:  {BRONZE_DIR / 'parquet'}")
print(f"   3. Summary:  {BRONZE_DIR / '_summary_*.json'}")
print()
print("🎯 Next Steps:")
if crimes:
    print("   1. Review crime data in Parquet files")
    print("   2. Run Silver layer transformation")
    print("   3. Analyze crime patterns")
else:
    print("   1. Check back later for more recent data")
    print("   2. Try different time periods")
    print("   3. Consider expanding to nearby areas")
print()
print(f"⏱️  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
