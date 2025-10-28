# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Postcode Data
# MAGIC
# MAGIC **Converted from**: `notebooks/04_bronze_postcodes_full.py`
# MAGIC
# MAGIC Collects postcode reference data from Postcodes.io API

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
from src.utils.api_client import PostcodesAPIClient, add_metadata
from src.bronze.ingestion import (
    save_to_bronze_parquet,
    save_to_bronze_json,
    create_bronze_summary
)

print("="*80)
print("🥉 BRONZE LAYER: Stockport Postcode Data Ingestion")
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

print("📍 Target Area:")
print(f"   Name: {STOCKPORT['name']}")
print(f"   Council: {STOCKPORT['council']}")
print()

# Stockport postcode outward codes
STOCKPORT_OUTCODES = ['SK1', 'SK2', 'SK3', 'SK4', 'SK5', 'SK6', 'SK7', 'SK8']

print("📮 Postcode Areas:")
for outcode in STOCKPORT_OUTCODES:
    print(f"   - {outcode}")
print()

# Also collect sample full postcodes
SAMPLE_POSTCODES = STOCKPORT.get('sample_postcodes', [])

print(f"📋 Sample Postcodes: {len(SAMPLE_POSTCODES)}")
for postcode in SAMPLE_POSTCODES:
    print(f"   - {postcode}")
print()

# Output directories
if IS_DATABRICKS:
    # Use DBFS path in Databricks
    BRONZE_DIR = f"{BRONZE_BASE_PATH}/stockport/postcodes"
    dbutils.fs.mkdirs(BRONZE_DIR)
    BRONZE_DIR = BRONZE_DIR.replace('dbfs:', '/dbfs')  # Convert to local path for Python operations
else:
    # Use local path for development
    from pathlib import Path
    BRONZE_DIR = Path(BRONZE_BASE_PATH) / 'stockport' / 'postcodes'
    os.makedirs(BRONZE_DIR, exist_ok=True)

print(f"💾 Output Directory: {BRONZE_DIR}")
print()

# ============================================================================
# DATA COLLECTION - OUTWARD CODES
# ============================================================================

print("="*80)
print("🔍 COLLECTING OUTWARD CODE DATA")
print("="*80)
print()

# Initialize Postcodes API client
client = PostcodesAPIClient()

# Query outward codes
print(f"⏳ Querying Postcodes.io API for {len(STOCKPORT_OUTCODES)} outward codes...")
print()

outcode_data = client.search_postcodes_in_area(STOCKPORT_OUTCODES)

print()
print(f"✅ Retrieved data for {len(outcode_data)} outward codes")
print()

# ============================================================================
# DATA COLLECTION - SAMPLE POSTCODES
# ============================================================================

print("="*80)
print("🔍 COLLECTING SAMPLE POSTCODE DATA")
print("="*80)
print()

print(f"⏳ Querying {len(SAMPLE_POSTCODES)} sample postcodes...")
print()

sample_data = []
for postcode in SAMPLE_POSTCODES:
    print(f"   Querying {postcode}...")
    result = client.lookup_postcode(postcode)
    if result:
        sample_data.append(result)
        print(f"   ✅ {postcode}: Success")
    else:
        print(f"   ❌ {postcode}: Failed")

print()
print(f"✅ Retrieved {len(sample_data)} sample postcodes")
print()

# ============================================================================
# ANALYZE DATA
# ============================================================================

if outcode_data or sample_data:
    print("="*80)
    print("📊 DATA ANALYSIS")
    print("="*80)
    print()
    
    if outcode_data:
        print("Outward Code Data:")
        for outcode in outcode_data:
            code = outcode.get('outcode')
            lat = outcode.get('latitude')
            lon = outcode.get('longitude')
            district = outcode.get('admin_district')
            print(f"   {code}: ({lat:.4f}, {lon:.4f}) - {district}")
        print()
    
    if sample_data:
        print("Sample Postcode Data:")
        districts = set()
        wards = set()
        
        for postcode in sample_data:
            districts.add(postcode.get('admin_district'))
            wards.add(postcode.get('admin_ward'))
        
        print(f"   Unique districts: {len(districts)}")
        for district in sorted(districts):
            print(f"      - {district}")
        print()
        
        print(f"   Unique wards: {len(wards)}")
        for ward in sorted(wards)[:10]:  # Show first 10
            print(f"      - {ward}")
        if len(wards) > 10:
            print(f"      ... and {len(wards) - 10} more")
        print()

# ============================================================================
# SAVE RAW DATA (JSON)
# ============================================================================

print("="*80)
print("💾 SAVING RAW DATA")
print("="*80)
print()

# Combine all data
all_postcode_data = {
    'outward_codes': outcode_data,
    'sample_postcodes': sample_data
}

print("1️⃣  Saving raw JSON response...")

# Add metadata
postcodes_with_metadata = add_metadata(
    all_postcode_data,
    source='postcodes_io_api',
    area='stockport'
)

json_path = save_to_bronze_json(
    postcodes_with_metadata,
    str(BRONZE_DIR / 'raw_json'),
    'postcodes_full'
)

if json_path:
    print(f"   ✅ Raw JSON saved")
else:
    print(f"   ⚠️  Failed to save raw JSON")
print()

# ============================================================================
# FLATTEN AND SAVE AS PARQUET
# ============================================================================

print("2️⃣  Preparing data for Parquet...")

# Prepare outward codes for Parquet
outward_records = []
for outcode in outcode_data:
    record = {
        'postcode_type': 'outward_code',
        'postcode': outcode.get('outcode'),
        'lat': outcode.get('latitude'),
        'lon': outcode.get('longitude'),
        'eastings': outcode.get('eastings'),
        'northings': outcode.get('northings'),
        'admin_district': outcode.get('admin_district', [None])[0] if outcode.get('admin_district') else None,
        'admin_county': outcode.get('admin_county', [None])[0] if outcode.get('admin_county') else None,
        'admin_ward': None,
        'parish': None,
        'region': outcode.get('region'),
        'country': outcode.get('country', [None])[0] if outcode.get('country') else None,
        'parliamentary_constituency': None,
        'ingestion_timestamp': datetime.now().isoformat(),
        'ingestion_date': datetime.now().strftime('%Y-%m-%d'),
        'source_api': 'postcodes_io',
        'area': 'stockport'
    }
    outward_records.append(record)

# Prepare sample postcodes for Parquet
sample_records = []
for postcode in sample_data:
    record = {
        'postcode_type': 'full_postcode',
        'postcode': postcode.get('postcode'),
        'lat': postcode.get('latitude'),
        'lon': postcode.get('longitude'),
        'eastings': postcode.get('eastings'),
        'northings': postcode.get('northings'),
        'admin_district': postcode.get('admin_district'),
        'admin_county': postcode.get('admin_county'),
        'admin_ward': postcode.get('admin_ward'),
        'parish': postcode.get('parish'),
        'region': postcode.get('region'),
        'country': postcode.get('country'),
        'parliamentary_constituency': postcode.get('parliamentary_constituency'),
        'ingestion_timestamp': datetime.now().isoformat(),
        'ingestion_date': datetime.now().strftime('%Y-%m-%d'),
        'source_api': 'postcodes_io',
        'area': 'stockport'
    }
    sample_records.append(record)

# Combine all records
all_records = outward_records + sample_records
print(f"   ✅ Prepared {len(all_records)} records")
print(f"      - Outward codes: {len(outward_records)}")
print(f"      - Sample postcodes: {len(sample_records)}")
print()

print("3️⃣  Saving as Parquet...")

parquet_path = # Save to local parquet first
parquet_path = save_to_bronze_parquet(
    all_records,
    str(BRONZE_DIR / 'parquet'),
    'postcodes_stockport',
    partition_cols=['ingestion_date', 'postcode_type']
)

if parquet_path:
    print(f"   ✅ Parquet saved (partitioned by date and type)")
else:
    print(f"   ⚠️  Failed to save Parquet")
print()

# ============================================================================
# CREATE SUMMARY
# ============================================================================

print("4️⃣  Creating ingestion summary...")

create_bronze_summary(
    str(BRONZE_DIR),
    'postcodes',
    len(all_records),
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
print(f"📊 Total Records: {len(all_records)}")
print(f"   - Outward codes: {len(outward_records)}")
print(f"   - Sample postcodes: {len(sample_records)}")
print()
print(f"💾 Data Location: {BRONZE_DIR}")
print()
print("📁 Files Created:")
print(f"   1. Raw JSON: {BRONZE_DIR / 'raw_json'}")
print(f"   2. Parquet:  {BRONZE_DIR / 'parquet'}")
print(f"   3. Summary:  {BRONZE_DIR / '_summary_*.json'}")
print()
print("🎯 Next Steps:")
print("   1. Use postcode data for coordinate conversion")
print("   2. Map infrastructure to postcodes")
print("   3. Analyze data by admin district/ward")
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
