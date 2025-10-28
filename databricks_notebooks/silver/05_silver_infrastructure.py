# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Infrastructure Transformation
# MAGIC
# MAGIC **Converted from**: `notebooks/05_silver_infrastructure.py`
# MAGIC
# MAGIC Transforms and enriches infrastructure data with BNG coordinates

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
import pandas as pd
import numpy as np
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

# Import transformation utilities
from src.silver.transformations import (
    fix_infrastructure_coordinates,
    transform_coordinates_to_bng,
    calculate_infrastructure_length,
    calculate_metadata_quality_score,
    add_quality_flags,
    validate_stockport_bbox,
    add_audit_columns,
    enforce_schema
)

print("="*80)
print("🥈 SILVER LAYER: Infrastructure Transformation")
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
    BRONZE_DIR = f"{BRONZE_BASE_PATH}/stockport/infrastructure"
    SILVER_DIR = f"{SILVER_BASE_PATH}/stockport/infrastructure"
    POSTCODE_DIR = f"{BRONZE_BASE_PATH}/stockport/postcodes"

    # Create Silver directory
    dbutils.fs.mkdirs(SILVER_DIR)

    # Convert to local paths for Python operations
    BRONZE_DIR = BRONZE_DIR.replace('dbfs:', '/dbfs')
    SILVER_DIR = SILVER_DIR.replace('dbfs:', '/dbfs')
    POSTCODE_DIR = POSTCODE_DIR.replace('dbfs:', '/dbfs')
else:
    # Use local paths for development
    from pathlib import Path
    BRONZE_DIR = Path(BRONZE_BASE_PATH) / 'stockport' / 'infrastructure'
    SILVER_DIR = Path(SILVER_BASE_PATH) / 'stockport' / 'infrastructure'
    POSTCODE_DIR = Path(BRONZE_BASE_PATH) / 'stockport' / 'postcodes'

    # Create Silver directory
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

print(f"📂 Bronze Input: {BRONZE_DIR}")
print(f"📂 Silver Output: {SILVER_DIR}")
print()

# ============================================================================
# LOAD BRONZE DATA
# ============================================================================

print("="*80)
print("📥 LOADING BRONZE DATA")
print("="*80)
print()

print("1️⃣  Loading infrastructure...")
df_infra = pd.read_parquet(BRONZE_DIR / 'parquet' / 'infrastructure_stockport')
print(f"   ✅ Loaded {len(df_infra)} infrastructure records")
print()

print("2️⃣  Loading postcodes (for enrichment)...")
df_postcode = pd.read_parquet(POSTCODE_DIR / 'parquet' / 'postcodes_stockport')
print(f"   ✅ Loaded {len(df_postcode)} postcode records")
print()

# ============================================================================
# TRANSFORMATION 1: FIX MISSING COORDINATES
# ============================================================================

print("="*80)
print("🔧 TRANSFORMATION 1: Fix Missing Coordinates")
print("="*80)
print()

initial_missing = df_infra['lat'].isna().sum()
print(f"📊 Initial state: {initial_missing} records missing coordinates")
print()

df_infra = fix_infrastructure_coordinates(df_infra)

final_missing = df_infra['lat'].isna().sum()
print(f"\n📊 Final state: {final_missing} records missing coordinates")
print(f"✅ Fixed: {initial_missing - final_missing} records")
print()

# ============================================================================
# TRANSFORMATION 2: CALCULATE LINE LENGTHS
# ============================================================================

print("="*80)
print("📏 TRANSFORMATION 2: Calculate Infrastructure Lengths")
print("="*80)
print()

df_infra = calculate_infrastructure_length(df_infra)

# Show length statistics
lengths = df_infra[df_infra['length_meters'].notna()]
if len(lengths) > 0:
    print(f"\n📊 Length Statistics:")
    print(f"   Min: {lengths['length_meters'].min():.0f}m")
    print(f"   Max: {lengths['length_meters'].max():.0f}m")
    print(f"   Mean: {lengths['length_meters'].mean():.0f}m")
    print(f"   Total: {lengths['length_meters'].sum():.0f}m")
print()

# ============================================================================
# TRANSFORMATION 3: COORDINATE TRANSFORMATION
# ============================================================================

print("="*80)
print("🗺️  TRANSFORMATION 3: Transform to British National Grid")
print("="*80)
print()

df_infra = transform_coordinates_to_bng(
    df_infra,
    lat_col='lat',
    lon_col='lon',
    drop_original=False  # Keep WGS84 for reference
)

# Show BNG statistics
bng_valid = df_infra[['easting_bng', 'northing_bng']].notna().all(axis=1).sum()
print(f"\n📊 BNG Statistics:")
print(f"   Valid BNG coordinates: {bng_valid}/{len(df_infra)}")
print(f"   Easting range: {df_infra['easting_bng'].min():.0f} to {df_infra['easting_bng'].max():.0f}")
print(f"   Northing range: {df_infra['northing_bng'].min():.0f} to {df_infra['northing_bng'].max():.0f}")
print()

# ============================================================================
# TRANSFORMATION 4: VALIDATE LOCATION
# ============================================================================

print("="*80)
print("✅ TRANSFORMATION 4: Validate Locations")
print("="*80)
print()

df_infra = validate_stockport_bbox(df_infra)

invalid = (~df_infra['is_valid_location']).sum()
if invalid > 0:
    print(f"   ⚠️  {invalid} records outside Stockport bbox (will be flagged)")
print()

# ============================================================================
# TRANSFORMATION 5: SPATIAL ENRICHMENT
# ============================================================================

print("="*80)
print("🗺️  TRANSFORMATION 5: Spatial Enrichment with Postcodes")
print("="*80)
print()

print("Preparing postcode reference data...")

# Use full postcodes for better accuracy
postcodes_full = df_postcode[df_postcode['postcode_type'] == 'full_postcode'].copy()

if len(postcodes_full) == 0:
    print("   ⚠️  No full postcodes available, using outward codes...")
    postcodes_full = df_postcode[df_postcode['postcode_type'] == 'outward_code'].copy()

# Transform postcode coordinates to BNG if needed
if 'eastings' not in postcodes_full.columns:
    print("   🔄 Transforming postcode coordinates...")
    postcodes_full = transform_coordinates_to_bng(
        postcodes_full,
        lat_col='lat',
        lon_col='lon'
    )
    postcodes_full = postcodes_full.rename(columns={
        'easting_bng': 'eastings',
        'northing_bng': 'northings'
    })

print(f"   📍 Using {len(postcodes_full)} reference points")
print()

# Perform spatial join
from src.silver.transformations import spatial_join_nearest

df_infra = spatial_join_nearest(
    df_infra,
    postcodes_full,
    left_coords=('easting_bng', 'northing_bng'),
    right_coords=('eastings', 'northings'),
    right_columns=['postcode', 'admin_district', 'admin_ward'],
    max_distance=2000  # 2km max distance
)

enriched = df_infra['postcode'].notna().sum()
print(f"\n✅ Enrichment complete: {enriched}/{len(df_infra)} records matched")
print()

# ============================================================================
# TRANSFORMATION 6: METADATA QUALITY SCORE
# ============================================================================

print("="*80)
print("📊 TRANSFORMATION 6: Calculate Quality Scores")
print("="*80)
print()

metadata_fields = ['operator', 'name', 'substance', 'material']
df_infra['metadata_quality_score'] = calculate_metadata_quality_score(
    df_infra,
    metadata_fields
)

# Quality tier
df_infra['quality_tier'] = df_infra['metadata_quality_score'].apply(
    lambda x: 'high' if x > 0.5 else 'medium' if x > 0 else 'basic'
)

print("📈 Quality Score Distribution:")
print(df_infra['quality_tier'].value_counts())
print()

# ============================================================================
# TRANSFORMATION 7: ADD QUALITY FLAGS
# ============================================================================

print("="*80)
print("🏷️  TRANSFORMATION 7: Add Quality Flags")
print("="*80)
print()

df_infra = add_quality_flags(df_infra)

print("Quality Flags Summary:")
for col in ['has_coordinates', 'has_bng_coords', 'has_postcode', 'has_admin_data']:
    if col in df_infra.columns:
        count = df_infra[col].sum()
        pct = (count / len(df_infra)) * 100
        print(f"   {col:20} {count:3}/{len(df_infra)} ({pct:.1f}%)")
print()

# ============================================================================
# TRANSFORMATION 8: ADD AUDIT COLUMNS
# ============================================================================

print("="*80)
print("📋 TRANSFORMATION 8: Add Audit Columns")
print("="*80)
print()

df_infra = add_audit_columns(
    df_infra,
    source_system='overpass_api',
    layer='silver'
)

print("✅ Audit columns added:")
print("   - transformation_timestamp")
print("   - transformation_date")
print("   - data_layer")
print("   - source_system")
print()

# ============================================================================
# TRANSFORMATION 9: CLEAN AND REORDER COLUMNS
# ============================================================================

print("="*80)
print("🧹 TRANSFORMATION 9: Clean and Reorder Columns")
print("="*80)
print()

# Define Silver schema
silver_columns = [
    # IDs
    'id', 'osm_id',
    
    # Classification
    'infrastructure_type', 'type',
    
    # Coordinates (WGS84)
    'lat', 'lon',
    
    # Coordinates (BNG)
    'easting_bng', 'northing_bng',
    
    # Geometry
    'length_meters',
    
    # Metadata
    'operator', 'name', 'substance', 'material', 'diameter', 'location',
    
    # Enrichment
    'postcode', 'admin_district', 'admin_ward',
    
    # Quality
    'metadata_quality_score', 'quality_tier',
    'has_coordinates', 'has_bng_coords', 'has_postcode', 'has_admin_data',
    'is_valid_location',
    
    # Audit
    'ingestion_timestamp', 'ingestion_date',
    'transformation_timestamp', 'transformation_date',
    'source_system', 'data_layer', 'area'
]

# Keep only columns that exist
df_silver = df_infra[[col for col in silver_columns if col in df_infra.columns]].copy()

# Add osm_id alias
df_silver['osm_id'] = df_silver['id']

print(f"✅ Final Silver columns: {len(df_silver.columns)}")
print(f"   Selected {len(df_silver.columns)} of {len(silver_columns)} schema columns")
print()

# ============================================================================
# TRANSFORMATION 10: ENFORCE DATA TYPES
# ============================================================================

print("="*80)
print("📋 TRANSFORMATION 10: Enforce Data Types")
print("="*80)
print()

# Define schema
schema = {
    'id': 'int64',
    'osm_id': 'int64',
    'infrastructure_type': 'category',
    'type': 'category',
    'lat': 'float64',
    'lon': 'float64',
    'easting_bng': 'Int64',
    'northing_bng': 'Int64',
    'length_meters': 'float64',
    'quality_tier': 'category',
    'has_coordinates': 'bool',
    'has_bng_coords': 'bool',
    'has_postcode': 'bool',
    'has_admin_data': 'bool',
    'is_valid_location': 'bool',
    'metadata_quality_score': 'float64',
    'transformation_date': 'category',
    'data_layer': 'category'
}

df_silver = enforce_schema(df_silver, schema)

print("✅ Data types enforced")
print()

# ============================================================================
# SAVE SILVER DATA
# ============================================================================

print("="*80)
print("💾 SAVING SILVER DATA")
print("="*80)
print()

# Save as Parquet with partitioning
output_path = SILVER_DIR / 'parquet' / 'infrastructure_cleaned'
df_silver.to_parquet(
    output_path,
    partition_cols=['transformation_date', 'infrastructure_type'],
    engine='pyarrow',
    compression='snappy',
    index=False
)

print(f"✅ Saved to: {output_path}")
print(f"   Records: {len(df_silver)}")
print(f"   Columns: {len(df_silver.columns)}")
print(f"   Partitioned by: transformation_date, infrastructure_type")
print()

# Also save summary CSV for easy viewing
summary_path = SILVER_DIR / 'infrastructure_silver_summary.csv'
df_silver[['id', 'infrastructure_type', 'lat', 'lon', 'easting_bng', 'northing_bng',
           'postcode', 'admin_district', 'quality_tier']].to_csv(summary_path, index=False)
print(f"✅ Summary CSV: {summary_path}")
print()

# ============================================================================
# QUALITY REPORT
# ============================================================================

print("="*80)
print("📊 SILVER LAYER QUALITY REPORT")
print("="*80)
print()

print("Data Completeness:")
print(f"   Total records: {len(df_silver)}")
print(f"   With coordinates: {df_silver['has_coordinates'].sum()} ({df_silver['has_coordinates'].mean()*100:.1f}%)")
print(f"   With BNG coords: {df_silver['has_bng_coords'].sum()} ({df_silver['has_bng_coords'].mean()*100:.1f}%)")
print(f"   With postcode: {df_silver['has_postcode'].sum()} ({df_silver['has_postcode'].mean()*100:.1f}%)")
print(f"   Valid location: {df_silver['is_valid_location'].sum()} ({df_silver['is_valid_location'].mean()*100:.1f}%)")
print()

print("Infrastructure Types:")
print(df_silver['infrastructure_type'].value_counts())
print()

print("Quality Tiers:")
print(df_silver['quality_tier'].value_counts())
print()

print("Geographic Coverage:")
enriched_by_district = df_silver[df_silver['admin_district'].notna()]['admin_district'].value_counts()
if len(enriched_by_district) > 0:
    print("   Districts:")
    for district, count in enriched_by_district.items():
        print(f"      {district}: {count}")
print()

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print("="*80)
print("✅ SILVER LAYER TRANSFORMATION COMPLETE")
print("="*80)
print()

print(f"📊 Transformation Summary:")
print(f"   Input (Bronze): {len(df_infra)} records")
print(f"   Output (Silver): {len(df_silver)} records")
print(f"   Fixed coordinates: {initial_missing - final_missing}")
print(f"   Transformed to BNG: {bng_valid}")
print(f"   Enriched with postcodes: {enriched}")
print()

print(f"💾 Output Location: {SILVER_DIR}")
print(f"   1. Parquet: {output_path}")
print(f"   2. Summary CSV: {summary_path}")
print()

print(f"🎯 Next Steps:")
print(f"   1. Review Silver data quality")
print(f"   2. Run Silver transformation for crime data")
print(f"   3. Build Gold layer analytics")
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
