"""
Silver Layer: Crime Transformation
Transform Bronze crime data into clean, validated Silver table
"""

import sys
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import transformation utilities
from src.silver.transformations import (
    convert_string_coordinates_to_float,
    transform_coordinates_to_bng,
    parse_date_fields,
    add_quality_flags,
    validate_stockport_bbox,
    add_audit_columns,
    enforce_schema,
    spatial_join_nearest
)

print("="*80)
print("🥈 SILVER LAYER: Crime Transformation")
print("="*80)
print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ============================================================================
# CONFIGURATION
# ============================================================================

# Paths
BRONZE_DIR = project_root / 'data' / 'bronze' / 'stockport' / 'crime'
SILVER_DIR = project_root / 'data' / 'silver' / 'stockport' / 'crime'
POSTCODE_DIR = project_root / 'data' / 'bronze' / 'stockport' / 'postcodes'

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

print("1️⃣  Loading crime data...")
df_crime = pd.read_parquet(BRONZE_DIR / 'parquet' / 'crimes_stockport')
print(f"   ✅ Loaded {len(df_crime)} crime records")
print()

print("2️⃣  Loading postcodes (for enrichment)...")
df_postcode = pd.read_parquet(POSTCODE_DIR / 'parquet' / 'postcodes_stockport')
print(f"   ✅ Loaded {len(df_postcode)} postcode records")
print()

# ============================================================================
# TRANSFORMATION 1: FIX COORDINATE DATA TYPES
# ============================================================================

print("="*80)
print("🔢 TRANSFORMATION 1: Convert String Coordinates to Float")
print("="*80)
print()

print(f"📊 Initial data types:")
print(f"   lat: {df_crime['lat'].dtype}")
print(f"   lon: {df_crime['lon'].dtype}")
print()

df_crime = convert_string_coordinates_to_float(df_crime)

print(f"\n📊 Final data types:")
print(f"   lat: {df_crime['lat'].dtype}")
print(f"   lon: {df_crime['lon'].dtype}")
print()

# ============================================================================
# TRANSFORMATION 2: PARSE DATE FIELDS
# ============================================================================

print("="*80)
print("📅 TRANSFORMATION 2: Parse Date Fields")
print("="*80)
print()

df_crime = parse_date_fields(df_crime, ['month', 'outcome_date'])

print()

# ============================================================================
# TRANSFORMATION 3: COORDINATE TRANSFORMATION
# ============================================================================

print("="*80)
print("🗺️  TRANSFORMATION 3: Transform to British National Grid")
print("="*80)
print()

df_crime = transform_coordinates_to_bng(
    df_crime,
    lat_col='lat',
    lon_col='lon',
    drop_original=False
)

# Show BNG statistics
bng_valid = df_crime[['easting_bng', 'northing_bng']].notna().all(axis=1).sum()
print(f"\n📊 BNG Statistics:")
print(f"   Valid BNG coordinates: {bng_valid}/{len(df_crime)}")

if bng_valid > 0:
    print(f"   Easting range: {df_crime['easting_bng'].min():.0f} to {df_crime['easting_bng'].max():.0f}")
    print(f"   Northing range: {df_crime['northing_bng'].min():.0f} to {df_crime['northing_bng'].max():.0f}")
print()

# ============================================================================
# TRANSFORMATION 4: VALIDATE LOCATION
# ============================================================================

print("="*80)
print("✅ TRANSFORMATION 4: Validate Locations")
print("="*80)
print()

df_crime = validate_stockport_bbox(df_crime)

invalid = (~df_crime['is_valid_location']).sum()
if invalid > 0:
    print(f"   ⚠️  {invalid} records outside Stockport bbox")
print()

# ============================================================================
# TRANSFORMATION 5: SPATIAL ENRICHMENT
# ============================================================================

print("="*80)
print("🗺️  TRANSFORMATION 5: Spatial Enrichment with Postcodes")
print("="*80)
print()

# Prepare postcode data
postcodes_full = df_postcode[df_postcode['postcode_type'] == 'full_postcode'].copy()

if len(postcodes_full) == 0:
    postcodes_full = df_postcode[df_postcode['postcode_type'] == 'outward_code'].copy()

# Ensure postcodes have BNG coords
if 'eastings' not in postcodes_full.columns:
    postcodes_full = transform_coordinates_to_bng(postcodes_full, 'lat', 'lon')
    postcodes_full = postcodes_full.rename(columns={
        'easting_bng': 'eastings',
        'northing_bng': 'northings'
    })

print(f"   📍 Using {len(postcodes_full)} reference points")
print()

# Perform spatial join
df_crime = spatial_join_nearest(
    df_crime,
    postcodes_full,
    left_coords=('easting_bng', 'northing_bng'),
    right_coords=('eastings', 'northings'),
    right_columns=['postcode', 'admin_district', 'admin_ward'],
    max_distance=1000  # 1km max for crimes
)

enriched = df_crime['postcode'].notna().sum()
print(f"\n✅ Enrichment complete: {enriched}/{len(df_crime)} records matched")
print()

# ============================================================================
# TRANSFORMATION 6: ADD QUALITY FLAGS
# ============================================================================

print("="*80)
print("🏷️  TRANSFORMATION 6: Add Quality Flags")
print("="*80)
print()

df_crime = add_quality_flags(df_crime)

# Add crime-specific flags
df_crime['has_outcome'] = df_crime['outcome_category'].notna()
df_crime['has_street'] = df_crime['street_name'].notna()

print("Quality Flags Summary:")
for col in ['has_coordinates', 'has_bng_coords', 'has_postcode', 'has_outcome', 'has_street']:
    if col in df_crime.columns:
        count = df_crime[col].sum()
        pct = (count / len(df_crime)) * 100
        print(f"   {col:20} {count:3}/{len(df_crime)} ({pct:.1f}%)")
print()

# ============================================================================
# TRANSFORMATION 7: ADD AUDIT COLUMNS
# ============================================================================

print("="*80)
print("📋 TRANSFORMATION 7: Add Audit Columns")
print("="*80)
print()

df_crime = add_audit_columns(
    df_crime,
    source_system='uk_police_api',
    layer='silver'
)

print("✅ Audit columns added")
print()

# ============================================================================
# TRANSFORMATION 8: CLEAN AND REORDER COLUMNS
# ============================================================================

print("="*80)
print("🧹 TRANSFORMATION 8: Clean and Reorder Columns")
print("="*80)
print()

# Define Silver schema
silver_columns = [
    # IDs
    'crime_id', 'persistent_id',
    
    # Classification
    'category',
    
    # Time
    'month',
    
    # Coordinates (WGS84)
    'lat', 'lon',
    
    # Coordinates (BNG)
    'easting_bng', 'northing_bng',
    
    # Location
    'street_id', 'street_name', 'location_type', 'location_subtype',
    
    # Enrichment
    'postcode', 'admin_district', 'admin_ward',
    
    # Outcome
    'outcome_category', 'outcome_date',
    
    # Context
    'context',
    
    # Quality
    'has_coordinates', 'has_bng_coords', 'has_postcode',
    'has_outcome', 'has_street', 'is_valid_location',
    
    # Audit
    'ingestion_timestamp', 'ingestion_date',
    'transformation_timestamp', 'transformation_date',
    'source_system', 'data_layer', 'area'
]

# Keep only existing columns
df_silver = df_crime[[col for col in silver_columns if col in df_crime.columns]].copy()

print(f"✅ Final Silver columns: {len(df_silver.columns)}")
print()

# ============================================================================
# TRANSFORMATION 9: ENFORCE DATA TYPES
# ============================================================================

print("="*80)
print("📋 TRANSFORMATION 9: Enforce Data Types")
print("="*80)
print()

schema = {
    'crime_id': 'int64',
    'category': 'category',
    'month': 'datetime64[ns]',
    'outcome_date': 'datetime64[ns]',
    'lat': 'float64',
    'lon': 'float64',
    'easting_bng': 'Int64',
    'northing_bng': 'Int64',
    'has_coordinates': 'bool',
    'has_bng_coords': 'bool',
    'has_postcode': 'bool',
    'has_outcome': 'bool',
    'has_street': 'bool',
    'is_valid_location': 'bool',
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
output_path = SILVER_DIR / 'parquet' / 'crime_cleaned'
df_silver.to_parquet(
    output_path,
    partition_cols=['transformation_date', 'category'],
    engine='pyarrow',
    compression='snappy',
    index=False
)

print(f"✅ Saved to: {output_path}")
print(f"   Records: {len(df_silver)}")
print(f"   Columns: {len(df_silver.columns)}")
print(f"   Partitioned by: transformation_date, category")
print()

# Save summary CSV
summary_path = SILVER_DIR / 'crime_silver_summary.csv'
df_silver[['crime_id', 'category', 'month', 'lat', 'lon', 'easting_bng', 'northing_bng',
           'street_name', 'postcode', 'outcome_category']].to_csv(summary_path, index=False)
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
print(f"   With outcome: {df_silver['has_outcome'].sum()} ({df_silver['has_outcome'].mean()*100:.1f}%)")
print()

print("Crime Categories:")
print(df_silver['category'].value_counts())
print()

print("Monthly Distribution:")
print(df_silver.groupby(df_silver['month'].dt.to_period('M')).size())
print()

print("Outcomes:")
if 'outcome_category' in df_silver.columns:
    print(df_silver['outcome_category'].value_counts())
print()

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print("="*80)
print("✅ SILVER LAYER TRANSFORMATION COMPLETE")
print("="*80)
print()

print(f"📊 Transformation Summary:")
print(f"   Input (Bronze): {len(df_crime)} records")
print(f"   Output (Silver): {len(df_silver)} records")
print(f"   Transformed to BNG: {bng_valid}")
print(f"   Enriched with postcodes: {enriched}")
print()

print(f"💾 Output Location: {SILVER_DIR}")
print(f"   1. Parquet: {output_path}")
print(f"   2. Summary CSV: {summary_path}")
print()

print(f"⏱️  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)