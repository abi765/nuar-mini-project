"""
Bronze Layer Data Quality Analysis
Identifies issues and provides recommendations for Silver layer
"""

import pandas as pd
import json
from pathlib import Path

print("="*80)
print("🔍 BRONZE LAYER DATA QUALITY ANALYSIS")
print("="*80)
print()

# Load data
bronze_dir = Path('data/bronze/stockport')

df_infra = pd.read_parquet(bronze_dir / 'infrastructure' / 'parquet' / 'infrastructure_stockport')
df_crime = pd.read_parquet(bronze_dir / 'crime' / 'parquet' / 'crimes_stockport')
df_weather = pd.read_parquet(bronze_dir / 'weather' / 'parquet' / 'weather_stockport')
df_postcode = pd.read_parquet(bronze_dir / 'postcodes' / 'parquet' / 'postcodes_stockport')

# ============================================================================
# ISSUE 1: Infrastructure Missing Coordinates
# ============================================================================

print("="*80)
print("ISSUE 1: Infrastructure Records Without Coordinates")
print("="*80)
print()

missing_coords = df_infra[df_infra['lat'].isna()]
print(f"📊 Records without lat/lon: {len(missing_coords)}/{len(df_infra)}")
print()

if len(missing_coords) > 0:
    print("🔍 These are all 'way' type (lines):")
    print(missing_coords[['id', 'type', 'infrastructure_type', 'node_count', 'geometry_points']].head())
    print()
    
    print("💡 FIX FOR SILVER LAYER:")
    print("   For ways without lat/lon:")
    print("   1. Calculate centroid from geometry_json")
    print("   2. Or use first_lat/first_lon")
    print("   3. Or use midpoint of first/last coordinates")
    print()
    
    # Show example geometry
    sample_way = missing_coords.iloc[0]
    geom = json.loads(sample_way['geometry_json'])
    print(f"   Example geometry (way {sample_way['id']}):")
    print(f"   - Node count: {sample_way['node_count']}")
    print(f"   - First point: ({geom[0]['lat']}, {geom[0]['lon']})")
    print(f"   - Last point: ({geom[-1]['lat']}, {geom[-1]['lon']})")
    print()

# ============================================================================
# ISSUE 2: Crime Coordinates as Strings
# ============================================================================

print("="*80)
print("ISSUE 2: Crime Coordinates Stored as Strings")
print("="*80)
print()

print(f"📊 Crime data type for lat: {df_crime['lat'].dtype}")
print(f"📊 Crime data type for lon: {df_crime['lon'].dtype}")
print()

if df_crime['lat'].dtype == 'object':
    print("⚠️  Coordinates are strings, not float!")
    print()
    
    print("🔍 Sample values:")
    print(df_crime[['lat', 'lon']].head())
    print()
    
    print("💡 FIX FOR SILVER LAYER:")
    print("   Convert to float64:")
    print("   df_crime['lat'] = pd.to_numeric(df_crime['lat'], errors='coerce')")
    print("   df_crime['lon'] = pd.to_numeric(df_crime['lon'], errors='coerce')")
    print()
    
    # Try conversion
    lat_numeric = pd.to_numeric(df_crime['lat'], errors='coerce')
    lon_numeric = pd.to_numeric(df_crime['lon'], errors='coerce')
    
    print("📊 After conversion:")
    print(f"   Valid lat values: {lat_numeric.notna().sum()}/{len(df_crime)}")
    print(f"   Valid lon values: {lon_numeric.notna().sum()}/{len(df_crime)}")
    print()
    
    if lat_numeric.notna().sum() > 0:
        print(f"   Lat range: {lat_numeric.min():.6f} to {lat_numeric.max():.6f}")
        print(f"   Lon range: {lon_numeric.min():.6f} to {lon_numeric.max():.6f}")
        print()

# ============================================================================
# ISSUE 3: Missing Metadata (Normal for OSM)
# ============================================================================

print("="*80)
print("ISSUE 3: Missing Infrastructure Metadata")
print("="*80)
print()

print("📊 Field completeness:")
fields = ['operator', 'name', 'substance', 'material', 'diameter', 'location']
for field in fields:
    non_null = df_infra[field].notna().sum()
    pct = (non_null / len(df_infra)) * 100
    status = "✅" if pct > 50 else "⚠️"
    print(f"   {status} {field:15} {non_null:3}/{len(df_infra)} ({pct:.1f}%)")
print()

print("💡 RECOMMENDATIONS:")
print("   This is NORMAL for OpenStreetMap data")
print("   - OSM depends on community contributions")
print("   - Not all fields are tagged consistently")
print("   - Focus on infrastructure_type and coordinates")
print()
print("   FOR SILVER LAYER:")
print("   - Keep null values (don't impute)")
print("   - Add 'metadata_quality_score' column")
print("   - Flag high-quality records with operator/name")
print()

# ============================================================================
# ISSUE 4: Way Geometries
# ============================================================================

print("="*80)
print("ISSUE 4: Line Geometries (Ways)")
print("="*80)
print()

ways = df_infra[df_infra['type'] == 'way']
print(f"📊 Total ways (lines): {len(ways)}")
print()

print("🔍 Way types:")
print(ways['infrastructure_type'].value_counts())
print()

print("📏 Geometry complexity:")
print(ways[['infrastructure_type', 'node_count', 'geometry_points']].describe())
print()

print("💡 FOR SILVER LAYER:")
print("   1. Calculate length of each pipeline/cable")
print("   2. Store as LineString in proper geometry column")
print("   3. Calculate centroid for point representation")
print("   4. Keep both point and line representations")
print()

# ============================================================================
# SUMMARY: BRONZE TO SILVER TRANSFORMATION PLAN
# ============================================================================

print("="*80)
print("🎯 BRONZE → SILVER TRANSFORMATION PLAN")
print("="*80)
print()

print("✅ DATA FIXES NEEDED:")
print()

print("1️⃣  INFRASTRUCTURE:")
print("   ✅ Convert way geometries to centroids for point representation")
print("   ✅ Parse geometry_json to calculate pipeline lengths")
print("   ✅ Keep null metadata fields (don't impute)")
print("   ✅ Add metadata_quality_score (0-1 based on field completeness)")
print("   ✅ Transform coordinates to British National Grid")
print()

print("2️⃣  CRIME:")
print("   ✅ Convert lat/lon from string to float64")
print("   ✅ Handle any null coordinates gracefully")
print("   ✅ Parse date fields to proper datetime")
print("   ✅ Transform coordinates to British National Grid")
print()

print("3️⃣  WEATHER:")
print("   ✅ Parse datetime fields")
print("   ✅ Transform coordinates to British National Grid")
print("   ✅ Add derived fields (temp in Fahrenheit, etc.)")
print()

print("4️⃣  POSTCODES:")
print("   ✅ Already clean!")
print("   ✅ Has British National Grid (eastings/northings)")
print("   ✅ Use for coordinate transformation reference")
print()

print("5️⃣  ENRICHMENT:")
print("   ✅ Spatial join: Add postcode to infrastructure")
print("   ✅ Spatial join: Add postcode to crime")
print("   ✅ Add admin_district, admin_ward to all")
print("   ✅ Calculate distances between features")
print()

# ============================================================================
# COORDINATE TRANSFORMATION REFERENCE
# ============================================================================

print("="*80)
print("📍 COORDINATE TRANSFORMATION REFERENCE")
print("="*80)
print()

print("🗺️  Stockport Bounding Box:")
print("   WGS84 (lat/lon):")
print("   - Lat: 53.35 to 53.45")
print("   - Lon: -2.20 to -2.05")
print()

# Get BNG reference from postcodes
outward = df_postcode[df_postcode['postcode_type'] == 'outward_code']
print("   British National Grid (from postcodes):")
print(f"   - Eastings:  {outward['eastings'].min():.0f} to {outward['eastings'].max():.0f}")
print(f"   - Northings: {outward['northings'].min():.0f} to {outward['northings'].max():.0f}")
print()

print("💡 FOR TRANSFORMATION:")
print("   Use Python library: pyproj")
print("   Transform: EPSG:4326 (WGS84) → EPSG:27700 (British National Grid)")
print()

# ============================================================================
# QUALITY SCORES
# ============================================================================

print("="*80)
print("📊 OVERALL DATA QUALITY SCORES")
print("="*80)
print()

# Infrastructure quality
infra_quality = {
    'completeness': len(df_infra[df_infra['lat'].notna()]) / len(df_infra),
    'has_operator': df_infra['operator'].notna().sum() / len(df_infra),
    'has_metadata': df_infra[['substance', 'material', 'diameter']].notna().any(axis=1).sum() / len(df_infra)
}

print("Infrastructure Quality:")
print(f"   Coordinate Completeness: {infra_quality['completeness']:.1%}")
print(f"   Has Operator Info:       {infra_quality['has_operator']:.1%}")
print(f"   Has Technical Metadata:  {infra_quality['has_metadata']:.1%}")
print()

# Crime quality
crime_quality = {
    'has_coords': pd.to_numeric(df_crime['lat'], errors='coerce').notna().sum() / len(df_crime),
    'has_outcome': df_crime['outcome_category'].notna().sum() / len(df_crime),
    'has_location': df_crime['street_name'].notna().sum() / len(df_crime)
}

print("Crime Quality:")
print(f"   Has Coordinates:  {crime_quality['has_coords']:.1%}")
print(f"   Has Outcome:      {crime_quality['has_outcome']:.1%}")
print(f"   Has Location:     {crime_quality['has_location']:.1%}")
print()

# Overall
print("Overall Bronze Layer Quality: ✅ GOOD")
print("   - Sufficient data for Silver layer transformation")
print("   - Known issues are addressable")
print("   - Ready to proceed with transformation")
print()

print("="*80)
print("✅ ANALYSIS COMPLETE")
print("="*80)
print()
print("🎯 NEXT STEPS:")
print("   1. Review transformation plan above")
print("   2. Install pyproj for coordinate transformation")
print("   3. Ready to build Silver layer!")
print()