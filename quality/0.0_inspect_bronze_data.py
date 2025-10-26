"""
Bronze Layer Data Inspection
Detailed analysis of all collected Stockport data
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime

print("="*80)
print("🔍 BRONZE LAYER DATA INSPECTION - Stockport")
print("="*80)
print(f"📅 Inspection Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# Project paths
project_root = Path.cwd()
bronze_dir = project_root / 'data' / 'bronze' / 'stockport'

# ============================================================================
# 1. INFRASTRUCTURE DATA
# ============================================================================

print("="*80)
print("1️⃣  INFRASTRUCTURE DATA")
print("="*80)
print()

infra_parquet = bronze_dir / 'infrastructure' / 'parquet' / 'infrastructure_stockport'

if infra_parquet.exists():
    df_infra = pd.read_parquet(infra_parquet)
    
    print(f"📊 Total Records: {len(df_infra)}")
    print(f"📋 Columns: {len(df_infra.columns)}")
    print()
    
    # Show all columns
    print("📝 All Columns:")
    for i, col in enumerate(df_infra.columns, 1):
        print(f"   {i:2}. {col}")
    print()
    
    # Data types
    print("🔤 Data Types:")
    print(df_infra.dtypes)
    print()
    
    # Infrastructure types breakdown
    print("🏗️  Infrastructure Types:")
    print(df_infra['infrastructure_type'].value_counts())
    print()
    
    # Geometry types
    print("📐 Geometry Types:")
    print(df_infra['type'].value_counts())
    print()
    
    # Check for missing values
    print("❓ Missing Values:")
    missing = df_infra.isnull().sum()
    missing = missing[missing > 0].sort_values(ascending=False)
    if len(missing) > 0:
        print(missing)
    else:
        print("   ✅ No missing values!")
    print()
    
    # Coordinate ranges
    print("📍 Coordinate Ranges:")
    print(f"   Latitude:  {df_infra['lat'].min():.6f} to {df_infra['lat'].max():.6f}")
    print(f"   Longitude: {df_infra['lon'].min():.6f} to {df_infra['lon'].max():.6f}")
    print()
    
    # Sample records by type
    print("📋 Sample Records by Infrastructure Type:")
    for infra_type in df_infra['infrastructure_type'].unique():
        print(f"\n   --- {infra_type.upper()} ---")
        sample = df_infra[df_infra['infrastructure_type'] == infra_type].iloc[0]
        print(f"   ID: {sample['id']}")
        print(f"   Type: {sample['type']}")
        print(f"   Location: ({sample['lat']:.6f}, {sample['lon']:.6f})")
        print(f"   Operator: {sample['operator']}")
        print(f"   Name: {sample['name']}")
        if sample['type'] == 'way':
            print(f"   Node Count: {sample['node_count']}")
            print(f"   Geometry Points: {sample['geometry_points']}")
    print()
    
    # Show full first record
    print("🔎 First Complete Record:")
    print(df_infra.iloc[0])
    print()
    
    # Check all_tags (OSM metadata)
    print("🏷️  Sample OSM Tags (first manhole):")
    manhole = df_infra[df_infra['infrastructure_type'] == 'manhole'].iloc[0]
    tags = json.loads(manhole['all_tags'])
    for key, value in tags.items():
        print(f"   {key}: {value}")
    print()
    
else:
    print("❌ Infrastructure Parquet not found!")
    print()

# ============================================================================
# 2. CRIME DATA
# ============================================================================

print("="*80)
print("2️⃣  CRIME DATA")
print("="*80)
print()

crime_parquet = bronze_dir / 'crime' / 'parquet' / 'crimes_stockport'

if crime_parquet.exists():
    df_crime = pd.read_parquet(crime_parquet)
    
    print(f"📊 Total Records: {len(df_crime)}")
    print(f"📋 Columns: {len(df_crime.columns)}")
    print()
    
    # Show all columns
    print("📝 All Columns:")
    for i, col in enumerate(df_crime.columns, 1):
        print(f"   {i:2}. {col}")
    print()
    
    # Data types
    print("🔤 Data Types:")
    print(df_crime.dtypes)
    print()
    
    # Crime categories
    print("🚔 Crime Categories:")
    print(df_crime['category'].value_counts())
    print()
    
    # Monthly distribution
    print("📅 Monthly Distribution:")
    print(df_crime['month'].value_counts())
    print()
    
    # Outcome status
    print("⚖️  Outcome Status:")
    outcome_counts = df_crime['outcome_category'].value_counts()
    print(outcome_counts)
    print()
    
    # Missing values
    print("❓ Missing Values:")
    missing = df_crime.isnull().sum()
    missing = missing[missing > 0].sort_values(ascending=False)
    if len(missing) > 0:
        print(missing)
    else:
        print("   ✅ No missing values!")
    print()
    
    # Coordinate ranges
    print("📍 Coordinate Ranges:")
    lat_non_null = df_crime['lat'].dropna()
    lon_non_null = df_crime['lon'].dropna()
    if len(lat_non_null) > 0:
        # Convert to float if stored as string
        try:
            lat_non_null = pd.to_numeric(lat_non_null, errors='coerce')
            lon_non_null = pd.to_numeric(lon_non_null, errors='coerce')
            lat_non_null = lat_non_null.dropna()
            lon_non_null = lon_non_null.dropna()
            
            if len(lat_non_null) > 0:
                print(f"   Latitude:  {lat_non_null.min():.6f} to {lat_non_null.max():.6f}")
                print(f"   Longitude: {lon_non_null.min():.6f} to {lon_non_null.max():.6f}")
            else:
                print(f"   ⚠️  All coordinates are null or invalid")
        except:
            print(f"   ⚠️  Coordinates stored as strings: {lat_non_null.iloc[0]}, {lon_non_null.iloc[0]}")
    else:
        print(f"   ⚠️  No coordinates available")
    print()
    
    # Sample records
    print("📋 Sample Crime Records:")
    for i, row in df_crime.head(3).iterrows():
        print(f"\n   Crime #{i+1}:")
        print(f"   ID: {row['crime_id']}")
        print(f"   Category: {row['category']}")
        print(f"   Month: {row['month']}")
        print(f"   Location: {row['street_name']}")
        print(f"   Coordinates: ({row['lat']}, {row['lon']})")
        print(f"   Outcome: {row['outcome_category']}")
    print()
    
    # Show full first record
    print("🔎 First Complete Record:")
    print(df_crime.iloc[0])
    print()
    
else:
    print("❌ Crime Parquet not found!")
    print()

# ============================================================================
# 3. WEATHER DATA
# ============================================================================

print("="*80)
print("3️⃣  WEATHER DATA")
print("="*80)
print()

weather_parquet = bronze_dir / 'weather' / 'parquet' / 'weather_stockport'

if weather_parquet.exists():
    df_weather = pd.read_parquet(weather_parquet)
    
    print(f"📊 Total Snapshots: {len(df_weather)}")
    print(f"📋 Columns: {len(df_weather.columns)}")
    print()
    
    # Show all columns
    print("📝 All Columns:")
    for i, col in enumerate(df_weather.columns, 1):
        print(f"   {i:2}. {col}")
    print()
    
    # Data types
    print("🔤 Data Types:")
    print(df_weather.dtypes)
    print()
    
    # Latest snapshot details
    print("🌤️  Latest Weather Snapshot:")
    latest = df_weather.iloc[-1]
    print(f"   Location: {latest['location_name']}")
    print(f"   Time: {latest['datetime']}")
    print(f"   Temperature: {latest['temp_celsius']}°C")
    print(f"   Feels Like: {latest['feels_like_celsius']}°C")
    print(f"   Conditions: {latest['weather_description']}")
    print(f"   Humidity: {latest['humidity_percent']}%")
    print(f"   Wind Speed: {latest['wind_speed_ms']} m/s")
    print(f"   Pressure: {latest['pressure_hpa']} hPa")
    print(f"   Visibility: {latest['visibility_meters']} m")
    print(f"   Clouds: {latest['clouds_percent']}%")
    print()
    
    # Missing values
    print("❓ Missing Values:")
    missing = df_weather.isnull().sum()
    missing = missing[missing > 0].sort_values(ascending=False)
    if len(missing) > 0:
        print(missing)
    else:
        print("   ✅ No missing values!")
    print()
    
    # Show full record
    print("🔎 Complete Weather Record:")
    print(df_weather.iloc[0])
    print()
    
else:
    print("❌ Weather Parquet not found!")
    print()

# ============================================================================
# 4. POSTCODE DATA
# ============================================================================

print("="*80)
print("4️⃣  POSTCODE DATA")
print("="*80)
print()

postcode_parquet = bronze_dir / 'postcodes' / 'parquet' / 'postcodes_stockport'

if postcode_parquet.exists():
    df_postcode = pd.read_parquet(postcode_parquet)
    
    print(f"📊 Total Records: {len(df_postcode)}")
    print(f"📋 Columns: {len(df_postcode.columns)}")
    print()
    
    # Show all columns
    print("📝 All Columns:")
    for i, col in enumerate(df_postcode.columns, 1):
        print(f"   {i:2}. {col}")
    print()
    
    # Data types
    print("🔤 Data Types:")
    print(df_postcode.dtypes)
    print()
    
    # Postcode types
    print("📮 Postcode Types:")
    print(df_postcode['postcode_type'].value_counts())
    print()
    
    # Outward codes
    outward = df_postcode[df_postcode['postcode_type'] == 'outward_code']
    print("🗺️  Outward Codes (SK areas):")
    for _, row in outward.iterrows():
        print(f"   {row['postcode']}: ({row['lat']:.4f}, {row['lon']:.4f}) - {row['admin_district']}")
    print()
    
    # Full postcodes
    full = df_postcode[df_postcode['postcode_type'] == 'full_postcode']
    print("📍 Sample Full Postcodes:")
    for _, row in full.head(3).iterrows():
        print(f"   {row['postcode']}: {row['admin_ward']}, {row['admin_district']}")
        print(f"      WGS84: ({row['lat']:.6f}, {row['lon']:.6f})")
        print(f"      BNG:   ({row['eastings']}, {row['northings']})")
    print()
    
    # Missing values
    print("❓ Missing Values:")
    missing = df_postcode.isnull().sum()
    missing = missing[missing > 0].sort_values(ascending=False)
    if len(missing) > 0:
        print(missing)
    else:
        print("   ✅ No missing values!")
    print()
    
    # Show full first record
    print("🔎 First Complete Record:")
    print(df_postcode.iloc[0])
    print()
    
else:
    print("❌ Postcode Parquet not found!")
    print()

# ============================================================================
# 5. DATA QUALITY SUMMARY
# ============================================================================

print("="*80)
print("5️⃣  DATA QUALITY SUMMARY")
print("="*80)
print()

print("✅ Data Completeness:")
if infra_parquet.exists():
    print(f"   Infrastructure: {len(df_infra)} records ✅")
else:
    print(f"   Infrastructure: ❌ Missing")

if crime_parquet.exists():
    print(f"   Crime: {len(df_crime)} records ✅")
else:
    print(f"   Crime: ❌ Missing")

if weather_parquet.exists():
    print(f"   Weather: {len(df_weather)} snapshots ✅")
else:
    print(f"   Weather: ❌ Missing")

if postcode_parquet.exists():
    print(f"   Postcodes: {len(df_postcode)} records ✅")
else:
    print(f"   Postcodes: ❌ Missing")

print()

# Coordinate validation
print("📍 Coordinate Validation:")
if infra_parquet.exists():
    valid_coords = ((df_infra['lat'] >= 53.35) & (df_infra['lat'] <= 53.45) &
                    (df_infra['lon'] >= -2.2) & (df_infra['lon'] <= -2.05)).sum()
    print(f"   Infrastructure: {valid_coords}/{len(df_infra)} within Stockport bbox ✅")

if crime_parquet.exists() and len(lat_non_null) > 0:
    valid_coords = ((lat_non_null >= 53.35) & (lat_non_null <= 53.45) &
                    (lon_non_null >= -2.2) & (lon_non_null <= -2.05)).sum()
    print(f"   Crime: {valid_coords}/{len(lat_non_null)} within Stockport bbox")

print()

# Timestamp validation
print("🕐 Ingestion Timestamps:")
if infra_parquet.exists():
    print(f"   Infrastructure: {df_infra['ingestion_timestamp'].iloc[0]}")
if crime_parquet.exists():
    print(f"   Crime: {df_crime['ingestion_timestamp'].iloc[0]}")
if weather_parquet.exists():
    print(f"   Weather: {df_weather['ingestion_timestamp'].iloc[0]}")
if postcode_parquet.exists():
    print(f"   Postcodes: {df_postcode['ingestion_timestamp'].iloc[0]}")

print()

# ============================================================================
# 6. SAVE INSPECTION REPORT
# ============================================================================

print("="*80)
print("💾 SAVING INSPECTION REPORT")
print("="*80)
print()

report = {
    'inspection_date': datetime.now().isoformat(),
    'infrastructure': {
        'records': len(df_infra) if infra_parquet.exists() else 0,
        'columns': list(df_infra.columns) if infra_parquet.exists() else [],
        'types': df_infra['infrastructure_type'].value_counts().to_dict() if infra_parquet.exists() else {}
    },
    'crime': {
        'records': len(df_crime) if crime_parquet.exists() else 0,
        'columns': list(df_crime.columns) if crime_parquet.exists() else [],
        'categories': df_crime['category'].value_counts().to_dict() if crime_parquet.exists() else {}
    },
    'weather': {
        'snapshots': len(df_weather) if weather_parquet.exists() else 0,
        'columns': list(df_weather.columns) if weather_parquet.exists() else []
    },
    'postcodes': {
        'records': len(df_postcode) if postcode_parquet.exists() else 0,
        'columns': list(df_postcode.columns) if postcode_parquet.exists() else []
    }
}

report_path = bronze_dir / 'inspection_report.json'
with open(report_path, 'w') as f:
    json.dump(report, f, indent=2)

print(f"📊 Report saved to: {report_path}")
print()

print("="*80)
print("✅ INSPECTION COMPLETE")
print("="*80)
print()
print("🎯 Key Findings:")
print("   1. Review column names and data types above")
print("   2. Check for missing values and data quality issues")
print("   3. Verify coordinates are within expected ranges")
print("   4. Understand the structure before building Silver layer")
print()
print("📁 Next Steps:")
print("   1. Open inspection_report.json for summary")
print("   2. Query Parquet files directly if needed")
print("   3. Ready to design Silver layer transformations!")
print()