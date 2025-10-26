"""
Export Bronze Layer Parquet to CSV
Makes data easy to view in Excel, Numbers, or any spreadsheet app
"""

import pandas as pd
from pathlib import Path

print("="*80)
print("📊 EXPORTING BRONZE LAYER TO CSV")
print("="*80)
print()

# Paths
bronze_dir = Path('data/bronze/stockport')
csv_dir = Path('data/bronze/stockport_csv_export')
csv_dir.mkdir(exist_ok=True)

print(f"📂 Export Directory: {csv_dir}")
print()

# ============================================================================
# 1. INFRASTRUCTURE
# ============================================================================

print("1️⃣  Exporting Infrastructure Data...")

df_infra = pd.read_parquet(bronze_dir / 'infrastructure' / 'parquet' / 'infrastructure_stockport')

# Full export
output_file = csv_dir / 'infrastructure_full.csv'
df_infra.to_csv(output_file, index=False)
print(f"   ✅ Full data: {output_file} ({len(df_infra)} rows)")

# Simplified view (key columns only)
simple_cols = ['id', 'infrastructure_type', 'type', 'lat', 'lon', 
               'operator', 'name', 'substance', 'location']
df_simple = df_infra[simple_cols]
output_simple = csv_dir / 'infrastructure_simple.csv'
df_simple.to_csv(output_simple, index=False)
print(f"   ✅ Simple view: {output_simple}")

# By type
for infra_type in df_infra['infrastructure_type'].unique():
    df_type = df_infra[df_infra['infrastructure_type'] == infra_type]
    output_type = csv_dir / f'infrastructure_{infra_type}.csv'
    df_type.to_csv(output_type, index=False)
    print(f"   ✅ {infra_type}: {output_type} ({len(df_type)} rows)")

print()

# ============================================================================
# 2. CRIME
# ============================================================================

print("2️⃣  Exporting Crime Data...")

df_crime = pd.read_parquet(bronze_dir / 'crime' / 'parquet' / 'crimes_stockport')

# Full export
output_file = csv_dir / 'crime_full.csv'
df_crime.to_csv(output_file, index=False)
print(f"   ✅ Full data: {output_file} ({len(df_crime)} rows)")

# Simplified view
simple_cols = ['crime_id', 'category', 'month', 'lat', 'lon', 
               'street_name', 'outcome_category']
df_simple = df_crime[simple_cols]
output_simple = csv_dir / 'crime_simple.csv'
df_simple.to_csv(output_simple, index=False)
print(f"   ✅ Simple view: {output_simple}")

# By category
for category in df_crime['category'].unique():
    df_cat = df_crime[df_crime['category'] == category]
    output_cat = csv_dir / f'crime_{category.replace("-", "_")}.csv'
    df_cat.to_csv(output_cat, index=False)
    print(f"   ✅ {category}: {output_cat} ({len(df_cat)} rows)")

print()

# ============================================================================
# 3. WEATHER
# ============================================================================

print("3️⃣  Exporting Weather Data...")

df_weather = pd.read_parquet(bronze_dir / 'weather' / 'parquet' / 'weather_stockport')

# Full export
output_file = csv_dir / 'weather_snapshots.csv'
df_weather.to_csv(output_file, index=False)
print(f"   ✅ Weather snapshots: {output_file} ({len(df_weather)} rows)")

# Key metrics only
key_cols = ['datetime', 'location_name', 'temp_celsius', 'feels_like_celsius',
            'weather_description', 'humidity_percent', 'wind_speed_ms']
df_key = df_weather[key_cols]
output_key = csv_dir / 'weather_key_metrics.csv'
df_key.to_csv(output_key, index=False)
print(f"   ✅ Key metrics: {output_key}")

print()

# ============================================================================
# 4. POSTCODES
# ============================================================================

print("4️⃣  Exporting Postcode Data...")

df_postcode = pd.read_parquet(bronze_dir / 'postcodes' / 'parquet' / 'postcodes_stockport')

# Full export
output_file = csv_dir / 'postcodes_full.csv'
df_postcode.to_csv(output_file, index=False)
print(f"   ✅ Full data: {output_file} ({len(df_postcode)} rows)")

# Outward codes only
outward = df_postcode[df_postcode['postcode_type'] == 'outward_code']
output_outward = csv_dir / 'postcodes_outward_codes.csv'
outward.to_csv(output_outward, index=False)
print(f"   ✅ Outward codes (SK areas): {output_outward} ({len(outward)} rows)")

# Full postcodes only
full = df_postcode[df_postcode['postcode_type'] == 'full_postcode']
output_full = csv_dir / 'postcodes_full_only.csv'
full.to_csv(output_full, index=False)
print(f"   ✅ Full postcodes: {output_full} ({len(full)} rows)")

print()

# ============================================================================
# 5. SUMMARY & COORDINATE FILES
# ============================================================================

print("5️⃣  Creating Summary Files...")

# Infrastructure coordinates only (for mapping)
coords = df_infra[['id', 'infrastructure_type', 'lat', 'lon', 'operator']].copy()
coords = coords.dropna(subset=['lat', 'lon'])  # Remove nulls
output_coords = csv_dir / 'infrastructure_coordinates_for_mapping.csv'
coords.to_csv(output_coords, index=False)
print(f"   ✅ Infrastructure coords: {output_coords} ({len(coords)} rows)")

# Crime coordinates only (for mapping)
crime_coords = df_crime[['crime_id', 'category', 'month', 'lat', 'lon']].copy()
# Convert to numeric first
crime_coords['lat'] = pd.to_numeric(crime_coords['lat'], errors='coerce')
crime_coords['lon'] = pd.to_numeric(crime_coords['lon'], errors='coerce')
crime_coords = crime_coords.dropna(subset=['lat', 'lon'])
output_crime_coords = csv_dir / 'crime_coordinates_for_mapping.csv'
crime_coords.to_csv(output_crime_coords, index=False)
print(f"   ✅ Crime coords: {output_crime_coords} ({len(crime_coords)} rows)")

# Summary stats
summary = {
    'dataset': ['Infrastructure', 'Crime', 'Weather', 'Postcodes'],
    'total_records': [len(df_infra), len(df_crime), len(df_weather), len(df_postcode)],
    'has_coordinates': [
        len(coords),
        len(crime_coords),
        len(df_weather),
        len(df_postcode)
    ]
}
df_summary = pd.DataFrame(summary)
output_summary = csv_dir / 'data_summary.csv'
df_summary.to_csv(output_summary, index=False)
print(f"   ✅ Data summary: {output_summary}")

print()

# ============================================================================
# COMPLETION
# ============================================================================

print("="*80)
print("✅ EXPORT COMPLETE")
print("="*80)
print()
print(f"📁 All CSV files saved to: {csv_dir}")
print()
print("📊 Files Created:")
print()
print("   INFRASTRUCTURE (9 files):")
print("      - infrastructure_full.csv (all columns)")
print("      - infrastructure_simple.csv (key columns only)")
print("      - infrastructure_pole.csv")
print("      - infrastructure_manhole.csv")
print("      - infrastructure_pipeline.csv")
print("      - infrastructure_cable.csv")
print("      - infrastructure_coordinates_for_mapping.csv")
print()
print("   CRIME (4+ files):")
print("      - crime_full.csv (all columns)")
print("      - crime_simple.csv (key columns only)")
print("      - crime_[category].csv (one per category)")
print("      - crime_coordinates_for_mapping.csv")
print()
print("   WEATHER (2 files):")
print("      - weather_snapshots.csv (all data)")
print("      - weather_key_metrics.csv (main metrics only)")
print()
print("   POSTCODES (3 files):")
print("      - postcodes_full.csv (all data)")
print("      - postcodes_outward_codes.csv (SK1-SK8)")
print("      - postcodes_full_only.csv (sample postcodes)")
print()
print("   SUMMARY (1 file):")
print("      - data_summary.csv (overview)")
print()
print("🎯 HOW TO USE:")
print("   1. Open any CSV in Excel, Google Sheets, or Numbers")
print("   2. Sort, filter, and explore your data")
print("   3. Use *_simple.csv files for quick overview")
print("   4. Use *_coordinates_for_mapping.csv for GIS tools")
print()
print("💡 TIP: Use infrastructure_simple.csv and crime_simple.csv")
print("   for the easiest viewing experience!")
print()