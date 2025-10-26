"""
Interactive Bronze Data Exploration
Run this in Python REPL or Jupyter for interactive exploration
"""

import pandas as pd
import json
from pathlib import Path

# Set pandas display options for better viewing
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)

# Load data
bronze_dir = Path('data/bronze/stockport')

# ============================================================================
# Load all datasets
# ============================================================================

print("Loading data...")

# Infrastructure
df_infra = pd.read_parquet(bronze_dir / 'infrastructure' / 'parquet' / 'infrastructure_stockport')
print(f"✅ Loaded {len(df_infra)} infrastructure records")

# Crime
df_crime = pd.read_parquet(bronze_dir / 'crime' / 'parquet' / 'crimes_stockport')
print(f"✅ Loaded {len(df_crime)} crime records")

# Weather
df_weather = pd.read_parquet(bronze_dir / 'weather' / 'parquet' / 'weather_stockport')
print(f"✅ Loaded {len(df_weather)} weather snapshots")

# Postcodes
df_postcode = pd.read_parquet(bronze_dir / 'postcodes' / 'parquet' / 'postcodes_stockport')
print(f"✅ Loaded {len(df_postcode)} postcode records")

print("\n" + "="*80)
print("🎯 INTERACTIVE EXPLORATION COMMANDS")
print("="*80)
print("""
Now you can explore the data interactively! Try these commands:

# ============================================================================
# INFRASTRUCTURE EXPLORATION
# ============================================================================

# View first few records
df_infra.head()
df_infra.head(10)

# View column names
df_infra.columns.tolist()

# View data types
df_infra.dtypes

# Summary statistics
df_infra.describe()

# Count by infrastructure type
df_infra['infrastructure_type'].value_counts()

# Filter by type
manholes = df_infra[df_infra['infrastructure_type'] == 'manhole']
poles = df_infra[df_infra['infrastructure_type'] == 'pole']
pipelines = df_infra[df_infra['infrastructure_type'] == 'pipeline']

manholes.head()
len(manholes)

# View specific columns
df_infra[['id', 'infrastructure_type', 'lat', 'lon', 'operator']].head()

# Check for missing values
df_infra.isnull().sum()

# View OSM tags for a manhole
sample_manhole = df_infra[df_infra['infrastructure_type'] == 'manhole'].iloc[0]
print(json.loads(sample_manhole['all_tags']))

# View geometry info for pipelines
pipelines[['id', 'node_count', 'geometry_points', 'first_lat', 'first_lon']].head()

# ============================================================================
# CRIME EXPLORATION
# ============================================================================

# View first few records
df_crime.head()

# Count by category
df_crime['category'].value_counts()

# Count by month
df_crime['month'].value_counts()

# View specific crime
df_crime.iloc[0]

# Filter violent crimes
violent = df_crime[df_crime['category'] == 'violent-crime']
violent.head()

# Check outcomes
df_crime['outcome_category'].value_counts()

# Crimes with locations
df_crime[['category', 'month', 'street_name', 'lat', 'lon']].head()

# ============================================================================
# WEATHER EXPLORATION
# ============================================================================

# View weather snapshot
df_weather.head()

# View all weather columns
df_weather.columns.tolist()

# Key weather metrics
df_weather[['datetime', 'temp_celsius', 'weather_description', 
            'humidity_percent', 'wind_speed_ms']].head()

# ============================================================================
# POSTCODE EXPLORATION
# ============================================================================

# View all postcodes
df_postcode.head()

# Separate outward codes and full postcodes
outward = df_postcode[df_postcode['postcode_type'] == 'outward_code']
full_postcodes = df_postcode[df_postcode['postcode_type'] == 'full_postcode']

# View outward codes (SK areas)
outward[['postcode', 'lat', 'lon', 'admin_district']]

# View full postcodes with British National Grid
full_postcodes[['postcode', 'lat', 'lon', 'eastings', 'northings', 'admin_ward']]

# ============================================================================
# CROSS-DATASET QUERIES
# ============================================================================

# Find infrastructure near a crime location
crime_point = df_crime.iloc[0]
crime_lat, crime_lon = crime_point['lat'], crime_point['lon']

# Simple distance calculation (approximate)
df_infra['dist_to_crime'] = ((df_infra['lat'] - crime_lat)**2 + 
                              (df_infra['lon'] - crime_lon)**2)**0.5

nearby_infra = df_infra.nsmallest(5, 'dist_to_crime')
nearby_infra[['id', 'infrastructure_type', 'lat', 'lon', 'dist_to_crime']]

# ============================================================================
# SAVE FILTERED DATASETS
# ============================================================================

# Save manholes only
manholes.to_csv('data/sample/manholes_only.csv', index=False)

# Save violent crimes only
violent.to_csv('data/sample/violent_crimes_only.csv', index=False)

# ============================================================================
# DATA QUALITY CHECKS
# ============================================================================

# Check coordinate ranges
print(f"Infrastructure lat range: {df_infra['lat'].min():.6f} to {df_infra['lat'].max():.6f}")
print(f"Infrastructure lon range: {df_infra['lon'].min():.6f} to {df_infra['lon'].max():.6f}")

# Check for duplicates
print(f"Duplicate infrastructure IDs: {df_infra['id'].duplicated().sum()}")
print(f"Duplicate crime IDs: {df_crime['crime_id'].duplicated().sum()}")

# Check for nulls
print(f"Infrastructure nulls per column:")
print(df_infra.isnull().sum()[df_infra.isnull().sum() > 0])

# ============================================================================
# EXPORT FOR VISUALIZATION
# ============================================================================

# Export coordinates for mapping
coords_infra = df_infra[['id', 'infrastructure_type', 'lat', 'lon']].copy()
coords_infra.to_csv('data/sample/infrastructure_coords.csv', index=False)

coords_crime = df_crime[['crime_id', 'category', 'lat', 'lon']].copy()
coords_crime.to_csv('data/sample/crime_coords.csv', index=False)

print("✅ Coordinate files saved for mapping!")

# ============================================================================
# SUMMARY STATISTICS
# ============================================================================

print("\\n" + "="*80)
print("📊 DATASET SUMMARY")
print("="*80)
print(f"Infrastructure: {len(df_infra)} records")
print(f"  - Poles: {len(df_infra[df_infra['infrastructure_type']=='pole'])}")
print(f"  - Manholes: {len(df_infra[df_infra['infrastructure_type']=='manhole'])}")
print(f"  - Pipelines: {len(df_infra[df_infra['infrastructure_type']=='pipeline'])}")
print(f"  - Cables: {len(df_infra[df_infra['infrastructure_type']=='cable'])}")
print()
print(f"Crime: {len(df_crime)} records")
for cat in df_crime['category'].unique():
    count = len(df_crime[df_crime['category']==cat])
    print(f"  - {cat}: {count}")
print()
print(f"Weather: {len(df_weather)} snapshots")
print(f"Postcodes: {len(df_postcode)} records")
print("="*80)

""")

print("\n💡 TIP: Copy and paste the commands above to explore your data!")
print("   Or run specific sections as needed.")