"""
Silver Layer: Weather Transformation
Transform Bronze weather data for consistency and BNG coordinates
"""

import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

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
# CONFIGURATION
# ============================================================================

BRONZE_DIR = project_root / 'data' / 'bronze' / 'stockport' / 'weather'
SILVER_DIR = project_root / 'data' / 'silver' / 'stockport' / 'weather'
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