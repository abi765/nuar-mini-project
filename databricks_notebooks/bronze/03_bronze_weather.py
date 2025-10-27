# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Weather Data
# MAGIC
# MAGIC **Converted from**: `notebooks/03_bronze_weather.py`
# MAGIC
# MAGIC Collects current weather snapshots from OpenWeatherMap API

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
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import Databricks configuration
import sys
sys.path.append('/Workspace/Repos/mnbabdullah765@yahoo.com/nuar_mini_project')

from config.databricks_settings import *

# Import utilities
from src.utils.api_client import WeatherAPIClient, add_metadata
from src.bronze.ingestion import (
    flatten_weather_data,
    save_to_bronze_parquet,
    save_to_bronze_json,
    create_bronze_summary
)

print("="*80)
print("🥉 BRONZE LAYER: Stockport Weather Data Ingestion")
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
CENTER = STOCKPORT['center']

print("📍 Target Area:")
print(f"   Name: {STOCKPORT['name']}")
print(f"   Council: {STOCKPORT['council']}")
print(f"   Center: ({CENTER['lat']}, {CENTER['lon']})")
print()

# Get API key
WEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')

if not WEATHER_API_KEY:
    print("❌ ERROR: OPENWEATHER_API_KEY not found in .env file")
    print("   Please add your API key to .env file")
    sys.exit(1)

print("✅ API Key loaded from .env")
print()

# Output directories
BRONZE_DIR = project_root / 'data' / 'bronze' / 'stockport' / 'weather'
os.makedirs(BRONZE_DIR, exist_ok=True)

print(f"💾 Output Directory: {BRONZE_DIR}")
print()

# ============================================================================
# DATA COLLECTION
# ============================================================================

print("="*80)
print("🔍 COLLECTING WEATHER DATA")
print("="*80)
print()

# Initialize Weather API client
client = WeatherAPIClient(WEATHER_API_KEY)

# Query current weather
print(f"⏳ Querying OpenWeatherMap API for Stockport center...")
print()

weather_data = client.get_current_weather(CENTER['lat'], CENTER['lon'])

if not weather_data:
    print("\n❌ FAILED: Could not retrieve weather data")
    print("   Possible reasons:")
    print("   - Invalid API key")
    print("   - Network issues")
    print("   - API quota exceeded")
    sys.exit(1)

print(f"✅ Successfully retrieved weather data")
print()

# ============================================================================
# ANALYZE DATA
# ============================================================================

print("="*80)
print("📊 CURRENT WEATHER")
print("="*80)
print()

main = weather_data.get('main', {})
weather = weather_data.get('weather', [{}])[0]
wind = weather_data.get('wind', {})

print(f"Location: {weather_data.get('name')}")
print(f"Time: {datetime.fromtimestamp(weather_data.get('dt', 0)).strftime('%Y-%m-%d %H:%M:%S')}")
print()
print(f"🌡️  Temperature: {main.get('temp')}°C (feels like {main.get('feels_like')}°C)")
print(f"☁️  Conditions: {weather.get('description')}")
print(f"💧 Humidity: {main.get('humidity')}%")
print(f"💨 Wind: {wind.get('speed')} m/s")
print(f"🔍 Visibility: {weather_data.get('visibility')} meters")
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
weather_with_metadata = add_metadata(
    weather_data,
    source='openweathermap_api',
    area='stockport'
)

json_path = save_to_bronze_json(
    weather_with_metadata,
    str(BRONZE_DIR / 'raw_json'),
    'weather_snapshot'
)

if json_path:
    print(f"   ✅ Raw JSON saved")
else:
    print(f"   ⚠️  Failed to save raw JSON")
print()

# ============================================================================
# FLATTEN AND SAVE AS PARQUET
# ============================================================================

print("2️⃣  Flattening data structure...")

flattened_data = flatten_weather_data(weather_data)
print(f"   ✅ Flattened weather record")
print()

# Add metadata columns
flattened_data['ingestion_timestamp'] = datetime.now().isoformat()
flattened_data['ingestion_date'] = datetime.now().strftime('%Y-%m-%d')
flattened_data['source_api'] = 'openweathermap'
flattened_data['area'] = 'stockport'

print("3️⃣  Saving as Parquet...")

parquet_path = # Save to local parquet first
parquet_path = save_to_bronze_parquet(
    [flattened_data],  # Single record as list
    str(BRONZE_DIR / 'parquet'),
    'weather_stockport',
    partition_cols=['ingestion_date']
)

if parquet_path:
    print(f"   ✅ Parquet saved (partitioned by date)")
else:
    print(f"   ⚠️  Failed to save Parquet")
print()

# ============================================================================
# CREATE SUMMARY
# ============================================================================

print("4️⃣  Creating ingestion summary...")

create_bronze_summary(
    str(BRONZE_DIR),
    'weather',
    1,  # Single weather snapshot
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
print(f"📊 Total Records: 1 (snapshot)")
print(f"💾 Data Location: {BRONZE_DIR}")
print()
print("📁 Files Created:")
print(f"   1. Raw JSON: {BRONZE_DIR / 'raw_json'}")
print(f"   2. Parquet:  {BRONZE_DIR / 'parquet'}")
print(f"   3. Summary:  {BRONZE_DIR / '_summary_*.json'}")
print()
print("🎯 Next Steps:")
print("   1. Run this script regularly (daily/hourly) to build history")
print("   2. Review weather data in Parquet files")
print("   3. Analyze weather patterns over time")
print()
print("💡 Schedule this script:")
print("   - Cron job: Run every hour")
print("   - Databricks: Schedule as daily job")
print("   - Manual: Run as needed")
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
