# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Environment Smoke Test
# MAGIC
# MAGIC Validates that the Databricks environment is correctly configured

# COMMAND ----------
# Test 1: Catalog and Schema Setup
print("🔍 Test 1: Checking catalog and schemas...")
try:
    spark.sql("USE CATALOG nuar_catalog")
    schemas = spark.sql("SHOW SCHEMAS IN nuar_catalog").collect()
    schema_names = [row.databaseName for row in schemas]

    for schema in ['bronze', 'silver', 'gold']:
        if schema in schema_names:
            print(f"✅ Schema '{schema}' exists")
        else:
            print(f"❌ Schema '{schema}' not found")
except Exception as e:
    print(f"❌ Catalog error: {e}")

# COMMAND ----------
# Test 2: API Connectivity
print("\n🔍 Test 2: Testing API connectivity...")
print("⚠️  Note: API tests may fail due to network restrictions in Databricks")
print("   This is normal and won't prevent the data collection notebooks from working")
import requests

apis = {
    "Overpass": "https://overpass-api.de/api/status",
    "Police": "https://data.police.uk/api/forces",
    "Postcodes": "https://api.postcodes.io/postcodes/SK1+1EB"
}

failed_count = 0
for name, url in apis.items():
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            print(f"✅ {name} API: OK")
        else:
            print(f"⚠️  {name} API: Status {response.status_code}")
    except Exception as e:
        failed_count += 1
        if "name resolution" in str(e).lower():
            print(f"⚠️  {name} API: Network restricted (expected in some Databricks workspaces)")
        else:
            print(f"❌ {name} API: {str(e)}")

if failed_count == len(apis):
    print("\n⚠️  All API tests failed - likely network/firewall restriction")
    print("   ✅ This is okay! The actual data notebooks may still work")
    print("   ✅ Or you can run notebooks locally and upload data to Databricks")

# COMMAND ----------
# Test 3: Secrets Access
print("\n🔍 Test 3: Checking secrets...")
try:
    api_key = dbutils.secrets.get(scope="nuar_secrets", key="openweather_api_key")
    if api_key and len(api_key) > 10:
        print(f"✅ OpenWeather API key: Configured (length: {len(api_key)})")
    else:
        print("❌ OpenWeather API key: Invalid or empty")
except Exception as e:
    print(f"⚠️  Secret access: {str(e)}")
    print("   Note: You may need to create secret scope first")

# COMMAND ----------
# Test 4: Required Libraries
print("\n🔍 Test 4: Checking required libraries...")
try:
    import pandas as pd
    import geopandas as gpd
    from pyproj import Transformer
    from shapely.geometry import Point
    import scipy
    print("✅ All required libraries installed")
except ImportError as e:
    print(f"❌ Missing library: {e}")

# COMMAND ----------
# Test 5: Configuration Loading
print("\n🔍 Test 5: Loading project configuration...")
try:
    import sys
    sys.path.append('/Workspace/Repos/mnbabdullah765@yahoo.com/nuar_mini_project')

    from config.databricks_settings import *
    print(f"✅ Configuration loaded")
    print(f"   Catalog: {CATALOG_NAME}")
    print(f"   Bronze schema: {BRONZE_SCHEMA_FULL}")
    print(f"   Silver schema: {SILVER_SCHEMA_FULL}")
except Exception as e:
    print(f"❌ Configuration error: {e}")

# COMMAND ----------
print("\n" + "="*60)
print("🎉 Smoke Test Complete!")
print("="*60)
print("\n📊 Summary:")
print("   If 3-4 out of 5 tests passed, you're ready to proceed!")
print("   API connectivity failures are common and usually not blocking.")
print("\n🚀 Next Steps:")
print("   1. Run: setup_delta_tables.sql")
print("   2. Run: Bronze layer notebooks")
print("   3. If APIs still fail, run notebooks locally and upload data")
