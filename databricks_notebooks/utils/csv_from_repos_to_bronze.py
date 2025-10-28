# Databricks notebook source
# MAGIC %md
# MAGIC # Convert CSVs from Repos to Bronze Delta Tables
# MAGIC
# MAGIC **Purpose**: Read CSV files from Databricks Repos and convert to Bronze Delta tables
# MAGIC
# MAGIC **Note**: This reads CSVs directly from your Git repository instead of FileStore uploads

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

print("=" * 80)
print("📄 CSV FROM REPOS TO BRONZE DELTA CONVERSION")
print("=" * 80)
print()
print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Locate Repository Path

# COMMAND ----------

import getpass

# Auto-detect repository path
username = getpass.getuser()
possible_repo_paths = [
    f'/Workspace/Repos/{username}/nuar-mini-project',
    f'/Repos/{username}/nuar-mini-project',
    '/Workspace/Repos/mnbabdullah765@yahoo.com/nuar-mini-project',
    '/Repos/mnbabdullah765@yahoo.com/nuar-mini-project',
]

repo_path = None
for path in possible_repo_paths:
    if os.path.exists(path):
        repo_path = path
        break

if not repo_path:
    print("❌ Repository not found. Tried:")
    for path in possible_repo_paths:
        print(f"   {path}")
    dbutils.notebook.exit("ERROR: Repository not found")

print(f"✅ Repository found at: {repo_path}")
print()

# CSV folder location
csv_folder = f"{repo_path}/bronze_csv_for_upload"
print(f"📁 CSV folder: {csv_folder}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Check Available CSV Files

# COMMAND ----------

print("🔍 Checking for CSV files...")
print()

# Check if folder exists
if not os.path.exists(csv_folder):
    print(f"❌ Folder not found: {csv_folder}")
    print()
    print("Make sure:")
    print("1. CSV files were committed to Git")
    print("2. Latest code was pulled in Databricks Repos")
    dbutils.notebook.exit("ERROR: CSV folder not found")

# List CSV files
csv_files = {}
expected_files = ['infrastructure.csv', 'crime.csv', 'weather.csv', 'postcodes.csv']

for filename in expected_files:
    file_path = os.path.join(csv_folder, filename)
    if os.path.exists(file_path):
        # Get file size
        size_kb = os.path.getsize(file_path) / 1024
        domain = filename.replace('.csv', '')
        csv_files[domain] = file_path
        print(f"✅ {filename} ({size_kb:.1f} KB)")
    else:
        print(f"❌ {filename} (not found)")

print()

if not csv_files:
    print("❌ No CSV files found!")
    dbutils.notebook.exit("ERROR: No CSV files found")

print(f"Found {len(csv_files)}/{len(expected_files)} CSV files")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Convert Each CSV to Delta Table

# COMMAND ----------

# Bronze Delta table base location
BRONZE_BASE = 'dbfs:/FileStore/nuar/bronze/stockport'

print("📦 Converting CSVs to Delta tables...")
print()

results = {}

for domain, csv_path in csv_files.items():
    print("=" * 80)
    print(f"📦 Converting: {domain}")
    print(f"   Source: {csv_path}")
    print("=" * 80)

    try:
        # Read CSV using Pandas (since it's a local file in Repos)
        import pandas as pd

        pdf = pd.read_csv(csv_path)
        record_count = len(pdf)
        print(f"   Records: {record_count}")

        # Convert to Spark DataFrame
        df = spark.createDataFrame(pdf)

        # Delta table path
        delta_path = f"{BRONZE_BASE}/{domain}"
        print(f"   Target: {delta_path}")

        # Write as Delta
        df.write.format("delta").mode("overwrite").save(delta_path)

        print(f"   ✅ Converted to Delta table")
        results[domain] = True

    except Exception as e:
        print(f"   ❌ Error: {e}")
        results[domain] = False

    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Catalog Tables

# COMMAND ----------

print("📊 Creating catalog tables...")
print()

# Create catalog and schemas if using Unity Catalog
try:
    spark.sql("CREATE CATALOG IF NOT EXISTS nuar_catalog")
    spark.sql("CREATE SCHEMA IF NOT EXISTS nuar_catalog.bronze")
    print("✅ Catalog and schema ready")
    print()

    # Register tables
    for domain in csv_files.keys():
        if results.get(domain):
            delta_path = f"{BRONZE_BASE}/{domain}"
            table_name = f"nuar_catalog.bronze.{domain}"

            try:
                # Drop table if exists
                spark.sql(f"DROP TABLE IF EXISTS {table_name}")

                # Create table
                spark.sql(f"""
                    CREATE TABLE {table_name}
                    USING DELTA
                    LOCATION '{delta_path}'
                """)
                print(f"✅ Registered: {table_name}")
            except Exception as e:
                print(f"⚠️  {table_name}: {e}")

except Exception as e:
    print(f"⚠️  Catalog creation skipped: {e}")
    print("   (Not a problem if not using Unity Catalog)")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Summary

# COMMAND ----------

print("=" * 80)
print("📊 CONVERSION SUMMARY")
print("=" * 80)
print()

for domain, success in results.items():
    status = "✅" if success else "❌"
    print(f"{status} {domain}")

print()

success_count = sum(1 for v in results.values() if v)
total_count = len(results)

print(f"Total: {success_count}/{total_count} successful")
print()

if success_count == total_count:
    print("🎉 All CSV files converted to Delta tables!")
    print()
    print("Data location: dbfs:/FileStore/nuar/bronze/stockport/")
    print()
    print("Catalog tables:")
    for domain in csv_files.keys():
        if results.get(domain):
            print(f"   ✅ nuar_catalog.bronze.{domain}")
    print()
    print("Next steps:")
    print("  1. Run Silver layer notebooks:")
    print("     - databricks_notebooks/silver/05_silver_infrastructure.py")
    print("     - databricks_notebooks/silver/06_silver_crime.py")
    print("     - databricks_notebooks/silver/07_silver_weather.py")
elif success_count > 0:
    print("⚠️  Partial conversion")
else:
    print("❌ Conversion failed")

print()
print(f"⏰ Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Data

# COMMAND ----------

print("🔍 Verifying Bronze tables...")
print()

for domain in csv_files.keys():
    if results.get(domain):
        table_name = f"nuar_catalog.bronze.{domain}"

        try:
            df = spark.table(table_name)
            count = df.count()
            columns = len(df.columns)

            print(f"✅ {domain}: {count} records, {columns} columns")

        except Exception as e:
            print(f"❌ {domain}: {e}")

print()
print("✅ Bronze layer ready!")
