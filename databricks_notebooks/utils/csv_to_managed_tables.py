# Databricks notebook source
# MAGIC %md
# MAGIC # Create Unity Catalog Managed Tables from CSV
# MAGIC
# MAGIC **Purpose**: Create Bronze tables as Unity Catalog managed tables (bypasses DBFS restriction)
# MAGIC
# MAGIC **Why**: Your workspace has "Public DBFS root is disabled" - cannot write to DBFS
# MAGIC
# MAGIC **Solution**: Unity Catalog managed tables store data in your organization's cloud storage

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import os

print("=" * 80)
print("📄 CREATE UNITY CATALOG MANAGED TABLES FROM CSV")
print("=" * 80)
print()
print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup Catalog and Schema

# COMMAND ----------

# Create catalog and schema
print("📊 Setting up Unity Catalog...")
print()

try:
    spark.sql("CREATE CATALOG IF NOT EXISTS nuar_catalog")
    print("✅ Catalog: nuar_catalog")

    spark.sql("CREATE SCHEMA IF NOT EXISTS nuar_catalog.bronze")
    print("✅ Schema: nuar_catalog.bronze")

    # Set as default
    spark.sql("USE CATALOG nuar_catalog")
    spark.sql("USE SCHEMA bronze")
    print("✅ Set as default catalog and schema")

except Exception as e:
    print(f"❌ Error: {e}")
    print()
    print("You may need admin permissions to create catalogs.")
    print("Ask your admin to create: nuar_catalog.bronze")
    dbutils.notebook.exit("ERROR: Cannot create catalog")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Locate CSV Files in Repos

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
    print("❌ Repository not found")
    dbutils.notebook.exit("ERROR: Repository not found")

print(f"✅ Repository: {repo_path}")

csv_folder = f"{repo_path}/bronze_csv_for_upload"
print(f"📁 CSV folder: {csv_folder}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Find CSV Files

# COMMAND ----------

print("🔍 Checking for CSV files...")
print()

csv_files = {}
expected_files = ['infrastructure.csv', 'crime.csv', 'weather.csv', 'postcodes.csv']

for filename in expected_files:
    file_path = os.path.join(csv_folder, filename)
    if os.path.exists(file_path):
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
# MAGIC ## Step 4: Create Managed Tables (No DBFS Location)

# COMMAND ----------

print("📦 Creating Unity Catalog managed tables...")
print()

results = {}

for domain, csv_path in csv_files.items():
    print("=" * 80)
    print(f"📦 Creating: {domain}")
    print(f"   Source: {csv_path}")
    print("=" * 80)

    try:
        # Read CSV using Pandas
        import pandas as pd
        pdf = pd.read_csv(csv_path)
        record_count = len(pdf)
        print(f"   Records: {record_count}")

        # Convert to Spark DataFrame
        df = spark.createDataFrame(pdf)

        # Table name (managed table - NO LOCATION clause)
        table_name = f"nuar_catalog.bronze.{domain}"
        print(f"   Target: {table_name}")

        # Drop table if exists
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

        # Create managed table (Unity Catalog handles storage)
        df.write.format("delta") \
            .mode("overwrite") \
            .saveAsTable(table_name)

        print(f"   ✅ Created managed table")
        results[domain] = True

    except Exception as e:
        print(f"   ❌ Error: {e}")
        results[domain] = False

    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Summary

# COMMAND ----------

print("=" * 80)
print("📊 TABLE CREATION SUMMARY")
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
    print("🎉 All tables created successfully!")
    print()
    print("Tables created (Unity Catalog managed):")
    for domain in csv_files.keys():
        if results.get(domain):
            print(f"   ✅ nuar_catalog.bronze.{domain}")
    print()
    print("Storage: Managed by Unity Catalog (not in DBFS)")
    print()
    print("Next steps:")
    print("  1. Verify tables: SELECT * FROM nuar_catalog.bronze.infrastructure LIMIT 10")
    print("  2. Run Silver layer notebooks")
elif success_count > 0:
    print("⚠️  Partial creation")
else:
    print("❌ Table creation failed")

print()
print(f"⏰ Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Tables

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

            # Show sample
            print(f"   Sample data:")
            df.select(df.columns[:3]).show(2, truncate=False)

        except Exception as e:
            print(f"❌ {domain}: {e}")

print()
print("✅ Bronze layer ready!")
print()
print("Note: Tables are stored in Unity Catalog managed storage, NOT in DBFS")
