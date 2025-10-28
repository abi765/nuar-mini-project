# Databricks notebook source
# MAGIC %md
# MAGIC # Convert Uploaded CSVs to Bronze Delta Tables
# MAGIC
# MAGIC **Purpose**: Convert CSV files uploaded via UI to proper Bronze Delta Lake tables
# MAGIC
# MAGIC **Prerequisites**:
# MAGIC 1. Upload CSV files via Databricks UI (Data → Create Table):
# MAGIC    - infrastructure.csv
# MAGIC    - crime.csv
# MAGIC    - weather.csv
# MAGIC    - postcodes.csv
# MAGIC 2. Note the upload paths (usually /FileStore/tables/)
# MAGIC 3. Update the paths in this notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# UPDATE THESE PATHS after uploading your CSV files
CSV_PATHS = {
    'infrastructure': '/FileStore/tables/infrastructure.csv',
    'crime': '/FileStore/tables/crime.csv',
    'weather': '/FileStore/tables/weather.csv',
    'postcodes': '/FileStore/tables/postcodes.csv'
}

# Bronze Delta table locations
BRONZE_BASE = 'dbfs:/FileStore/nuar/bronze/stockport'

print("=" * 80)
print("📄 CSV TO BRONZE DELTA CONVERSION")
print("=" * 80)
print()
print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Verify CSV Files

# COMMAND ----------

print("🔍 Checking for uploaded CSV files...")
print()

available = {}

for domain, csv_path in CSV_PATHS.items():
    dbfs_path = csv_path.replace('/FileStore/', 'dbfs:/FileStore/')

    try:
        # Try to list the file
        files = dbutils.fs.ls(dbfs_path)
        print(f"✅ {domain}: {csv_path}")
        available[domain] = csv_path
    except:
        print(f"❌ {domain}: {csv_path} (not found)")

print()

if not available:
    print("❌ No CSV files found!")
    print()
    print("Please:")
    print("1. Run locally: python convert_to_csv_for_upload.py")
    print("2. Upload CSV files via Databricks UI (Data → Create Table)")
    print("3. Update CSV_PATHS in this notebook with actual upload paths")
    print()
    dbutils.notebook.exit("ERROR: No CSV files found")

print(f"Found {len(available)}/{len(CSV_PATHS)} CSV files")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Convert Each CSV to Delta Table

# COMMAND ----------

results = {}

for domain, csv_path in available.items():
    print("=" * 80)
    print(f"📦 Converting: {domain}")
    print(f"   Source: {csv_path}")
    print("=" * 80)

    try:
        # Read CSV
        df = spark.read.csv(csv_path, header=True, inferSchema=True)

        record_count = df.count()
        print(f"   Records: {record_count}")

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
# MAGIC ## Step 3: Create Catalog Tables (Optional)

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
    for domain in available.keys():
        if results.get(domain):
            delta_path = f"{BRONZE_BASE}/{domain}"
            table_name = f"nuar_catalog.bronze.{domain}"

            try:
                spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {table_name}
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
# MAGIC ## Step 4: Summary

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
# MAGIC ## Step 5: Verify Data

# COMMAND ----------

print("🔍 Verifying Bronze data...")
print()

for domain in available.keys():
    if results.get(domain):
        delta_path = f"{BRONZE_BASE}/{domain}"

        try:
            df = spark.read.format("delta").load(delta_path)
            count = df.count()
            columns = len(df.columns)
            print(f"✅ {domain}: {count} records, {columns} columns")

            # Show sample
            print(f"   Sample:")
            df.select(df.columns[:5]).show(3, truncate=False)

        except Exception as e:
            print(f"❌ {domain}: {e}")

print()
print("✅ Bronze layer ready!")
