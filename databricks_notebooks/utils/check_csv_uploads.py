# Databricks notebook source
# MAGIC %md
# MAGIC # Check CSV Uploads - Debug Missing Data
# MAGIC
# MAGIC **Purpose**: Investigate why some Bronze tables have 0 records

# COMMAND ----------

print("🔍 CHECKING CSV UPLOADS AND DATA")
print("=" * 80)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: List All Files in FileStore

# COMMAND ----------

print("📂 Files in /FileStore/tables/:")
print()

try:
    files = dbutils.fs.ls('dbfs:/FileStore/tables/')
    for file in files:
        size_kb = file.size / 1024
        print(f"   📄 {file.name} ({size_kb:.1f} KB)")
except Exception as e:
    print(f"   ❌ Error: {e}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Try Reading Each CSV

# COMMAND ----------

csv_files = {
    'infrastructure': 'dbfs:/FileStore/tables/infrastructure.csv',
    'crime': 'dbfs:/FileStore/tables/crime.csv',
    'weather': 'dbfs:/FileStore/tables/weather.csv',
    'postcodes': 'dbfs:/FileStore/tables/postcodes.csv'
}

print("📊 Testing CSV files:")
print()

csv_results = {}

for domain, path in csv_files.items():
    print(f"📦 {domain}")
    print(f"   Path: {path}")

    try:
        # Try to read CSV
        df = spark.read.csv(path, header=True, inferSchema=True)
        count = df.count()
        columns = len(df.columns)

        print(f"   ✅ Records: {count}")
        print(f"   ✅ Columns: {columns}")

        if count > 0:
            print(f"   Sample columns: {df.columns[:5]}")
            csv_results[domain] = count
        else:
            print(f"   ⚠️  WARNING: CSV has 0 records!")
            csv_results[domain] = 0

    except Exception as e:
        print(f"   ❌ Error reading CSV: {e}")
        csv_results[domain] = None

    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Check Bronze Delta Tables

# COMMAND ----------

print("📊 Bronze Delta Tables:")
print()

bronze_base = 'dbfs:/FileStore/nuar/bronze/stockport'

for domain in ['infrastructure', 'crime', 'weather', 'postcodes']:
    delta_path = f"{bronze_base}/{domain}"

    print(f"📦 {domain}")
    print(f"   Path: {delta_path}")

    try:
        # Check if path exists
        files = dbutils.fs.ls(delta_path)
        print(f"   ✅ Directory exists ({len(files)} files)")

        # Try reading as Delta
        df = spark.read.format("delta").load(delta_path)
        count = df.count()
        columns = len(df.columns)

        print(f"   Records: {count}")
        print(f"   Columns: {columns}")

    except Exception as e:
        print(f"   ❌ Error: {e}")

    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Summary and Next Steps

# COMMAND ----------

print("=" * 80)
print("📊 DIAGNOSTIC SUMMARY")
print("=" * 80)
print()

print("CSV Upload Status:")
for domain, count in csv_results.items():
    if count is None:
        print(f"   ❌ {domain}: File not found or unreadable")
    elif count == 0:
        print(f"   ⚠️  {domain}: CSV uploaded but empty (0 records)")
    else:
        print(f"   ✅ {domain}: {count} records in CSV")

print()
print("=" * 80)
print()

# Check what went wrong
missing = [d for d, c in csv_results.items() if c is None]
empty = [d for d, c in csv_results.items() if c == 0]
has_data = [d for d, c in csv_results.items() if c and c > 0]

if missing:
    print("❌ MISSING CSV FILES:")
    for domain in missing:
        print(f"   - {domain}.csv was not uploaded")
    print()
    print("Action: Upload these CSV files via Databricks UI (Data → Create Table)")
    print()

if empty:
    print("⚠️  EMPTY CSV FILES:")
    for domain in empty:
        print(f"   - {domain}.csv is empty (0 records)")
    print()
    print("Action: Re-generate CSVs locally:")
    print("   python convert_to_csv_for_upload.py")
    print()

if has_data:
    print("✅ WORKING CSV FILES:")
    for domain in has_data:
        print(f"   - {domain}.csv: {csv_results[domain]} records")
    print()

print()
print("Next steps:")
print("1. Upload any missing CSV files")
print("2. Re-run: csv_to_bronze_tables.py")
print("3. Verify with: fix_table_schema.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Show File Paths for Upload

# COMMAND ----------

import os

print("📁 CSV files on your local machine:")
print()
print("Location: ~/Downloads/PersonalProjects/nuar_mini_project/bronze_csv_for_upload/")
print()
print("Files to upload:")
print("   - infrastructure.csv (62.9 KB)")
print("   - crime.csv (6.9 KB)")
print("   - weather.csv (1.4 KB)")
print("   - postcodes.csv (6.8 KB)")
print()
print("Upload via: Databricks UI → Data → Create Table → Upload File")
