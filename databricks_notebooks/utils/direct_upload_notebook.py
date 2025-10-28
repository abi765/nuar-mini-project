# Databricks notebook source
# MAGIC %md
# MAGIC # Direct Upload - Bronze Data to DBFS
# MAGIC
# MAGIC **Purpose**: Upload Bronze data directly from notebook without using zip
# MAGIC
# MAGIC **Instructions**:
# MAGIC 1. Upload this notebook to Databricks
# MAGIC 2. Upload your bronze_data.zip to /tmp/ using the widget below
# MAGIC 3. Run all cells

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload Method: Use dbutils.fs.put() with File Upload Widget
# MAGIC
# MAGIC Since you can't access DBFS File Browser, we'll use a different approach:
# MAGIC
# MAGIC ### Method: Use Notebook File Upload
# MAGIC
# MAGIC Run this cell and use the file upload widget that appears

# COMMAND ----------

# This creates a file upload widget in the notebook
dbutils.widgets.text("zip_path", "/FileStore/bronze_data.zip", "Uploaded Zip Path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Upload Your Zip File
# MAGIC
# MAGIC **How to upload**:
# MAGIC
# MAGIC 1. In this notebook, go to **File** menu → **Upload Data**
# MAGIC 2. Or click the **Upload** button in notebook toolbar
# MAGIC 3. Select `bronze_data.zip` from your computer
# MAGIC 4. It will upload to: `/FileStore/` or `/FileStore/tables/`
# MAGIC 5. Copy the path shown and update the widget above

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Use These Databricks UI Methods
# MAGIC
# MAGIC ### Method A: Import Data Menu
# MAGIC 1. In Databricks, click **Create** → **Table**
# MAGIC 2. Click **Upload File**
# MAGIC 3. Upload bronze_data.zip
# MAGIC 4. Note the path (usually `/FileStore/tables/bronze_data.zip`)
# MAGIC
# MAGIC ### Method B: Notebook Menu
# MAGIC 1. In any notebook, click **File** → **Upload Data to DBFS**
# MAGIC 2. Upload bronze_data.zip
# MAGIC 3. Copy the resulting path

# COMMAND ----------

import os
import shutil
from pathlib import Path
import zipfile

# Get the uploaded path from widget
zip_path_input = dbutils.widgets.get("zip_path")

# Try common upload locations
possible_paths = [
    f"/dbfs{zip_path_input}",
    f"/dbfs/FileStore/bronze_data.zip",
    f"/dbfs/FileStore/tables/bronze_data.zip",
    "/dbfs/tmp/bronze_data.zip"
]

print("🔍 Searching for uploaded zip file...")
print()

zip_path = None
for path in possible_paths:
    if os.path.exists(path):
        zip_path = path
        zip_size = os.path.getsize(path) / (1024 * 1024)  # MB
        print(f"✅ Found: {path}")
        print(f"   Size: {zip_size:.2f} MB")
        break
    else:
        print(f"❌ Not found: {path}")

print()

if not zip_path:
    print("=" * 80)
    print("⚠️  ZIP FILE NOT FOUND")
    print("=" * 80)
    print()
    print("Please upload bronze_data.zip using one of these methods:")
    print()
    print("Method 1 - Create Table UI:")
    print("  1. Click 'Create' → 'Table' in Databricks")
    print("  2. Click 'Upload File'")
    print("  3. Select bronze_data.zip")
    print("  4. Cancel table creation after upload")
    print("  5. Path will be: /FileStore/tables/bronze_data.zip")
    print()
    print("Method 2 - Notebook File Menu:")
    print("  1. In notebook: File → Upload Data to DBFS")
    print("  2. Upload bronze_data.zip")
    print("  3. Note the path shown")
    print()
    print("Method 3 - Manual dbutils command:")
    print("  Upload via browser, then run:")
    print("  dbutils.fs.ls('dbfs:/FileStore/')")
    print()
    dbutils.notebook.exit("ERROR: Zip file not found. Please upload first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Extract and Organize Data

# COMMAND ----------

print("=" * 80)
print("📦 EXTRACTING BRONZE DATA")
print("=" * 80)
print()

# Configuration
EXTRACT_TO = "/dbfs/FileStore/nuar/bronze/stockport"
TEMP_EXTRACT = "/tmp/bronze_extraction"

# Clean temp directory
if os.path.exists(TEMP_EXTRACT):
    shutil.rmtree(TEMP_EXTRACT)
os.makedirs(TEMP_EXTRACT, exist_ok=True)

# Extract zip
print(f"📂 Extracting to temporary location...")
try:
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(TEMP_EXTRACT)
    print(f"   ✅ Extracted to: {TEMP_EXTRACT}")
    print()
except Exception as e:
    print(f"   ❌ Extraction failed: {e}")
    dbutils.notebook.exit("ERROR: Failed to extract zip")

# COMMAND ----------

# Find and copy data domains
print("📤 Copying data to DBFS...")
print(f"   Target: {EXTRACT_TO}")
print()

domains = ['infrastructure', 'crime', 'weather', 'postcodes']
results = {}

for domain in domains:
    # Try to find source directory
    possible_sources = [
        os.path.join(TEMP_EXTRACT, 'data', 'bronze', 'stockport', domain),
        os.path.join(TEMP_EXTRACT, 'stockport', domain),
        os.path.join(TEMP_EXTRACT, domain),
    ]

    source_path = None
    for src in possible_sources:
        if os.path.exists(src):
            source_path = src
            break

    if not source_path:
        print(f"⚠️  {domain}: Source not found")
        results[domain] = False
        continue

    target_path = os.path.join(EXTRACT_TO, domain)

    print(f"📦 {domain}")
    print(f"   Source: {source_path}")
    print(f"   Target: {target_path}")

    try:
        # Create target directory
        os.makedirs(target_path, exist_ok=True)

        # Copy all files
        for item in os.listdir(source_path):
            s = os.path.join(source_path, item)
            d = os.path.join(target_path, item)

            if os.path.isdir(s):
                shutil.copytree(s, d, dirs_exist_ok=True)
            else:
                shutil.copy2(s, d)

        # Count files
        file_count = sum(1 for _ in Path(target_path).rglob('*') if _.is_file())
        print(f"   ✅ Copied {file_count} files")
        results[domain] = True

    except Exception as e:
        print(f"   ❌ Error: {e}")
        results[domain] = False

    print()

# COMMAND ----------

# Cleanup
print("🧹 Cleaning up temporary files...")
try:
    shutil.rmtree(TEMP_EXTRACT)
    print("   ✅ Cleanup complete")
except Exception as e:
    print(f"   ⚠️  Cleanup warning: {e}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Summary and Verification

# COMMAND ----------

print("=" * 80)
print("📊 UPLOAD SUMMARY")
print("=" * 80)
print()

success_count = sum(1 for v in results.values() if v)
total_count = len(results)

for domain, success in results.items():
    status = "✅" if success else "❌"
    print(f"{status} {domain}")

print()
print(f"Total: {success_count}/{total_count} successful")
print()

if success_count == total_count:
    print("🎉 All data uploaded successfully!")
    print()
    print("Data location: dbfs:/FileStore/nuar/bronze/stockport/")
    print()
    print("Verify with:")
    for domain in results.keys():
        print(f"  dbutils.fs.ls('dbfs:/FileStore/nuar/bronze/stockport/{domain}/')")
    print()
    print("Next steps:")
    print("  1. Run Silver layer notebooks:")
    print("     - databricks_notebooks/silver/05_silver_infrastructure.py")
    print("     - databricks_notebooks/silver/06_silver_crime.py")
    print("     - databricks_notebooks/silver/07_silver_weather.py")
elif success_count > 0:
    print("⚠️  Partial upload")
else:
    print("❌ Upload failed")

print()
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Verification Commands
# MAGIC
# MAGIC Run these to verify your data:

# COMMAND ----------

# Check main directory
display(dbutils.fs.ls('dbfs:/FileStore/nuar/bronze/stockport/'))

# COMMAND ----------

# Check infrastructure files
display(dbutils.fs.ls('dbfs:/FileStore/nuar/bronze/stockport/infrastructure/'))

# COMMAND ----------

# Check a parquet file
display(dbutils.fs.ls('dbfs:/FileStore/nuar/bronze/stockport/infrastructure/parquet/'))
