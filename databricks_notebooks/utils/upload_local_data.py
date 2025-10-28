# Databricks notebook source
# MAGIC %md
# MAGIC # Upload Local Bronze Data to DBFS
# MAGIC
# MAGIC **Purpose**: Upload locally collected Bronze data files to DBFS
# MAGIC
# MAGIC **When to use**:
# MAGIC - When Databricks cluster cannot access external APIs due to network restrictions
# MAGIC - After running local data collection scripts
# MAGIC
# MAGIC **Instructions**:
# MAGIC 1. Collect data locally: `python run_bronze_collection.py`
# MAGIC 2. Zip the data folder locally
# MAGIC 3. Upload the zip via Databricks UI (Data → Upload File)
# MAGIC 4. Run this notebook to extract and organize the data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Upload Your Local Data
# MAGIC
# MAGIC Before running this notebook:
# MAGIC
# MAGIC 1. **On your local machine**, zip your Bronze data:
# MAGIC    ```bash
# MAGIC    cd ~/Downloads/PersonalProjects/nuar_mini_project
# MAGIC    zip -r bronze_data.zip data/bronze/stockport/
# MAGIC    ```
# MAGIC
# MAGIC 2. **In Databricks UI**:
# MAGIC    - Go to **Data** → **Upload File**
# MAGIC    - Upload `bronze_data.zip`
# MAGIC    - It will go to: `dbfs:/FileStore/` or `dbfs:/FileStore/tables/`
# MAGIC
# MAGIC 3. **Update the path below** to match where your zip was uploaded

# COMMAND ----------

import os
import shutil
from pathlib import Path

# Configuration
UPLOADED_ZIP_PATH = "/dbfs/FileStore/bronze_data.zip"  # Update this path after upload
EXTRACT_TO = "/dbfs/FileStore/nuar/bronze/stockport"
TEMP_EXTRACT = "/tmp/bronze_extraction"

print("=" * 80)
print("📦 BRONZE DATA EXTRACTION AND UPLOAD")
print("=" * 80)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verify Uploaded Zip File

# COMMAND ----------

print("🔍 Step 1: Checking for uploaded zip file...")
print(f"   Looking for: {UPLOADED_ZIP_PATH}")
print()

if os.path.exists(UPLOADED_ZIP_PATH):
    zip_size = os.path.getsize(UPLOADED_ZIP_PATH) / (1024 * 1024)  # MB
    print(f"   ✅ Found: {UPLOADED_ZIP_PATH}")
    print(f"   Size: {zip_size:.2f} MB")
    print()
else:
    print(f"   ❌ Not found: {UPLOADED_ZIP_PATH}")
    print()
    print("   Please:")
    print("   1. Zip your local data: zip -r bronze_data.zip data/bronze/stockport/")
    print("   2. Upload via Databricks UI: Data → Upload File")
    print("   3. Update UPLOADED_ZIP_PATH variable above with the actual path")
    print("   4. Common paths:")
    print("      - dbfs:/FileStore/bronze_data.zip")
    print("      - dbfs:/FileStore/tables/bronze_data.zip")
    print()
    dbutils.notebook.exit("ERROR: Zip file not found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Extract and Organize Data

# COMMAND ----------

print("📂 Step 2: Extracting data...")
print()

# Clean temp directory if exists
if os.path.exists(TEMP_EXTRACT):
    shutil.rmtree(TEMP_EXTRACT)
os.makedirs(TEMP_EXTRACT, exist_ok=True)

# Extract zip
import zipfile

try:
    with zipfile.ZipFile(UPLOADED_ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(TEMP_EXTRACT)
    print(f"   ✅ Extracted to: {TEMP_EXTRACT}")
    print()
except Exception as e:
    print(f"   ❌ Extraction failed: {e}")
    dbutils.notebook.exit("ERROR: Failed to extract zip")

# COMMAND ----------

print("📁 Step 3: Listing extracted contents...")
print()

# Find the data directory (handle different zip structures)
extracted_files = []
for root, dirs, files in os.walk(TEMP_EXTRACT):
    for file in files:
        full_path = os.path.join(root, file)
        rel_path = os.path.relpath(full_path, TEMP_EXTRACT)
        extracted_files.append(rel_path)

print(f"   Found {len(extracted_files)} files")
print()

# Show first 10 files
print("   Sample files:")
for f in extracted_files[:10]:
    print(f"   - {f}")
if len(extracted_files) > 10:
    print(f"   ... and {len(extracted_files) - 10} more")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Copy to Final DBFS Location

# COMMAND ----------

print("📤 Step 4: Copying to DBFS...")
print(f"   Target: {EXTRACT_TO}")
print()

# Find source directory (might be data/bronze/stockport or just stockport)
source_dirs = []
for domain in ['infrastructure', 'crime', 'weather', 'postcodes']:
    # Try different possible paths
    possible_paths = [
        os.path.join(TEMP_EXTRACT, 'data', 'bronze', 'stockport', domain),
        os.path.join(TEMP_EXTRACT, 'bronze', 'stockport', domain),
        os.path.join(TEMP_EXTRACT, 'stockport', domain),
        os.path.join(TEMP_EXTRACT, domain),
    ]

    for path in possible_paths:
        if os.path.exists(path):
            source_dirs.append((domain, path))
            break

if not source_dirs:
    print("   ❌ Could not find data directories in extracted files")
    print()
    print("   Extracted structure:")
    for root, dirs, files in os.walk(TEMP_EXTRACT):
        level = root.replace(TEMP_EXTRACT, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f'{indent}{os.path.basename(root)}/')
        if level < 3:  # Only show first 3 levels
            subindent = ' ' * 2 * (level + 1)
            for file in files[:3]:
                print(f'{subindent}{file}')
    dbutils.notebook.exit("ERROR: Cannot find data directories")

print(f"   Found {len(source_dirs)} data domains")
print()

# Copy each domain
results = {}

for domain, source_path in source_dirs:
    target_path = os.path.join(EXTRACT_TO, domain)

    print(f"   📦 {domain}")
    print(f"      Source: {source_path}")
    print(f"      Target: {target_path}")

    try:
        # Create target directory
        os.makedirs(target_path, exist_ok=True)

        # Copy files
        if os.path.exists(source_path):
            # Use shutil to copy directory contents
            for item in os.listdir(source_path):
                s = os.path.join(source_path, item)
                d = os.path.join(target_path, item)
                if os.path.isdir(s):
                    shutil.copytree(s, d, dirs_exist_ok=True)
                else:
                    shutil.copy2(s, d)

            # Count files
            file_count = sum(1 for _ in Path(target_path).rglob('*') if _.is_file())
            print(f"      ✅ Copied {file_count} files")
            results[domain] = True
        else:
            print(f"      ⚠️  Source not found")
            results[domain] = False

    except Exception as e:
        print(f"      ❌ Error: {e}")
        results[domain] = False

    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Cleanup and Summary

# COMMAND ----------

print("🧹 Step 5: Cleaning up...")
print()

# Remove temp directory
try:
    shutil.rmtree(TEMP_EXTRACT)
    print("   ✅ Temporary files removed")
except Exception as e:
    print(f"   ⚠️  Cleanup warning: {e}")

print()

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
    print("Next steps:")
    print("  1. Verify data with: dbutils.fs.ls('dbfs:/FileStore/nuar/bronze/stockport/')")
    print("  2. Run Silver layer transformation notebooks")
    print()
    print("Available domains:")
    for domain in results.keys():
        print(f"  - {domain}: dbfs:/FileStore/nuar/bronze/stockport/{domain}/")
elif success_count > 0:
    print("⚠️  Some domains failed to upload")
    print()
    print("Successfully uploaded:")
    for domain, success in results.items():
        if success:
            print(f"  - {domain}")
else:
    print("❌ Upload failed")
    print()
    print("Please check:")
    print("  - Zip file structure")
    print("  - File permissions")
    print("  - DBFS storage space")

print()
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Upload

# COMMAND ----------

print("🔍 Verifying uploaded data...")
print()

try:
    for domain in ['infrastructure', 'crime', 'weather', 'postcodes']:
        path = f"dbfs:/FileStore/nuar/bronze/stockport/{domain}"
        try:
            files = dbutils.fs.ls(path)
            print(f"✅ {domain}: {len(files)} items")
        except Exception as e:
            print(f"❌ {domain}: Not found or empty")

    print()
    print("Use this to browse data:")
    print("   dbutils.fs.ls('dbfs:/FileStore/nuar/bronze/stockport/infrastructure/')")

except Exception as e:
    print(f"⚠️  Verification error: {e}")

print()
print("✅ Upload process complete!")
