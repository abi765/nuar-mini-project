# Databricks Network Restriction Workaround

## Problem Summary

Your Databricks workspace has these restrictions:
- ❌ Cannot reach external APIs (Overpass, Police, Weather, Postcodes)
- ❌ CLI `databricks fs cp` authorization fails (403)
- ❌ REST API `/dbfs/put` authorization fails (403)
- ❌ UI only accepts CSV/JSON/TXT (not ZIP files)

## ✅ WORKING SOLUTION

Since all upload methods are blocked, use **Delta Lake tables directly from local data**:

### Step 1: Load Data to Delta Tables Locally

Run this command on your local machine:

```bash
python load_to_delta_locally.py
```

This will:
1. Read your Bronze parquet files
2. Write them as Delta tables
3. Create importable Delta transaction logs

### Step 2: Upload Delta Tables via Databricks SQL

In Databricks, run SQL commands to create external tables pointing to uploaded data.

---

## Alternative: Manual File-by-File Upload

If you need to upload files one-by-one:

### Upload Parquet Files via UI

1. In Databricks, go to **Data** → **Create Table**
2. Upload each parquet file individually:
   - `infrastructure` parquet files (4 files)
   - `crime` parquet files (8 files)
   - `weather` parquet files (1 file)
   - `postcodes` parquet files (4 files)
3. Note the upload paths

### Then Use This Notebook to Consolidate

Run `databricks_notebooks/utils/consolidate_uploaded_files.py` to:
- Find all uploaded parquet files
- Merge them into proper Bronze tables
- Organize into correct structure

---

## RECOMMENDED: Use Sample Data for Testing

Since uploads are blocked, let's test the pipeline with sample/mock data:

```bash
python create_sample_data.py
```

This creates minimal test data that you can:
1. Upload as individual CSVs (supported format)
2. Convert to parquet in Databricks
3. Test your Silver/Gold transformations

---

## Contact Your Databricks Admin

For production use, ask your admin to:
1. **Whitelist external APIs** (if you want to run Bronze collection in Databricks)
2. **Enable DBFS write permissions** (for proper data uploads)
3. **Or provide a mounted storage** (Azure Blob, S3, etc.) you can upload to

---

## What Works Right Now

✅ You **can** run Silver/Gold notebooks if Bronze data exists
✅ You **can** upload CSV/JSON files via UI
✅ You **can** create tables from uploaded files
✅ You **can** run SQL commands

## Next Best Step

Let me create a script that:
1. Converts your parquet files to CSV
2. You upload CSVs via UI (which works)
3. Notebook reads CSVs and creates Delta tables

Would you like me to create that?
