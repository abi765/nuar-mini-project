#!/usr/bin/env python3
"""
Upload locally collected Bronze data to Databricks DBFS

Prerequisites:
  - Databricks CLI installed: pip install databricks-cli
  - Databricks CLI configured: databricks configure --token
"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime

# Data to upload
DATA_DIRECTORIES = [
    ("data/bronze/stockport/infrastructure", "dbfs:/FileStore/nuar/bronze/stockport/infrastructure"),
    ("data/bronze/stockport/crime", "dbfs:/FileStore/nuar/bronze/stockport/crime"),
    ("data/bronze/stockport/weather", "dbfs:/FileStore/nuar/bronze/stockport/weather"),
    ("data/bronze/stockport/postcodes", "dbfs:/FileStore/nuar/bronze/stockport/postcodes"),
]

def check_databricks_cli():
    """Check if Databricks CLI is installed and configured"""
    print("🔍 Checking Databricks CLI...")

    # Check if installed
    try:
        result = subprocess.run(
            ["databricks", "--version"],
            capture_output=True,
            text=True,
            check=True
        )
        print(f"   ✅ Databricks CLI installed: {result.stdout.strip()}")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("   ❌ Databricks CLI not installed")
        print()
        print("   Install with: pip install databricks-cli")
        print("   Configure with: databricks configure --token")
        print()
        return False

    # Check if configured
    try:
        result = subprocess.run(
            ["databricks", "workspace", "ls", "/"],
            capture_output=True,
            text=True,
            check=True
        )
        print("   ✅ Databricks CLI configured")
        return True
    except subprocess.CalledProcessError:
        print("   ❌ Databricks CLI not configured")
        print()
        print("   Configure with: databricks configure --token")
        print("   You'll need:")
        print("   - Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)")
        print("   - Personal access token (from User Settings > Access Tokens)")
        print()
        return False

def check_local_data():
    """Check which data directories exist locally"""
    print()
    print("📂 Checking local data...")

    available = []
    missing = []

    for local_path, _ in DATA_DIRECTORIES:
        path = Path(local_path)
        if path.exists():
            # Count files
            files = list(path.rglob("*"))
            file_count = len([f for f in files if f.is_file()])
            print(f"   ✅ {local_path} ({file_count} files)")
            available.append((local_path, file_count))
        else:
            print(f"   ❌ {local_path} (not found)")
            missing.append(local_path)

    print()

    if not available:
        print("❌ No data found to upload!")
        print()
        print("Run data collection first: python run_bronze_collection.py")
        return False, []

    if missing:
        print(f"⚠️  {len(missing)} directories missing (will be skipped)")

    return True, available

def upload_directory(local_path: str, dbfs_path: str):
    """
    Upload a local directory to DBFS

    Args:
        local_path: Local directory path
        dbfs_path: DBFS destination path

    Returns:
        True if successful, False otherwise
    """
    print("=" * 80)
    print(f"📤 Uploading: {local_path}")
    print(f"   → {dbfs_path}")
    print("=" * 80)

    try:
        # Create parent directory in DBFS
        parent_dir = "/".join(dbfs_path.split("/")[:-1])
        subprocess.run(
            ["databricks", "fs", "mkdirs", parent_dir],
            check=False,  # Don't fail if already exists
            capture_output=True
        )

        # Upload with recursive flag
        result = subprocess.run(
            ["databricks", "fs", "cp", "-r", "--overwrite", local_path, dbfs_path],
            check=True,
            capture_output=True,
            text=True
        )

        print("✅ Upload successful")
        print()
        return True

    except subprocess.CalledProcessError as e:
        print(f"❌ Upload failed: {e.stderr}")
        print()
        return False

def main():
    """Main upload workflow"""
    print("=" * 80)
    print("📤 UPLOAD BRONZE DATA TO DATABRICKS")
    print("=" * 80)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Check Databricks CLI
    if not check_databricks_cli():
        return False

    # Check local data
    has_data, available = check_local_data()
    if not has_data:
        return False

    print(f"Found {len(available)} directories to upload")
    print(f"Total files: {sum(count for _, count in available)}")
    print()
    print("This will upload data to DBFS at: dbfs:/FileStore/nuar/bronze/stockport/")
    print()

    response = input("Continue with upload? (y/n): ").lower().strip()
    if response != 'y':
        print("Upload cancelled.")
        return False

    print()

    # Upload each directory
    results = {}

    for local_path, dbfs_path in DATA_DIRECTORIES:
        if not Path(local_path).exists():
            print(f"⏭️  Skipping {local_path} (not found)")
            print()
            continue

        success = upload_directory(local_path, dbfs_path)
        results[local_path] = success

    # Print summary
    print("=" * 80)
    print("📊 UPLOAD SUMMARY")
    print("=" * 80)
    print()

    for local_path, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"{status}: {local_path}")

    print()

    success_count = sum(1 for s in results.values() if s)
    total_count = len(results)

    print(f"Total: {success_count}/{total_count} successful")
    print()

    if success_count == total_count:
        print("🎉 All data uploaded successfully!")
        print()
        print("Next steps in Databricks:")
        print("  1. Pull latest code from Git (Repos → Pull)")
        print("  2. Run Silver layer notebooks to transform the data")
        print("  3. Data is now at: dbfs:/FileStore/nuar/bronze/stockport/")
    elif success_count > 0:
        print("⚠️  Some uploads failed")
        print()
        print("You can still proceed with successfully uploaded data")
    else:
        print("❌ No data uploaded")

    print()
    print(f"⏰ Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    return success_count == total_count

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
