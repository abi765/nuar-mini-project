#!/usr/bin/env python3
"""
Upload Bronze data to Databricks using REST API
Works when CLI doesn't support fs cp and UI doesn't accept zip files
"""

import os
import sys
import base64
import requests
from pathlib import Path
from datetime import datetime

# Read Databricks config
config_path = Path.home() / ".databrickscfg"
if not config_path.exists():
    print("❌ Databricks CLI not configured")
    print("   Run: databricks configure --token")
    sys.exit(1)

# Parse config
with open(config_path) as f:
    lines = f.readlines()
    host = None
    token = None
    for line in lines:
        if line.startswith("host"):
            host = line.split("=")[1].strip()
        elif line.startswith("token"):
            token = line.split("=")[1].strip()

if not host or not token:
    print("❌ Could not read Databricks config")
    sys.exit(1)

print("=" * 80)
print("📤 UPLOADING BRONZE DATA VIA REST API")
print("=" * 80)
print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()
print(f"Host: {host}")
print(f"Token: {token[:10]}...")
print()

# API endpoints
DBFS_API_URL = f"{host}/api/2.0/dbfs"

def upload_file(local_path: str, dbfs_path: str) -> bool:
    """
    Upload a single file to DBFS using REST API

    Args:
        local_path: Local file path
        dbfs_path: DBFS destination path (without dbfs: prefix)

    Returns:
        True if successful, False otherwise
    """
    try:
        # Read file content
        with open(local_path, 'rb') as f:
            content = f.read()

        # Base64 encode
        content_b64 = base64.b64encode(content).decode('utf-8')

        # Upload via API
        response = requests.post(
            f"{DBFS_API_URL}/put",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
            json={
                "path": dbfs_path,
                "contents": content_b64,
                "overwrite": True
            },
            timeout=60
        )

        if response.status_code == 200:
            return True
        else:
            print(f"   ❌ API Error: {response.status_code} - {response.text[:100]}")
            return False

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

def upload_directory(local_dir: str, dbfs_base: str) -> tuple:
    """
    Upload all files in a directory recursively

    Args:
        local_dir: Local directory path
        dbfs_base: DBFS base path

    Returns:
        Tuple of (success_count, total_count)
    """
    local_path = Path(local_dir)

    if not local_path.exists():
        print(f"   ⚠️  Directory not found: {local_dir}")
        return (0, 0)

    # Get all files
    files = [f for f in local_path.rglob('*') if f.is_file()]

    if not files:
        print(f"   ⚠️  No files found in: {local_dir}")
        return (0, 0)

    print(f"   Found {len(files)} files")

    success_count = 0

    for file_path in files:
        # Calculate relative path
        rel_path = file_path.relative_to(local_path)
        dbfs_path = f"{dbfs_base}/{rel_path}".replace("\\", "/")

        # Show progress
        print(f"   📄 {rel_path} ", end="", flush=True)

        if upload_file(str(file_path), dbfs_path):
            print("✅")
            success_count += 1
        else:
            print("❌")

    return (success_count, len(files))

def main():
    """Upload all Bronze data domains"""

    # Data to upload
    domains = [
        ("data/bronze/stockport/infrastructure", "/FileStore/nuar/bronze/stockport/infrastructure"),
        ("data/bronze/stockport/crime", "/FileStore/nuar/bronze/stockport/crime"),
        ("data/bronze/stockport/weather", "/FileStore/nuar/bronze/stockport/weather"),
        ("data/bronze/stockport/postcodes", "/FileStore/nuar/bronze/stockport/postcodes"),
    ]

    # Check if data exists
    print("📂 Checking local data...")
    print()

    available = []
    for local_path, dbfs_path in domains:
        if Path(local_path).exists():
            file_count = len([f for f in Path(local_path).rglob('*') if f.is_file()])
            print(f"   ✅ {local_path} ({file_count} files)")
            available.append((local_path, dbfs_path, file_count))
        else:
            print(f"   ❌ {local_path} (not found)")

    print()

    if not available:
        print("❌ No data found to upload!")
        print()
        print("Run data collection first: python run_bronze_collection.py")
        return False

    total_files = sum(count for _, _, count in available)
    print(f"Ready to upload {len(available)} domains ({total_files} files)")
    print()
    print("Starting upload...")
    print()

    # Upload each domain
    results = {}

    for local_path, dbfs_path, _ in available:
        domain = Path(local_path).name

        print("=" * 80)
        print(f"📤 Uploading: {domain}")
        print(f"   Local:  {local_path}")
        print(f"   DBFS:   {dbfs_path}")
        print("=" * 80)

        success, total = upload_directory(local_path, dbfs_path)
        results[domain] = (success, total)

        print(f"   Result: {success}/{total} files uploaded")
        print()

    # Summary
    print("=" * 80)
    print("📊 UPLOAD SUMMARY")
    print("=" * 80)
    print()

    for domain, (success, total) in results.items():
        status = "✅" if success == total else "⚠️" if success > 0 else "❌"
        print(f"{status} {domain}: {success}/{total} files")

    print()

    total_success = sum(s for s, _ in results.values())
    total_files = sum(t for _, t in results.values())

    print(f"Total: {total_success}/{total_files} files uploaded")
    print()

    if total_success == total_files:
        print("🎉 All files uploaded successfully!")
        print()
        print("Data location: dbfs:/FileStore/nuar/bronze/stockport/")
        print()
        print("Next steps in Databricks:")
        print("  1. Verify: dbutils.fs.ls('dbfs:/FileStore/nuar/bronze/stockport/')")
        print("  2. Run Silver layer notebooks")
    elif total_success > 0:
        print("⚠️  Partial upload - some files failed")
    else:
        print("❌ Upload failed")

    print()
    print(f"⏰ Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    return total_success == total_files

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Upload interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        sys.exit(1)
