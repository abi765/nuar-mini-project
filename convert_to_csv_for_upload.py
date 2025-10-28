#!/usr/bin/env python3
"""
Convert Bronze parquet files to CSV for Databricks UI upload
Since Databricks UI accepts CSV but not ZIP or parquet directly
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

print("=" * 80)
print("📄 CONVERTING BRONZE DATA TO CSV")
print("=" * 80)
print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# Output directory
output_dir = Path("bronze_csv_for_upload")
output_dir.mkdir(exist_ok=True)

# Domains to convert
domains = {
    'infrastructure': 'data/bronze/stockport/infrastructure/parquet/infrastructure_stockport',
    'crime': 'data/bronze/stockport/crime/parquet/crimes_stockport',
    'weather': 'data/bronze/stockport/weather/parquet/weather_stockport',
    'postcodes': 'data/bronze/stockport/postcodes/parquet/postcodes_stockport',
}

results = {}

for domain, parquet_path in domains.items():
    print(f"📦 Processing: {domain}")
    print(f"   Source: {parquet_path}")

    pq_path = Path(parquet_path)

    if not pq_path.exists():
        print(f"   ⚠️  Not found, skipping")
        results[domain] = False
        print()
        continue

    try:
        # Read all parquet files
        df = pd.read_parquet(pq_path)

        # Output CSV
        csv_file = output_dir / f"{domain}.csv"
        df.to_csv(csv_file, index=False)

        file_size = csv_file.stat().st_size / 1024  # KB
        print(f"   ✅ Created: {csv_file}")
        print(f"   Records: {len(df)}")
        print(f"   Size: {file_size:.1f} KB")
        results[domain] = True

    except Exception as e:
        print(f"   ❌ Error: {e}")
        results[domain] = False

    print()

# Summary
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

print(f"Total: {success_count}/{total_count} converted")
print()

if success_count > 0:
    print("✅ CSV files ready for upload!")
    print()
    print(f"📁 Location: {output_dir.absolute()}/")
    print()
    print("Files created:")
    for csv_file in sorted(output_dir.glob("*.csv")):
        size = csv_file.stat().st_size / 1024
        print(f"   - {csv_file.name} ({size:.1f} KB)")
    print()
    print("=" * 80)
    print("📤 NEXT STEPS")
    print("=" * 80)
    print()
    print("1. In Databricks UI:")
    print("   - Go to Data → Create Table")
    print("   - Upload each CSV file:")
    for domain in results.keys():
        if results[domain]:
            print(f"     • {domain}.csv")
    print()
    print("2. After upload, run this notebook:")
    print("   - databricks_notebooks/utils/csv_to_bronze_tables.py")
    print("   - It will convert CSVs to proper Bronze Delta tables")
    print()
    print("3. Then run Silver layer notebooks normally")
    print()
else:
    print("❌ No files converted")
    print()
    print("Check that Bronze data exists:")
    print("   python run_bronze_collection.py")

print(f"⏰ Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)
