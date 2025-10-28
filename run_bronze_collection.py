#!/usr/bin/env python3
"""
Run all Bronze layer data collection notebooks locally
This script runs the data collection from your local machine where APIs are accessible
"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime

# Notebook order
NOTEBOOKS = [
    "notebooks/01_bronze_infrastructure_full.py",
    "notebooks/02_bronze_crime_full.py",
    "notebooks/03_bronze_weather.py",
    "notebooks/04_bronze_postcodes_full.py"
]

def run_notebook(notebook_path: str) -> bool:
    """
    Run a notebook and return success status

    Args:
        notebook_path: Path to notebook

    Returns:
        True if successful, False otherwise
    """
    print("=" * 80)
    print(f"📓 Running: {notebook_path}")
    print("=" * 80)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    try:
        result = subprocess.run(
            [sys.executable, notebook_path],
            check=True,
            capture_output=False,  # Show output in real-time
            text=True
        )
        print()
        print(f"✅ SUCCESS: {notebook_path}")
        print(f"⏰ Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        return True

    except subprocess.CalledProcessError as e:
        print()
        print(f"❌ FAILED: {notebook_path}")
        print(f"   Error code: {e.returncode}")
        print()
        return False
    except KeyboardInterrupt:
        print()
        print("⚠️  Interrupted by user")
        return False

def main():
    """Run all Bronze collection notebooks"""
    print("=" * 80)
    print("🥉 BRONZE LAYER DATA COLLECTION")
    print("=" * 80)
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print("This will collect data from:")
    print("  1. Overpass API (Infrastructure)")
    print("  2. UK Police API (Crime data)")
    print("  3. OpenWeatherMap API (Weather)")
    print("  4. Postcodes.io API (Postcode reference data)")
    print()
    print("Expected duration: 5-10 minutes")
    print()
    input("Press Enter to start data collection...")
    print()

    # Track results
    results = {}

    # Run each notebook
    for notebook in NOTEBOOKS:
        if not Path(notebook).exists():
            print(f"⚠️  WARNING: Notebook not found: {notebook}")
            results[notebook] = False
            continue

        success = run_notebook(notebook)
        results[notebook] = success

        if not success:
            print()
            print("⚠️  A notebook failed. Do you want to continue with remaining notebooks?")
            response = input("Continue? (y/n): ").lower().strip()
            if response != 'y':
                print("Stopping data collection.")
                break

    # Print summary
    print()
    print("=" * 80)
    print("📊 COLLECTION SUMMARY")
    print("=" * 80)
    print()

    for notebook, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"{status}: {notebook}")

    print()

    success_count = sum(1 for s in results.values() if s)
    total_count = len(results)

    print(f"Total: {success_count}/{total_count} successful")
    print()

    if success_count == total_count:
        print("🎉 All data collection completed successfully!")
        print()
        print("Next steps:")
        print("  1. Run: python upload_to_databricks.py")
        print("  2. Or manually upload data/ folder to DBFS")
        print()
        print("Data location: ./data/bronze/stockport/")
    elif success_count > 0:
        print("⚠️  Some notebooks failed, but you can still upload the successful data")
        print()
        print("Next steps:")
        print("  1. Fix failed notebooks or skip them")
        print("  2. Run: python upload_to_databricks.py")
    else:
        print("❌ No data collected. Please check:")
        print("  - Internet connection")
        print("  - API keys in .env file")
        print("  - Python dependencies installed")

    print()
    print(f"⏰ Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    return success_count == total_count

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
