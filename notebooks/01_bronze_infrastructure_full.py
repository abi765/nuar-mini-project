"""
Bronze Layer: Full Infrastructure Data Ingestion for Stockport
Collects ALL infrastructure data from Overpass API and saves to Bronze layer
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Import utilities
from src.utils.api_client import OverpassClient, add_metadata
from src.bronze.ingestion import (
    flatten_overpass_elements,
    save_to_bronze_parquet,
    save_to_bronze_json,
    create_bronze_summary
)

print("="*80)
print("🥉 BRONZE LAYER: Stockport Infrastructure Data Ingestion")
print("="*80)
print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# ============================================================================
# CONFIGURATION
# ============================================================================

# Load Stockport configuration
with open(project_root / 'config' / 'stockport.json') as f:
    config = json.load(f)

STOCKPORT = config['stockport']
BBOX = STOCKPORT['bounding_box']

print("📍 Target Area:")
print(f"   Name: {STOCKPORT['name']}")
print(f"   Council: {STOCKPORT['council']}")
print(f"   Bounding Box: {BBOX}")
print(f"   Area: {STOCKPORT['area_km2']} km²")
print()

# Infrastructure types to collect
INFRASTRUCTURE_TYPES = [
    'manhole',
    'pipeline',
    'power_cable',
    'cable',
    'duct',
    'utility_pole'
]

print("🏗️  Infrastructure Types:")
for i_type in INFRASTRUCTURE_TYPES:
    print(f"   - {i_type}")
print()

# Output directories
BRONZE_DIR = project_root / 'data' / 'bronze' / 'stockport' / 'infrastructure'
os.makedirs(BRONZE_DIR, exist_ok=True)

print(f"💾 Output Directory: {BRONZE_DIR}")
print()

# ============================================================================
# DATA COLLECTION
# ============================================================================

print("="*80)
print("🔍 COLLECTING INFRASTRUCTURE DATA")
print("="*80)
print()

# Initialize Overpass client
client = OverpassClient(timeout=300)  # 5 minute timeout

# Query ALL infrastructure types
print("⏳ Querying Overpass API (this may take 2-5 minutes)...")
print()

result = client.query_infrastructure(BBOX, INFRASTRUCTURE_TYPES)

if not result:
    print("\n❌ FAILED: Could not retrieve data from Overpass API")
    print("   Possible reasons:")
    print("   - API timeout (area too large)")
    print("   - Network issues")
    print("   - API unavailable")
    print("\n💡 Suggestion: Try again or reduce bounding box size")
    sys.exit(1)

# Extract elements
elements = result.get('elements', [])
timestamp = result.get('osm3s', {}).get('timestamp_osm_base', 'N/A')

print()
print(f"✅ Successfully retrieved {len(elements)} infrastructure elements")
print(f"📊 OSM Data Timestamp: {timestamp}")
print()

if len(elements) == 0:
    print("⚠️  WARNING: No infrastructure elements found in this area")
    print("   This could mean:")
    print("   - Limited OSM data for Stockport")
    print("   - Incorrect bounding box")
    print("   - Infrastructure not tagged in OSM")
    sys.exit(0)

# ============================================================================
# ANALYZE DATA
# ============================================================================

print("="*80)
print("📊 DATA ANALYSIS")
print("="*80)
print()

# Count by type
type_counts = {}
for elem in elements:
    elem_type = elem.get('type')
    tags = elem.get('tags', {})
    infra_type = tags.get('man_made') or tags.get('power') or 'unknown'
    
    key = f"{elem_type}:{infra_type}"
    type_counts[key] = type_counts.get(key, 0) + 1

print("Infrastructure breakdown:")
for key, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"   {key:30} {count:5}")
print()

# Count nodes vs ways
nodes = sum(1 for e in elements if e.get('type') == 'node')
ways = sum(1 for e in elements if e.get('type') == 'way')
print(f"Geometry types:")
print(f"   Nodes (point features):  {nodes}")
print(f"   Ways (line features):    {ways}")
print()

# ============================================================================
# SAVE RAW DATA (JSON)
# ============================================================================

print("="*80)
print("💾 SAVING RAW DATA")
print("="*80)
print()

print("1️⃣  Saving raw JSON response...")

# Add metadata to result
result_with_metadata = add_metadata(
    result,
    source='overpass_api',
    area='stockport'
)

json_path = save_to_bronze_json(
    result_with_metadata,
    str(BRONZE_DIR / 'raw_json'),
    'overpass_full_response'
)

if json_path:
    print(f"   ✅ Raw JSON saved")
else:
    print(f"   ⚠️  Failed to save raw JSON")
print()

# ============================================================================
# FLATTEN AND SAVE AS PARQUET
# ============================================================================

print("2️⃣  Flattening data structure...")

flattened_data = flatten_overpass_elements(elements)
print(f"   ✅ Flattened {len(flattened_data)} records")
print()

# Add metadata columns
for record in flattened_data:
    record['ingestion_timestamp'] = datetime.now().isoformat()
    record['ingestion_date'] = datetime.now().strftime('%Y-%m-%d')
    record['source_api'] = 'overpass'
    record['area'] = 'stockport'
    record['osm_timestamp'] = timestamp

print("3️⃣  Saving as Parquet...")

parquet_path = save_to_bronze_parquet(
    flattened_data,
    str(BRONZE_DIR / 'parquet'),
    'infrastructure_stockport',
    partition_cols=['ingestion_date', 'infrastructure_type']
)

if parquet_path:
    print(f"   ✅ Parquet saved (partitioned by date and type)")
else:
    print(f"   ⚠️  Failed to save Parquet")
print()

# ============================================================================
# CREATE SUMMARY
# ============================================================================

print("4️⃣  Creating ingestion summary...")

create_bronze_summary(
    str(BRONZE_DIR),
    'infrastructure',
    len(flattened_data),
    parquet_path or json_path
)

print()

# ============================================================================
# FINAL SUMMARY
# ============================================================================

print("="*80)
print("✅ BRONZE LAYER INGESTION COMPLETE")
print("="*80)
print()
print(f"📊 Total Records: {len(flattened_data)}")
print(f"💾 Data Location: {BRONZE_DIR}")
print()
print("📁 Files Created:")
print(f"   1. Raw JSON: {BRONZE_DIR / 'raw_json'}")
print(f"   2. Parquet:  {BRONZE_DIR / 'parquet'}")
print(f"   3. Summary:  {BRONZE_DIR / '_summary_*.json'}")
print()
print("🎯 Next Steps:")
print("   1. Review data quality in Parquet files")
print("   2. Run Silver layer transformation notebook")
print("   3. Check for any data quality issues")
print()
print(f"⏱️  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*80)