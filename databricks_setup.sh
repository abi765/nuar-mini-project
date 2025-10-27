#!/bin/bash

# ============================================================================
# NUAR Mini - Databricks Setup Script
# ============================================================================
# This script helps prepare your project for Databricks deployment
# Run this locally before pushing to Databricks Repos
# ============================================================================

set -e  # Exit on error

echo "🚀 NUAR Mini - Databricks Setup Script"
echo "======================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# ============================================================================
# 1. Check Prerequisites
# ============================================================================

echo "📋 Step 1: Checking Prerequisites..."
echo ""

# Check Python version
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    echo -e "${GREEN}✅ Python3 installed: $PYTHON_VERSION${NC}"
else
    echo -e "${RED}❌ Python3 not found. Please install Python 3.8+${NC}"
    exit 1
fi

# Check Git
if command -v git &> /dev/null; then
    GIT_VERSION=$(git --version | cut -d' ' -f3)
    echo -e "${GREEN}✅ Git installed: $GIT_VERSION${NC}"
else
    echo -e "${RED}❌ Git not found. Please install Git${NC}"
    exit 1
fi

# Check for .env file
if [ -f ".env" ]; then
    echo -e "${GREEN}✅ .env file found${NC}"

    # Check if API key is configured
    if grep -q "OPENWEATHER_API_KEY=" .env; then
        echo -e "${GREEN}✅ OpenWeather API key configured${NC}"
    else
        echo -e "${YELLOW}⚠️  OpenWeather API key not found in .env${NC}"
        echo "   Get your free API key from: https://openweathermap.org/api"
    fi
else
    echo -e "${YELLOW}⚠️  .env file not found${NC}"
    echo "   Copy .env.example to .env and add your API key"
fi

echo ""

# ============================================================================
# 2. Create Databricks Notebooks Directory
# ============================================================================

echo "📁 Step 2: Creating Databricks Notebooks Directory..."
echo ""

mkdir -p databricks_notebooks/{bronze,silver,gold,tests}

echo -e "${GREEN}✅ Created databricks_notebooks/ structure${NC}"
echo ""

# ============================================================================
# 3. Create Databricks-Compatible Notebooks
# ============================================================================

echo "📝 Step 3: Creating Databricks-Compatible Notebooks..."
echo ""

# Create Bronze Infrastructure Notebook
cat > databricks_notebooks/bronze/01_infrastructure.py << 'EOF'
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Infrastructure Data
# MAGIC
# MAGIC Collects infrastructure data from Overpass API and saves to Delta Lake

# COMMAND ----------
# MAGIC %pip install pyarrow geopandas pyproj shapely scipy requests

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# Import configuration
import sys
sys.path.append('/Workspace/Repos/YOUR_EMAIL/nuar_mini_project')

from config.databricks_settings import *
from src.utils.api_client import OverpassClient
from src.bronze.ingestion import flatten_overpass_elements

# COMMAND ----------
print_config_summary()

# COMMAND ----------
# Query Overpass API
print("🔍 Querying Overpass API for Stockport infrastructure...")
client = OverpassClient(timeout=OVERPASS_TIMEOUT)
result = client.query_infrastructure(STOCKPORT_BBOX, STOCKPORT_INFRASTRUCTURE_TYPES)

# COMMAND ----------
# Process and save to Delta
if result and 'elements' in result:
    import pandas as pd
    from datetime import datetime

    elements = result['elements']
    print(f"📊 Retrieved {len(elements)} infrastructure elements")

    # Flatten data
    flattened = flatten_overpass_elements(elements)
    df_pd = pd.DataFrame(flattened)

    # Add metadata
    df_pd['ingestion_timestamp'] = datetime.now()
    df_pd['ingestion_date'] = datetime.now().date()
    df_pd['area'] = 'stockport'

    # Convert to Spark DataFrame
    df_spark = spark.createDataFrame(df_pd)

    # Write to Delta table
    df_spark.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("ingestion_date", "infrastructure_type") \
        .option("overwriteSchema", "true") \
        .saveAsTable(BRONZE_TABLES['infrastructure'])

    print(f"✅ Saved {len(df_pd)} records to {BRONZE_TABLES['infrastructure']}")
else:
    print("❌ No data retrieved from API")

# COMMAND ----------
# Verify data
display(spark.table(BRONZE_TABLES['infrastructure']).limit(10))

# COMMAND ----------
# Show summary statistics
spark.sql(f"""
    SELECT
        infrastructure_type,
        COUNT(*) as count,
        COUNT(DISTINCT id) as unique_elements,
        ingestion_date
    FROM {BRONZE_TABLES['infrastructure']}
    GROUP BY infrastructure_type, ingestion_date
    ORDER BY ingestion_date DESC, count DESC
""").display()
EOF

echo -e "${GREEN}✅ Created Bronze Infrastructure notebook${NC}"

# Create Test Notebook
cat > databricks_notebooks/tests/smoke_test.py << 'EOF'
# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Environment Smoke Test
# MAGIC
# MAGIC Validates that the Databricks environment is correctly configured

# COMMAND ----------
# Test 1: Catalog and Schema Setup
print("🔍 Test 1: Checking catalog and schemas...")
try:
    spark.sql("USE CATALOG nuar_catalog")
    schemas = spark.sql("SHOW SCHEMAS IN nuar_catalog").collect()
    schema_names = [row.databaseName for row in schemas]

    for schema in ['bronze', 'silver', 'gold']:
        if schema in schema_names:
            print(f"✅ Schema '{schema}' exists")
        else:
            print(f"❌ Schema '{schema}' not found")
except Exception as e:
    print(f"❌ Catalog error: {e}")

# COMMAND ----------
# Test 2: API Connectivity
print("\n🔍 Test 2: Testing API connectivity...")
import requests

apis = {
    "Overpass": "https://overpass-api.de/api/status",
    "Police": "https://data.police.uk/api/forces",
    "Postcodes": "https://api.postcodes.io/postcodes/SK1+1EB"
}

for name, url in apis.items():
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            print(f"✅ {name} API: OK")
        else:
            print(f"⚠️  {name} API: Status {response.status_code}")
    except Exception as e:
        print(f"❌ {name} API: {str(e)}")

# COMMAND ----------
# Test 3: Secrets Access
print("\n🔍 Test 3: Checking secrets...")
try:
    api_key = dbutils.secrets.get(scope="nuar_secrets", key="openweather_api_key")
    if api_key and len(api_key) > 10:
        print(f"✅ OpenWeather API key: Configured (length: {len(api_key)})")
    else:
        print("❌ OpenWeather API key: Invalid or empty")
except Exception as e:
    print(f"⚠️  Secret access: {str(e)}")
    print("   Note: You may need to create secret scope first")

# COMMAND ----------
# Test 4: Required Libraries
print("\n🔍 Test 4: Checking required libraries...")
try:
    import pandas as pd
    import geopandas as gpd
    from pyproj import Transformer
    from shapely.geometry import Point
    import scipy
    print("✅ All required libraries installed")
except ImportError as e:
    print(f"❌ Missing library: {e}")

# COMMAND ----------
# Test 5: Configuration Loading
print("\n🔍 Test 5: Loading project configuration...")
try:
    import sys
    sys.path.append('/Workspace/Repos/YOUR_EMAIL/nuar_mini_project')

    from config.databricks_settings import *
    print(f"✅ Configuration loaded")
    print(f"   Catalog: {CATALOG_NAME}")
    print(f"   Bronze schema: {BRONZE_SCHEMA_FULL}")
    print(f"   Silver schema: {SILVER_SCHEMA_FULL}")
except Exception as e:
    print(f"❌ Configuration error: {e}")

# COMMAND ----------
print("\n" + "="*60)
print("🎉 Smoke Test Complete!")
print("="*60)
EOF

echo -e "${GREEN}✅ Created Smoke Test notebook${NC}"
echo ""

# ============================================================================
# 4. Create Setup SQL Script
# ============================================================================

echo "🗄️  Step 4: Creating Delta Lake Setup SQL..."
echo ""

cat > databricks_notebooks/setup_delta_tables.sql << 'EOF'
-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Lake Table Setup
-- MAGIC
-- MAGIC Creates all Bronze, Silver, and Gold layer tables

-- COMMAND ----------
-- Create Catalog
CREATE CATALOG IF NOT EXISTS nuar_catalog;
USE CATALOG nuar_catalog;

-- COMMAND ----------
-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw data from APIs - Parquet format';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Cleaned and transformed data - Delta format';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Aggregated analytics - Delta format';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------
-- Bronze Infrastructure Table
CREATE TABLE IF NOT EXISTS bronze.infrastructure (
  id BIGINT COMMENT 'OSM element ID',
  type STRING COMMENT 'Element type (node/way)',
  lat DOUBLE COMMENT 'Latitude (WGS84)',
  lon DOUBLE COMMENT 'Longitude (WGS84)',
  infrastructure_type STRING COMMENT 'Type of infrastructure',
  substance STRING COMMENT 'Substance carried (e.g., water, gas)',
  location STRING COMMENT 'Location context',
  operator STRING COMMENT 'Infrastructure operator',
  name STRING COMMENT 'Element name',
  diameter STRING COMMENT 'Pipe/cable diameter',
  material STRING COMMENT 'Construction material',
  all_tags STRING COMMENT 'All OSM tags as JSON',
  node_count INT COMMENT 'Number of nodes in way',
  geometry_points INT COMMENT 'Number of geometry points',
  first_lat DOUBLE COMMENT 'First point latitude',
  first_lon DOUBLE COMMENT 'First point longitude',
  last_lat DOUBLE COMMENT 'Last point latitude',
  last_lon DOUBLE COMMENT 'Last point longitude',
  geometry_json STRING COMMENT 'Full geometry as JSON',
  ingestion_timestamp TIMESTAMP COMMENT 'Data ingestion timestamp',
  ingestion_date DATE COMMENT 'Data ingestion date',
  area STRING COMMENT 'Geographic area (e.g., stockport)'
)
USING DELTA
PARTITIONED BY (ingestion_date, infrastructure_type)
LOCATION 'dbfs:/FileStore/nuar/bronze/infrastructure'
COMMENT 'Raw infrastructure data from Overpass API';

-- COMMAND ----------
-- Verify table creation
DESCRIBE EXTENDED bronze.infrastructure;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Silver Layer Tables

-- COMMAND ----------
-- Silver Infrastructure Table
CREATE TABLE IF NOT EXISTS silver.infrastructure (
  id BIGINT,
  type STRING,
  lat DOUBLE,
  lon DOUBLE,
  easting_bng BIGINT COMMENT 'British National Grid Easting',
  northing_bng BIGINT COMMENT 'British National Grid Northing',
  infrastructure_type STRING,
  substance STRING,
  location STRING,
  operator STRING,
  name STRING,
  diameter STRING,
  material STRING,
  length_meters DOUBLE COMMENT 'Length of linear features in meters',
  postcode STRING COMMENT 'Nearest postcode',
  admin_district STRING COMMENT 'Administrative district',
  admin_ward STRING COMMENT 'Administrative ward',
  has_coordinates BOOLEAN COMMENT 'Has valid WGS84 coordinates',
  has_bng_coords BOOLEAN COMMENT 'Has valid BNG coordinates',
  has_postcode BOOLEAN COMMENT 'Has postcode enrichment',
  has_admin_data BOOLEAN COMMENT 'Has admin area data',
  is_valid_location BOOLEAN COMMENT 'Within Stockport bounding box',
  transformation_timestamp TIMESTAMP,
  transformation_date DATE,
  data_layer STRING,
  source_system STRING,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date, infrastructure_type)
LOCATION 'dbfs:/FileStore/nuar/silver/infrastructure'
COMMENT 'Cleaned infrastructure data with BNG coordinates and enrichment';

-- COMMAND ----------
-- Show all tables
SHOW TABLES IN bronze;
SHOW TABLES IN silver;

-- COMMAND ----------
SELECT 'Setup Complete! ✅' as status;
EOF

echo -e "${GREEN}✅ Created Delta Lake setup SQL${NC}"
echo ""

# ============================================================================
# 5. Create README for Databricks
# ============================================================================

echo "📚 Step 5: Creating Databricks Quick Start Guide..."
echo ""

cat > databricks_notebooks/DATABRICKS_QUICKSTART.md << 'EOF'
# 🚀 Databricks Quick Start Guide

## Step-by-Step Deployment

### 1. Push to GitHub

```bash
git add .
git commit -m "feat: Add Databricks deployment files"
git push origin main
```

### 2. Set Up Databricks Repos

1. Log into Databricks workspace
2. Click "Repos" in sidebar
3. Click "Add Repo"
4. Enter your GitHub repository URL
5. Click "Create Repo"

### 3. Configure Secrets

```bash
# Using Databricks CLI
databricks secrets create-scope --scope nuar_secrets
databricks secrets put --scope nuar_secrets --key openweather_api_key
```

### 4. Create Cluster

- Go to "Compute" → "Create Cluster"
- Name: `nuar-cluster`
- Runtime: 14.3 LTS
- Node: Single Node
- Install libraries: `pyarrow geopandas pyproj shapely scipy`

### 5. Run Setup

1. Open `databricks_notebooks/setup_delta_tables.sql`
2. Run all cells to create catalog and tables
3. Open `databricks_notebooks/tests/smoke_test.py`
4. Run smoke test to validate setup

### 6. Run Bronze Layer

1. Update paths in notebooks (replace `YOUR_EMAIL`)
2. Run bronze layer notebooks in order:
   - `01_infrastructure.py`
   - `02_crime.py`
   - `03_weather.py`
   - `04_postcodes.py`

### 7. Verify Data

```sql
SELECT * FROM nuar_catalog.bronze.infrastructure LIMIT 10;
```

## Troubleshooting

- **Import errors**: Update `sys.path.append()` with your Repos path
- **Secret errors**: Create secret scope in Databricks UI
- **API timeouts**: Normal for large queries, wait and retry

## Next Steps

- Set up Workflows for automation
- Create Silver layer transformations
- Build Gold layer analytics
- Create dashboards

**Need help?** See [DATABRICKS_MIGRATION.md](../docs/DATABRICKS_MIGRATION.md)
EOF

echo -e "${GREEN}✅ Created Quick Start Guide${NC}"
echo ""

# ============================================================================
# 6. Update .gitignore for Databricks
# ============================================================================

echo "🔒 Step 6: Updating .gitignore..."
echo ""

if [ -f ".gitignore" ]; then
    # Add Databricks-specific ignores if not already present
    if ! grep -q "# Databricks" .gitignore; then
        cat >> .gitignore << 'EOF'

# Databricks
.databricks/
databricks.log
*.dbconfig

# Databricks checkpoints
*/_checkpoint/
*/_delta_log/

# Databricks local cache
.dbx/
EOF
        echo -e "${GREEN}✅ Updated .gitignore${NC}"
    else
        echo -e "${YELLOW}⚠️  Databricks entries already in .gitignore${NC}"
    fi
else
    echo -e "${RED}❌ .gitignore not found${NC}"
fi

echo ""

# ============================================================================
# 7. Final Summary
# ============================================================================

echo ""
echo "=========================================="
echo "🎉 Databricks Setup Complete!"
echo "=========================================="
echo ""
echo "Created Files:"
echo "  ✅ databricks_notebooks/ directory"
echo "  ✅ Sample Bronze notebook"
echo "  ✅ Smoke test notebook"
echo "  ✅ Delta Lake setup SQL"
echo "  ✅ Quick Start Guide"
echo ""
echo "Next Steps:"
echo "  1. Review databricks_notebooks/DATABRICKS_QUICKSTART.md"
echo "  2. Update notebook paths (replace YOUR_EMAIL)"
echo "  3. Push to GitHub: git push origin main"
echo "  4. Set up Databricks Repos"
echo "  5. Run smoke test"
echo ""
echo "Documentation:"
echo "  📖 Full guide: docs/DATABRICKS_MIGRATION.md"
echo "  📖 Quick start: databricks_notebooks/DATABRICKS_QUICKSTART.md"
echo ""
echo "=========================================="
