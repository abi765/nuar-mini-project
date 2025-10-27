# 🏙️ NUAR Mini - UK Infrastructure Data Hub

## A production-ready multi-source data pipeline using Medallion Architecture for Stockport infrastructure analysis

[![Python 3.13](https://img.shields.io/badge/python-3.13-blue.svg)](https://www.python.org/downloads/)
[![Databricks](https://img.shields.io/badge/platform-Databricks-red.svg)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/storage-Delta%20Lake-green.svg)](https://delta.io/)

---

## 🎯 Project Overview

NUAR Mini is an end-to-end data engineering pipeline that integrates data from **4 public UK APIs** to create a comprehensive infrastructure and contextual dataset for **Stockport, Greater Manchester**. The project implements a **Medallion Architecture** (Bronze → Silver → Gold) following data engineering best practices.

### Focus Area

- **Location**: Stockport Metropolitan Borough Council (E08000007)
- **Region**: Greater Manchester, England
- **Coverage**: 126 km² | Population: ~295,000
- **Bounding Box**: 53.35-53.45 lat, -2.20 to -2.05 lon

---

## 🔌 Data Sources

| API | Purpose | Status | Data Volume |
|-----|---------|--------|-------------|
| **Overpass API** | Infrastructure elements (manholes, pipelines, cables, ducts, poles) | ✅ Working | 500-2,000 elements |
| **UK Police API** | Crime statistics and public safety data | ✅ Working | 1,000-5,000 records |
| **OpenWeatherMap** | Current weather snapshots | ✅ Working | Real-time data |
| **Postcodes.io** | Geographic reference and postcode lookups | ✅ Working | 100-500 postcodes |

---

## 🏗️ Architecture

```text
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   Bronze    │───▶│    Silver    │───▶│    Gold     │
│  Raw Data   │    │  Cleaned &   │    │ Aggregated  │
│  (Parquet)  │    │ Transformed  │    │  Analytics  │
└─────────────┘    └──────────────┘    └─────────────┘
     ▲                    │                    │
     │              Coordinate          Cross-source
  4 APIs            Transformation      Correlations
                    Data Quality        BI-Ready
```

### 🥉 Bronze Layer

- **Format**: Parquet files with Snappy compression
- **Content**: Raw API responses with ingestion metadata
- **Partitioning**: By ingestion_date and data type
- **Features**:
  - Preserves original API responses as JSON
  - Flattened tabular structure for analysis
  - Timestamped for versioning
  - Summary files for data quality tracking

### 🥈 Silver Layer

- **Format**: Delta tables
- **Content**: Cleaned, validated, and enriched data
- **Key Transformations**:
  - Coordinate transformation (WGS84 EPSG:4326 → British National Grid EPSG:27700)
  - Data quality validation and scoring
  - Spatial joins for geographic enrichment
  - Schema enforcement and type conversion
  - Missing value handling
  - Geometry calculations (centroids, lengths)
- **Features**:
  - Quality flags and audit columns
  - Bounding box validation
  - Deduplication
  - Metadata enrichment

### 🥇 Gold Layer

- **Format**: Optimized Delta tables
- **Content**: Business-ready aggregations (In Development)
- **Planned Features**:
  - Infrastructure density analysis
  - Crime hotspot identification
  - Weather impact correlations
  - Cross-source spatial analytics
  - BI-optimized views

---

## 🎓 Tech Stack

| Category | Technology |
|----------|-----------|
| **Platform** | Databricks |
| **Storage** | Delta Lake |
| **Processing** | PySpark, Pandas |
| **Geospatial** | PyProj, Shapely, GeoPandas, Scipy |
| **Language** | Python 3.13 |
| **Orchestration** | Databricks Workflows |
| **Version Control** | Git + GitHub |

---

## 🚀 Quick Start

### 1. Local Setup

```bash
# Clone repository
git clone <your-repo-url>
cd nuar_mini_project

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env and add your OpenWeatherMap API key
```

### 2. Test APIs

```bash
python notebooks/00_test_all_apis.py
```

**Expected Output**: ✅ All 4 APIs successful

### 3. Run Bronze Layer

```bash
# Collect all data sources (takes ~5-10 minutes)
python notebooks/01_bronze_infrastructure_full.py
python notebooks/02_bronze_crime_full.py
python notebooks/03_bronze_weather.py
python notebooks/04_bronze_postcodes_full.py
```

### 4. Run Silver Layer

```bash
# Transform and clean data
python notebooks/05_silver_infrastructure.py
python notebooks/06_silver_crime.py
python notebooks/07_silver_weather.py
```

---

## 📁 Project Structure

```text
nuar_mini_project/
├── config/
│   ├── settings.py              # Central configuration
│   ├── databricks_settings.py   # Databricks-specific config
│   └── stockport.json           # Area-specific config
├── src/
│   ├── utils/
│   │   ├── api_client.py        # API client classes
│   │   └── logger.py            # Logging utilities
│   ├── bronze/
│   │   └── ingestion.py         # Bronze layer utilities
│   ├── silver/
│   │   └── transformations.py   # Silver layer utilities
│   └── gold/
│       └── __init__.py          # Gold layer (planned)
├── notebooks/                   # 📓 LOCAL development (7 notebooks)
│   ├── 01-04_bronze_*.py        # Bronze layer
│   └── 05-07_silver_*.py        # Silver layer
├── databricks_notebooks/        # 🔷 DATABRICKS deployment
│   ├── bronze/                  # Auto-converted notebooks
│   ├── silver/                  # Auto-converted notebooks
│   ├── tests/smoke_test.py      # Environment validation
│   └── setup_delta_tables.sql   # Delta Lake setup
├── data/                        # Local data storage
│   ├── bronze/                  # Raw data (Parquet)
│   ├── silver/                  # Cleaned data
│   └── gold/                    # Analytics
├── docs/
│   ├── DATABRICKS_MIGRATION.md  # Full migration guide
│   ├── BRONZE_README.md         # Bronze layer docs
│   └── QUALITY_README.md        # Data quality docs
├── databricks_setup.sh          # 🚀 Setup script
├── convert_notebooks_to_databricks.py  # Converter script
├── NOTEBOOK_STRATEGY.md         # Local vs Databricks guide
└── DATABRICKS_DEPLOYMENT_CHECKLIST.md  # Deployment checklist
```

---

## 📊 Data Pipeline Status

| Layer | Status | Records Processed | Last Updated |
|-------|--------|-------------------|--------------|
| Bronze - Infrastructure | ✅ Complete | 500-2,000 | 2025-10-26 |
| Bronze - Crime | ✅ Complete | 1,000-5,000 | 2025-10-26 |
| Bronze - Weather | ✅ Complete | Snapshots | 2025-10-26 |
| Bronze - Postcodes | ✅ Complete | 100-500 | 2025-10-26 |
| Silver - Infrastructure | ✅ Complete | Transformed | 2025-10-26 |
| Silver - Crime | ✅ Complete | Transformed | 2025-10-26 |
| Silver - Weather | ✅ Complete | Transformed | 2025-10-26 |
| Gold - Analytics | 🔄 In Development | - | - |

---

## 🗺️ Databricks Migration

Ready to deploy to Databricks? See [DATABRICKS_MIGRATION.md](docs/DATABRICKS_MIGRATION.md) for step-by-step instructions including:

- Workspace setup and configuration
- Databricks Repos integration
- Secret management for API keys
- Notebook conversion and deployment
- Delta Lake table creation
- Workflow orchestration
- Cost optimization tips

---

## 📈 Key Features

### Data Quality

- ✅ Coordinate validation and transformation
- ✅ Missing value handling
- ✅ Schema enforcement
- ✅ Quality scoring and flags
- ✅ Audit trail with timestamps

### Geospatial Processing

- ✅ WGS84 → British National Grid transformation
- ✅ Centroid calculations for line features
- ✅ Distance calculations in meters
- ✅ Spatial joins using KDTree
- ✅ Bounding box validation

### Performance

- ✅ Partitioned storage for fast queries
- ✅ Parquet with Snappy compression
- ✅ Delta Lake for ACID transactions
- ✅ Efficient coordinate transformations
- ✅ Batch processing with pandas

---

## 🔍 Data Quality Metrics

### Bronze Layer Quality

```python
import pandas as pd

# Quick quality check
infra = pd.read_parquet('data/bronze/stockport/infrastructure/parquet/infrastructure_stockport')
print(f"Infrastructure: {len(infra)} records")
print(f"Missing coordinates: {infra[['lat','lon']].isna().sum().sum()}")
print(f"Types: {infra['infrastructure_type'].value_counts()}")
```

### Silver Layer Quality

```python
# Check transformations
infra_silver = pd.read_parquet('data/silver/stockport/infrastructure')
print(f"Valid BNG coordinates: {infra_silver['has_bng_coords'].sum()}")
print(f"Within Stockport bbox: {infra_silver['is_valid_location'].sum()}")
```

---

## 🆘 Troubleshooting

### API Issues

#### Overpass API Timeout

```bash
# Normal for large queries - script has fallback endpoints
# Wait 2-3 minutes and retry
```

#### Weather API 401 Error

```bash
# Verify API key in .env
cat .env | grep OPENWEATHER_API_KEY
# Get free key from: https://openweathermap.org/api
```

### Import Errors

```bash
# Ensure virtual environment is active
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt

# Run from project root
cd /path/to/nuar_mini_project
python notebooks/01_bronze_infrastructure_full.py
```

### File Permission Errors

```bash
# Create data directories
mkdir -p data/{bronze,silver,gold}/stockport/{infrastructure,crime,weather,postcodes}
```

---

## 🎯 Roadmap

- [x] API integration and testing
- [x] Bronze layer implementation
- [x] Silver layer transformations
- [x] Coordinate system transformations
- [x] Data quality framework
- [ ] Gold layer analytics
- [ ] Databricks deployment
- [ ] Dashboard creation
- [ ] Workflow automation
- [ ] Multi-area expansion

---

## 📚 Documentation

### 🔷 Databricks Deployment (Start Here!)

**[→ Databricks Documentation Index](docs/databricks/INDEX.md)** ⭐

Complete deployment guides with numbered sequence:

- **[0.0 START HERE](docs/databricks/0.0_START_HERE.md)** - Pre-deployment checklist
- **[1.0 QUICKSTART](docs/databricks/1.0_QUICKSTART_GUIDE.md)** - Deploy in 30 minutes
- **[3.0 DEPLOYMENT CHECKLIST](docs/databricks/3.0_DEPLOYMENT_CHECKLIST.md)** - Production deployment

### 📓 Local Development

- **[Documentation Index](docs/README.md)** - All documentation organized
- **[Bronze Layer Guide](docs/BRONZE_README.md)** - Complete Bronze layer docs
- **[Silver Layer Guide](docs/SILVER_LAYER_GUIDE.md)** - Transformation guide
- **[Data Quality](docs/QUALITY_README.md)** - Quality metrics and validation

---

## 🤝 Contributing

This is a demonstration project for data engineering best practices. Areas for expansion:

1. Additional UK council areas
2. Gold layer analytics and aggregations
3. Real-time streaming ingestion
4. ML models for predictive analytics
5. Dashboard and visualization layer

---

## 📄 License

This project uses publicly available UK government data and open APIs. Please respect rate limits and terms of service for all data sources.

---

## 🙏 Acknowledgments

**Data Sources:**

- OpenStreetMap via Overpass API
- UK Police Data API
- OpenWeatherMap API
- Postcodes.io

**Technologies:**

- Databricks Community
- Apache Spark
- Delta Lake
- PyProj/Shapely geospatial libraries

---

**Last Updated**: 2025-10-27
**Version**: 2.0.0
**Status**: Bronze & Silver ✅ | Gold 🔄 | Databricks Migration Ready 🚀
