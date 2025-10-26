# 🏙️ NUAR Mini - Infrastructure Data Hub

Multi-source data pipeline using Medallion Architecture for UK infrastructure analysis.

## 🎯 Project Overview

Integrates data from 4 APIs to analyze UK infrastructure across 3 UK regions:
- Central London
- Manchester Centre  
- Birmingham Centre

**Architecture**: Bronze (Parquet) → Silver (Delta) → Gold (Optimized Delta)

## 🔌 Data Sources

1. **Overpass API** - Infrastructure (pipelines, cables, manholes)
2. **UK Police API** - Crime data
3. **OpenWeatherMap** - Weather data (requires free API key)
4. **Postcodes.io** - Geo lookups

## 🚀 Quick Start

### Setup
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure API key
cp .env.example .env
# Edit .env and add your OpenWeatherMap API key
```

### Test APIs
```bash
python notebooks/00_test_all_apis.py
```

Expected: ✅ All 4 APIs successful

## 📁 Project Structure

- `notebooks/` - Databricks workflow notebooks
- `scripts/` - Standalone Python scripts
- `config/` - Configuration files
- `src/` - Reusable Python modules
- `tests/` - Unit tests
- `data/` - Local data storage

## 🎓 Tech Stack

- **Platform**: Databricks
- **Storage**: Delta Lake
- **Processing**: PySpark
- **Language**: Python + SQL
- **Version Control**: Git + GitHub

## 🗺️ Regions

| Region | Area | Expected Data |
|--------|------|---------------|
| Central London | 25 km² | ~8,000 elements |
| Manchester Centre | 16 km² | ~5,000 elements |
| Birmingham Centre | 16 km² | ~5,000 elements |

## 📊 Pipeline Layers

### 🥉 Bronze Layer
- Raw API responses
- Parquet format in Delta Lake
- Partitioned by date & region

### 🥈 Silver Layer
- Cleaned & validated data
- Coordinate transformation (WGS84 → British National Grid)
- Delta tables with enforced schemas

### 🥇 Gold Layer
- Aggregated analytics
- Cross-source correlations
- Optimized for BI tools

## 🔗 Next Steps

1. ✅ Test APIs locally (DONE!)
2. Push to GitHub
3. Connect Databricks Repos
4. Build Bronze layer
5. Build Silver layer
6. Build Gold layer
7. Create dashboards

## 🆘 Troubleshooting

**API Test Fails?**
- Check `.env` has OPENWEATHER_API_KEY
- Verify internet connection
- Activate virtual environment

**Import Errors?**
```bash
source venv/bin/activate
pip install -r requirements.txt
```

---

**Last Updated**: 2025-10-26  
**Status**: APIs Tested ✅ | Ready for Bronze Layer
