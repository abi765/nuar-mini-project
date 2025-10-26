# 🥉 Bronze Layer - Complete Data Ingestion Guide

## Overview

The Bronze layer collects **ALL raw data** for Stockport from 4 APIs and saves it as **Parquet files** in **Delta Lake format**.

---

## 📦 What You're Getting

### Complete Bronze Layer Package:

**Utilities** (Place in `src/`):
1. `src/utils/api_client.py` - API client classes for all 4 APIs
2. `src/bronze/ingestion.py` - Bronze layer data processing utilities

**Notebooks** (Place in `notebooks/`):
3. `01_bronze_infrastructure_full.py` - Infrastructure data (500-2000 elements)
4. `02_bronze_crime_full.py` - Crime data (1000-5000 records)
5. `03_bronze_weather.py` - Weather snapshots (run regularly)
6. `04_bronze_postcodes_full.py` - Postcode data (100-500 postcodes)

---

## 📁 File Placement

```bash
# 1. Copy utilities to src/
cp api_client.py ~/path/to/nuar_mini_project/src/utils/
cp bronze_ingestion.py ~/path/to/nuar_mini_project/src/bronze/

# 2. Copy notebooks
cp 01_bronze_infrastructure_full.py ~/path/to/nuar_mini_project/notebooks/
cp 02_bronze_crime_full.py ~/path/to/nuar_mini_project/notebooks/
cp 03_bronze_weather.py ~/path/to/nuar_mini_project/notebooks/
cp 04_bronze_postcodes_full.py ~/path/to/nuar_mini_project/notebooks/
```

---

## 🔧 Setup Requirements

### 1. Install Dependencies

```bash
cd ~/path/to/nuar_mini_project
source venv/bin/activate

# Install required packages
pip install pandas pyarrow requests python-dotenv
```

### 2. Verify Configuration

Make sure you have:
- ✅ `config/stockport.json` with bounding box
- ✅ `.env` with OPENWEATHER_API_KEY

### 3. Test Utilities

```bash
# Test imports
python3 -c "from src.utils.api_client import OverpassClient; print('✅ API client OK')"
python3 -c "from src.bronze.ingestion import save_to_bronze_parquet; print('✅ Ingestion utilities OK')"
```

---

## 🚀 Running Bronze Layer Notebooks

### **Recommended Order**:

```bash
# 1. Infrastructure (takes 2-5 minutes)
python notebooks/01_bronze_infrastructure_full.py

# 2. Crime data (takes 2-3 minutes due to grid queries)
python notebooks/02_bronze_crime_full.py

# 3. Weather (quick, <30 seconds)
python notebooks/03_bronze_weather.py

# 4. Postcodes (quick, <1 minute)
python notebooks/04_bronze_postcodes_full.py
```

### **Or run all at once**:

```bash
cd ~/path/to/nuar_mini_project
source venv/bin/activate

python notebooks/01_bronze_infrastructure_full.py && \
python notebooks/02_bronze_crime_full.py && \
python notebooks/03_bronze_weather.py && \
python notebooks/04_bronze_postcodes_full.py

echo "✅ All Bronze layer data collected!"
```

---

## 📊 Expected Output

### **Directory Structure After Running**:

```
data/bronze/stockport/
├── infrastructure/
│   ├── raw_json/
│   │   └── overpass_full_response_20251026_120000.json
│   ├── parquet/
│   │   └── infrastructure_stockport/
│   │       ├── ingestion_date=2025-10-26/
│   │       │   ├── infrastructure_type=manhole/
│   │       │   ├── infrastructure_type=pipeline/
│   │       │   └── infrastructure_type=cable/
│   └── _summary_infrastructure_*.json
│
├── crime/
│   ├── raw_json/
│   │   └── police_crimes_full_*.json
│   ├── parquet/
│   │   └── crimes_stockport/
│   │       └── ingestion_date=2025-10-26/
│   │           ├── month=2025-09/
│   │           └── month=2025-10/
│   └── _summary_crime_*.json
│
├── weather/
│   ├── raw_json/
│   │   └── weather_snapshot_*.json
│   ├── parquet/
│   │   └── weather_stockport/
│   │       └── ingestion_date=2025-10-26/
│   └── _summary_weather_*.json
│
└── postcodes/
    ├── raw_json/
    │   └── postcodes_full_*.json
    ├── parquet/
    │   └── postcodes_stockport/
    │       └── ingestion_date=2025-10-26/
    │           ├── postcode_type=outward_code/
    │           └── postcode_type=full_postcode/
    └── _summary_postcodes_*.json
```

---

## 📊 Expected Data Volumes

| Data Source | Expected Records | File Size | Query Time |
|-------------|-----------------|-----------|------------|
| Infrastructure | 500-2,000 | 1-5 MB | 2-5 min |
| Crime | 1,000-5,000 | 0.5-2 MB | 2-3 min |
| Weather | 1 (snapshot) | 5 KB | 30 sec |
| Postcodes | 100-500 | 50-200 KB | 1 min |

**Total Bronze Layer**: ~2-8 MB raw data

---

## ✅ Verification

### Check Infrastructure Data:

```python
import pandas as pd

# Read infrastructure Parquet
df = pd.read_parquet('data/bronze/stockport/infrastructure/parquet/infrastructure_stockport')

print(f"Total infrastructure elements: {len(df)}")
print(f"\nInfrastructure types:")
print(df['infrastructure_type'].value_counts())
print(f"\nSample record:")
print(df.iloc[0])
```

### Check Crime Data:

```python
import pandas as pd

# Read crime Parquet
df = pd.read_parquet('data/bronze/stockport/crime/parquet/crimes_stockport')

print(f"Total crimes: {len(df)}")
print(f"\nCrime categories:")
print(df['category'].value_counts())
print(f"\nMonthly distribution:")
print(df['month'].value_counts())
```

### Check All Summaries:

```bash
# View all Bronze layer summaries
find data/bronze/stockport -name "_summary_*.json" -exec echo "---" \; -exec cat {} \;
```

---

## 🎯 What Bronze Layer Provides

### For Each Data Source:

**1. Raw JSON**
- Exact API response
- Full metadata preserved
- Useful for debugging

**2. Parquet Files**
- Flattened tabular format
- Compressed (Snappy)
- Fast to read
- **Partitioned** by date and type

**3. Ingestion Metadata**
- Timestamp of collection
- Source API
- Area (Stockport)
- Record counts

---

## 🔍 Data Quality Checks

### After running Bronze layer:

```python
import pandas as pd
import json
from pathlib import Path

# Check infrastructure
infra_df = pd.read_parquet('data/bronze/stockport/infrastructure/parquet/infrastructure_stockport')
print(f"Infrastructure:")
print(f"  Total: {len(infra_df)}")
print(f"  Missing lat/lon: {infra_df[['lat', 'lon']].isna().sum().sum()}")
print(f"  Types: {infra_df['infrastructure_type'].nunique()}")

# Check crime
crime_df = pd.read_parquet('data/bronze/stockport/crime/parquet/crimes_stockport')
print(f"\nCrime:")
print(f"  Total: {len(crime_df)}")
print(f"  Missing lat/lon: {crime_df[['lat', 'lon']].isna().sum().sum()}")
print(f"  Categories: {crime_df['category'].nunique()}")

# Check weather
weather_df = pd.read_parquet('data/bronze/stockport/weather/parquet/weather_stockport')
print(f"\nWeather:")
print(f"  Snapshots: {len(weather_df)}")
print(f"  Latest temp: {weather_df['temp_celsius'].iloc[-1]}°C")

# Check postcodes
postcode_df = pd.read_parquet('data/bronze/stockport/postcodes/parquet/postcodes_stockport')
print(f"\nPostcodes:")
print(f"  Total: {len(postcode_df)}")
print(f"  Outward codes: {len(postcode_df[postcode_df['postcode_type']=='outward_code'])}")
print(f"  Full postcodes: {len(postcode_df[postcode_df['postcode_type']=='full_postcode'])}")
```

---

## 🔄 Re-running Bronze Layer

### You can re-run notebooks anytime:

**Infrastructure & Crime**: 
- Run when you want updated OSM data
- Crime data updates monthly

**Weather**:
- Run regularly (daily/hourly) to build historical dataset
- Each run adds a new snapshot

**Postcodes**:
- Run once (postcode boundaries rarely change)
- Re-run if you expand to new areas

### Data Versioning:

All files have timestamps in filenames:
- `infrastructure_stockport_20251026_120000.parquet`
- `weather_snapshot_20251026_140000.json`

Old data is preserved, new data is added.

---

## 🐛 Troubleshooting

### **"ModuleNotFoundError: No module named 'src'"**

```bash
# Run from project root
cd ~/path/to/nuar_mini_project
python notebooks/01_bronze_infrastructure_full.py
```

### **"Overpass API timeout"**

- Normal for large queries
- Script has fallback endpoints
- Retry after a few minutes

### **"No crime data found"**

- Normal for some areas/months
- Police API only has recent data
- Script still creates empty files for consistency

### **"Weather API 401 error"**

```bash
# Check API key
cat .env | grep OPENWEATHER_API_KEY

# Verify key is valid on openweathermap.org
```

### **"Permission denied writing files"**

```bash
# Create directories manually
mkdir -p data/bronze/stockport/{infrastructure,crime,weather,postcodes}
```

---

## 🎯 Next Steps After Bronze

Once Bronze layer is complete:

1. ✅ **Verify data quality** (run checks above)
2. ✅ **Review Parquet files** (open in pandas/spark)
3. ✅ **Check record counts** meet expectations
4. ✅ **Commit to Git** (if satisfied)
5. 🚀 **Build Silver Layer** (transformation & cleaning)

---

## 📧 Support

If you encounter issues:
1. Check troubleshooting section above
2. Verify all dependencies installed
3. Ensure config files are correct
4. Check API keys are valid

---

## ✅ Success Criteria

Bronze layer is successful when:
- ✅ All 4 notebooks run without errors
- ✅ Parquet files created in data/bronze/
- ✅ Record counts match expectations
- ✅ Data quality checks pass
- ✅ Summary JSON files present

**You're ready for Silver layer!** 🎉

---

**Last Updated**: 2025-10-26  
**Version**: 1.0.0