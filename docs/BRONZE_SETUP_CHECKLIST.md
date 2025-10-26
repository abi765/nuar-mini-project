# ✅ Bronze Layer - Quick Setup Checklist

## 📦 Files to Download (7 total)

### From Claude's Outputs:

**Core Utilities** (2 files):
- [ ] `api_client.py` → Save to: `src/utils/api_client.py`
- [ ] `bronze_ingestion.py` → Save to: `src/bronze/ingestion.py`

**Bronze Notebooks** (4 files):
- [ ] `01_bronze_infrastructure_full.py` → Save to: `notebooks/`
- [ ] `02_bronze_crime_full.py` → Save to: `notebooks/`
- [ ] `03_bronze_weather.py` → Save to: `notebooks/`
- [ ] `04_bronze_postcodes_full.py` → Save to: `notebooks/`

**Documentation**:
- [ ] `BRONZE_LAYER_README.md` → Save to: `docs/bronze_layer.md`

---

## 🔧 Quick Setup (5 Minutes)

### 1. Install Dependencies
```bash
cd ~/Downloads/PersonalProjects/nuar_mini_project
source venv/bin/activate
pip install pandas pyarrow
```

### 2. Verify Files
```bash
# Check utilities exist
ls -l src/utils/api_client.py
ls -l src/bronze/ingestion.py

# Check notebooks exist
ls -l notebooks/01_bronze_*.py
ls -l notebooks/02_bronze_*.py
ls -l notebooks/03_bronze_*.py
ls -l notebooks/04_bronze_*.py
```

### 3. Test Imports
```bash
python3 -c "from src.utils.api_client import OverpassClient; print('✅ OK')"
python3 -c "from src.bronze.ingestion import save_to_bronze_parquet; print('✅ OK')"
```

### 4. Verify Config
```bash
# Check config exists
cat config/stockport.json | grep "stockport"

# Check API key exists
cat .env | grep OPENWEATHER_API_KEY
```

---

## 🚀 Run Bronze Layer (10-15 Minutes Total)

```bash
cd ~/Downloads/PersonalProjects/nuar_mini_project
source venv/bin/activate

# Run all Bronze notebooks
echo "Starting Bronze layer ingestion..."

python notebooks/01_bronze_infrastructure_full.py
echo "✅ Infrastructure complete"

python notebooks/02_bronze_crime_full.py
echo "✅ Crime complete"

python notebooks/03_bronze_weather.py
echo "✅ Weather complete"

python notebooks/04_bronze_postcodes_full.py
echo "✅ Postcodes complete"

echo ""
echo "🎉 Bronze layer complete!"
echo "📁 Check: data/bronze/stockport/"
```

---

## 📊 Verify Results

```bash
# Check directories created
ls -R data/bronze/stockport/

# Count Parquet files
find data/bronze/stockport -name "*.parquet" | wc -l

# Check summaries
find data/bronze/stockport -name "_summary_*.json"
```

---

## ✅ Success Checklist

After running, verify:
- [ ] `data/bronze/stockport/infrastructure/` exists with Parquet files
- [ ] `data/bronze/stockport/crime/` exists with Parquet files
- [ ] `data/bronze/stockport/weather/` exists with Parquet files
- [ ] `data/bronze/stockport/postcodes/` exists with Parquet files
- [ ] Each directory has `_summary_*.json` file
- [ ] No error messages in terminal output

---

## 📈 Expected Metrics

| Data Source | Files | Records | Status |
|-------------|-------|---------|--------|
| Infrastructure | Parquet + JSON | 500-2000 | ✅ |
| Crime | Parquet + JSON | 1000-5000 | ✅ |
| Weather | Parquet + JSON | 1 | ✅ |
| Postcodes | Parquet + JSON | 100-500 | ✅ |

---

## 🎯 Next: Verify Data Quality

```python
import pandas as pd

# Quick check
infra = pd.read_parquet('data/bronze/stockport/infrastructure/parquet/infrastructure_stockport')
print(f"Infrastructure: {len(infra)} elements")
print(infra['infrastructure_type'].value_counts())

crime = pd.read_parquet('data/bronze/stockport/crime/parquet/crimes_stockport')
print(f"\nCrime: {len(crime)} records")
print(crime['category'].value_counts().head())
```

---

## 🔄 If Something Fails

**Import Error?**
```bash
# Make sure you're in project root
pwd  # Should show: .../nuar_mini_project
python notebooks/01_bronze_infrastructure_full.py
```

**API Timeout?**
- Wait 5 minutes and retry
- Overpass API can be slow for large areas
- Script has fallback endpoints

**No Crime Data?**
- Normal! Not all areas have recent crime data
- Script will complete successfully with 0 records

---

## 💡 Pro Tips

1. **Run Infrastructure First** - It's the biggest dataset
2. **Weather Can Be Run Anytime** - Quick snapshot
3. **Check Logs** - Each notebook prints detailed progress
4. **Save Terminal Output** - Run with `| tee bronze_log.txt`

---

**Ready?** Start with Step 1! 🚀