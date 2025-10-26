# 🥈 Silver Layer - Setup & Execution Guide

## 📦 What You're Building

Transform Bronze (raw) data into Silver (clean, validated) tables:

```
Bronze (Raw)              →     Silver (Clean)
├── Infrastructure (127)  →     ✅ Fixed coords, BNG, enriched
├── Crime (18)            →     ✅ Float coords, BNG, enriched
├── Weather (4)           →     ✅ BNG, parsed dates
└── Postcodes (13)        →     ✅ Reference data
```

---

## 🚀 Quick Start (15 Minutes)

### **Step 1: Install Dependencies** (2 minutes)

```bash
cd ~/Downloads/PersonalProjects/nuar_mini_project
source venv/bin/activate

# Update requirements.txt
cp requirements.txt requirements.txt.bak
# Then replace with new requirements.txt

# Install new packages
pip install pyproj shapely geopandas scipy
```

**What this installs**:
- `pyproj` - Coordinate transformation (WGS84 → BNG)
- `shapely` - Geometry operations
- `geopandas` - Geospatial data handling
- `scipy` - Spatial calculations (KDTree)

---

### **Step 2: Place Files** (1 minute)

**Transformation utilities**:
```bash
# Save transformations.py
mv transformations.py src/silver/transformations.py
```

**Silver notebooks**:
```bash
# Save Silver notebooks
mv 05_silver_infrastructure.py notebooks/
mv 06_silver_crime.py notebooks/
```

**File structure**:
```
src/silver/
└── transformations.py          # Reusable transformation functions

notebooks/
├── 01_bronze_infrastructure_full.py
├── 02_bronze_crime_full.py
├── 03_bronze_weather.py
├── 04_bronze_postcodes_full.py
├── 05_silver_infrastructure.py  ← NEW!
└── 06_silver_crime.py           ← NEW!
```

---

### **Step 3: Create Silver Directories** (30 seconds)

```bash
mkdir -p data/silver/stockport/{infrastructure,crime,weather,postcodes}
```

---

### **Step 4: Run Silver Transformations** (5 minutes)

```bash
# Infrastructure transformation (2 min)
python notebooks/05_silver_infrastructure.py

# Crime transformation (1 min)
python notebooks/06_silver_crime.py
```

---

## 📊 What Each Transformation Does

### **Infrastructure (05_silver_infrastructure.py)**:

**Input**: 127 Bronze records
- ⚠️  14 missing coordinates
- ⚠️  No BNG coordinates
- ⚠️  No postcode info

**Transformations**:
1. ✅ Calculate centroids for 14 ways
2. ✅ Calculate pipeline lengths
3. ✅ Transform to British National Grid
4. ✅ Add postcodes via spatial join
5. ✅ Calculate metadata quality scores
6. ✅ Add quality flags
7. ✅ Validate locations

**Output**: 127 Silver records
- ✅ All have coordinates
- ✅ All have BNG coordinates
- ✅ Enriched with postcodes
- ✅ Quality scored

---

### **Crime (06_silver_crime.py)**:

**Input**: 18 Bronze records
- ⚠️  Coordinates as strings
- ⚠️  No BNG coordinates
- ⚠️  No postcode info

**Transformations**:
1. ✅ Convert string → float coordinates
2. ✅ Parse date fields
3. ✅ Transform to British National Grid
4. ✅ Add postcodes via spatial join
5. ✅ Add quality flags
6. ✅ Validate locations

**Output**: 18 Silver records
- ✅ Numeric coordinates
- ✅ BNG coordinates
- ✅ Enriched with postcodes
- ✅ Quality flags

---

## 🔍 Expected Output

### **Console Output**:

```
================================================================================
🥈 SILVER LAYER: Infrastructure Transformation
================================================================================
📅 Started: 2025-10-26 12:00:00

📥 LOADING BRONZE DATA
✅ Loaded 127 infrastructure records
✅ Loaded 13 postcode records

🔧 TRANSFORMATION 1: Fix Missing Coordinates
📊 Initial state: 14 records missing coordinates
   📍 Calculating centroids for 14 records...
   ✅ Fixed 14 coordinates

📏 TRANSFORMATION 2: Calculate Infrastructure Lengths
   🔍 Calculating lengths for 14 ways...
   ✅ Calculated 14 lengths
📊 Length Statistics:
   Min: 25m
   Max: 5280m
   Mean: 1150m
   Total: 16100m

🗺️  TRANSFORMATION 3: Transform to British National Grid
🔄 Transforming 127 coordinates to British National Grid...
   ✅ Transformed 127 coordinates
📊 BNG Statistics:
   Valid BNG coordinates: 127/127
   Easting range: 385000 to 395000
   Northing range: 390000 to 398000

✅ TRANSFORMATION 4: Validate Locations
   ✅ 127/127 within Stockport bbox

🗺️  TRANSFORMATION 5: Spatial Enrichment with Postcodes
   📍 Using 13 reference points
🗺️  Spatial join: 127 records → nearest of 13 points
   ✅ Matched 127 within 2000m
✅ Enrichment complete: 127/127 records matched

📊 TRANSFORMATION 6: Calculate Quality Scores
📈 Quality Score Distribution:
basic     124
medium      3
high        0

🏷️  TRANSFORMATION 7: Add Quality Flags
Quality Flags Summary:
   has_coordinates       127/127 (100.0%)
   has_bng_coords       127/127 (100.0%)
   has_postcode         127/127 (100.0%)
   has_admin_data       127/127 (100.0%)

💾 SAVING SILVER DATA
✅ Saved to: .../data/silver/stockport/infrastructure/parquet/infrastructure_cleaned
   Records: 127
   Columns: 32
   Partitioned by: transformation_date, infrastructure_type

✅ SILVER LAYER TRANSFORMATION COMPLETE
```

---

### **File Output**:

```
data/silver/stockport/
├── infrastructure/
│   ├── parquet/
│   │   └── infrastructure_cleaned/
│   │       └── transformation_date=2025-10-26/
│   │           ├── infrastructure_type=pole/
│   │           ├── infrastructure_type=manhole/
│   │           ├── infrastructure_type=pipeline/
│   │           └── infrastructure_type=cable/
│   └── infrastructure_silver_summary.csv
│
└── crime/
    ├── parquet/
    │   └── crime_cleaned/
    │       └── transformation_date=2025-10-26/
    │           ├── category=violent-crime/
    │           ├── category=burglary/
    │           └── ...
    └── crime_silver_summary.csv
```

---

## 📋 Silver Layer Schema

### **Infrastructure Silver Table**:

| Column | Type | Description |
|--------|------|-------------|
| `id` | int64 | OSM element ID |
| `osm_id` | int64 | Alias for id |
| `infrastructure_type` | category | pole/manhole/pipeline/cable |
| `type` | category | node/way |
| **Coordinates (WGS84)** |
| `lat` | float64 | Latitude (decimal degrees) |
| `lon` | float64 | Longitude (decimal degrees) |
| **Coordinates (BNG)** |
| `easting_bng` | Int64 | British National Grid easting |
| `northing_bng` | Int64 | British National Grid northing |
| **Geometry** |
| `length_meters` | float64 | Length for lines (pipelines/cables) |
| **Metadata** |
| `operator` | string | Infrastructure operator |
| `name` | string | Asset name |
| `substance` | string | water/gas/electricity |
| `material` | string | Asset material |
| **Enrichment** |
| `postcode` | string | UK postcode |
| `admin_district` | string | Administrative district |
| `admin_ward` | string | Electoral ward |
| **Quality** |
| `metadata_quality_score` | float64 | 0-1 score |
| `quality_tier` | category | high/medium/basic |
| `has_coordinates` | bool | Has lat/lon |
| `has_bng_coords` | bool | Has BNG coords |
| `has_postcode` | bool | Has postcode |
| `is_valid_location` | bool | Within Stockport |

---

## ✅ Verification

After running, verify Silver layer:

```python
import pandas as pd

# Load Silver infrastructure
df = pd.read_parquet('data/silver/stockport/infrastructure/parquet/infrastructure_cleaned')

print(f"Records: {len(df)}")
print(f"Columns: {list(df.columns)}")
print(f"\nInfrastructure types:")
print(df['infrastructure_type'].value_counts())
print(f"\nWith BNG coordinates: {df['has_bng_coords'].sum()}")
print(f"With postcodes: {df['has_postcode'].sum()}")
print(f"\nSample:")
print(df[['id', 'infrastructure_type', 'easting_bng', 'northing_bng', 'postcode']].head())
```

**Expected**:
```
Records: 127
With BNG coordinates: 127
With postcodes: 127
```

---

## 🎯 What Silver Layer Provides

### **1. Clean Coordinates** ✅
- All missing coordinates calculated
- Both WGS84 and BNG available
- Validated within Stockport bounds

### **2. Spatial Enrichment** ✅
- Postcodes added via spatial join
- Admin districts and wards
- Geographic context

### **3. Quality Metadata** ✅
- Quality scores (0-1)
- Quality tiers (high/medium/basic)
- Completeness flags

### **4. Analytics-Ready** ✅
- Proper data types
- Partitioned storage
- Fast queries

---

## 🔧 Troubleshooting

### **"ModuleNotFoundError: No module named 'pyproj'"**

```bash
pip install pyproj shapely geopandas scipy
```

### **"FileNotFoundError: transformations.py"**

```bash
# Ensure file is in correct location
ls src/silver/transformations.py

# If not, move it
mv transformations.py src/silver/
```

### **"NameError: name 'spatial_join_nearest' is not defined"**

```bash
# Check import in notebook
python -c "from src.silver.transformations import spatial_join_nearest; print('OK')"
```

### **"Transformation very slow"**

- Normal! Spatial joins take 1-2 minutes
- Progress is shown in console
- KDTree makes it much faster than naive approach

---

## 📊 Silver vs Bronze Comparison

| Aspect | Bronze | Silver |
|--------|--------|--------|
| **Coordinates** | Some missing | ✅ All present |
| **Coordinate System** | WGS84 only | ✅ WGS84 + BNG |
| **Enrichment** | None | ✅ Postcodes, districts |
| **Data Types** | Mixed | ✅ Enforced |
| **Quality Flags** | None | ✅ Comprehensive |
| **Ready for Analysis** | ❌ No | ✅ Yes |

---

## 🚀 Next: Gold Layer

After Silver is complete:
1. **Aggregate** infrastructure by postcode
2. **Calculate** crime density
3. **Join** infrastructure with crime (proximity analysis)
4. **Create** analytics tables
5. **Build** dashboards

---

## 📧 Success Criteria

Silver layer is successful when:
- [x] All notebooks run without errors
- [x] Parquet files created in data/silver/
- [x] All records have BNG coordinates
- [x] Spatial enrichment complete
- [x] Quality flags present
- [x] Summary CSVs generated

**Ready for Gold layer!** 🥇

---

**Version**: 1.0.0  
**Last Updated**: 2025-10-26