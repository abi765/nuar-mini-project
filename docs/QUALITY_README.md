# Quality & Data Validation Scripts

Scripts for inspecting and validating Bronze layer data quality.

---

## 📁 Files

### **1. `0.0_inspect_bronze_data.py`** ⭐ RUN FIRST
**Purpose**: Comprehensive data inspection for all Bronze datasets

**What it does**:
- Shows all column names and data types
- Counts missing values
- Displays sample records
- Validates coordinate ranges
- Generates inspection report

**Run**:
```bash
python quality/0.0_inspect_bronze_data.py
```

**Output**:
- Terminal report (detailed analysis)
- `data/bronze/stockport/inspection_report.json`

**Runtime**: ~10 seconds

---

### **2. `0.1_analyze_data_quality.py`** 
**Purpose**: Deep dive into data quality issues and fixes

**What it does**:
- Identifies specific issues (missing coords, string types)
- Provides fix recommendations with code
- Calculates quality scores
- Generates transformation plan for Silver layer

**Run**:
```bash
python quality/0.1_analyze_data_quality.py
```

**Output**: Detailed analysis report in terminal

**Runtime**: ~15 seconds

---

### **3. `0.2_parquet_to_csv.py`**
**Purpose**: Export Bronze Parquet files to CSV for easy viewing

**What it does**:
- Converts all Parquet files to CSV
- Creates simplified views (key columns only)
- Generates separate files by type/category
- Creates coordinate-only files for mapping

**Run**:
```bash
python quality/0.2_parquet_to_csv.py
```

**Output**: `data/bronze/stockport_csv_export/` (20+ CSV files)

**Runtime**: ~30 seconds

**Use when**: You want to view data in Excel/Google Sheets

---

### **4. `0.3_explore_bronze_interactive.py`**
**Purpose**: Interactive data exploration in Python

**What it does**:
- Loads all datasets
- Provides example queries
- Shows interactive exploration patterns

**Run**:
```bash
python3
>>> exec(open('quality/0.3_explore_bronze_interactive.py').read())
>>> df_infra.head()
```

**Use when**: You want to query data interactively

---

## 🚀 Quick Start Workflow

### **Step 1: Basic Inspection** (10 seconds)
```bash
python quality/0.0_inspect_bronze_data.py
```
Review terminal output for overview.

### **Step 2: Export to CSV** (30 seconds)
```bash
python quality/0.2_parquet_to_csv.py
```
Open CSVs in Excel for comfortable viewing.

### **Step 3: Quality Analysis** (15 seconds)
```bash
python quality/0.1_analyze_data_quality.py
```
Get detailed quality report and fix recommendations.

### **Step 4: Interactive Exploration** (optional)
```python
python3
exec(open('quality/0.3_explore_bronze_interactive.py').read())
df_infra.head(20)
df_infra['infrastructure_type'].value_counts()
```

---

## 📊 What to Look For

After running scripts, check:

**Completeness**:
- [ ] Infrastructure: 127 records ✅
- [ ] Crime: 18 records ✅
- [ ] Missing coordinates: 14 infrastructure ways ⚠️

**Validity**:
- [ ] All coordinates in Stockport bbox (53.35-53.45, -2.2 to -2.05) ✅
- [ ] Crime coordinates as strings ⚠️ (need to convert)

**Quality Issues**:
- [ ] 14 infrastructure ways need centroid calculation
- [ ] Crime lat/lon need type conversion

---

## 🎯 After Quality Checks

Once you've inspected data and understand quality:

1. **Document findings** (add notes to this README)
2. **Ready for Silver layer** transformation
3. **Use findings to design Silver transformations**

---

## 📝 Your Findings

Document what you found:

```
Date: 2025-10-26
Inspector: [Your Name]

Key Findings:
- Infrastructure: 127 elements (92 poles, 21 manholes, 12 pipelines, 2 cables)
- Quality: Good (B+ overall)
- Issues: 14 missing coords, crime string types
- Action: Calculate centroids, convert types in Silver

Ready for Silver? YES ✅
```

---

## 🔧 Dependencies

All scripts require:
```bash
pip install pandas pyarrow
```

---

## 💡 Tips

**For Quick Checks**:
- Run inspection script
- Export to CSV
- Review in Excel

**For Detailed Analysis**:
- Run quality analysis script
- Review recommendations
- Plan Silver transformations

**For Exploration**:
- Use interactive script
- Query data yourself
- Find patterns

---

**Last Updated**: 2025-10-26
