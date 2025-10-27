#!/usr/bin/env python3
"""
Convert existing local notebooks to Databricks-compatible format
This script takes your working notebooks/ folder and creates Databricks versions
"""

import re
import os
from pathlib import Path

# Databricks notebook header template
DATABRICKS_HEADER = """# Databricks notebook source
# MAGIC %md
# MAGIC # {title}
# MAGIC
# MAGIC **Converted from**: `notebooks/{original_filename}`
# MAGIC
# MAGIC {description}

# COMMAND ----------
# MAGIC %md
# MAGIC ## Setup: Install Required Libraries

# COMMAND ----------
# Install required packages (only needed once per cluster, or use cluster libraries)
%pip install pyarrow geopandas pyproj shapely scipy python-dotenv requests

# COMMAND ----------
# Restart Python kernel to load new packages
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Import Configuration and Utilities

# COMMAND ----------
"""

def get_notebook_metadata(filename):
    """Extract title and description from filename"""
    metadata = {
        '01_bronze_infrastructure_full.py': {
            'title': 'Bronze Layer - Infrastructure Data',
            'description': 'Collects infrastructure data (manholes, pipelines, cables) from Overpass API'
        },
        '02_bronze_crime_full.py': {
            'title': 'Bronze Layer - Crime Data',
            'description': 'Collects crime statistics from UK Police API'
        },
        '03_bronze_weather.py': {
            'title': 'Bronze Layer - Weather Data',
            'description': 'Collects current weather snapshots from OpenWeatherMap API'
        },
        '04_bronze_postcodes_full.py': {
            'title': 'Bronze Layer - Postcode Data',
            'description': 'Collects postcode reference data from Postcodes.io API'
        },
        '05_silver_infrastructure.py': {
            'title': 'Silver Layer - Infrastructure Transformation',
            'description': 'Transforms and enriches infrastructure data with BNG coordinates'
        },
        '06_silver_crime.py': {
            'title': 'Silver Layer - Crime Transformation',
            'description': 'Transforms and enriches crime data with spatial information'
        },
        '07_silver_weather.py': {
            'title': 'Silver Layer - Weather Transformation',
            'description': 'Transforms weather data and adds quality checks'
        }
    }
    return metadata.get(filename, {'title': filename, 'description': 'Data processing notebook'})

def convert_imports(content):
    """Convert local imports to Databricks-compatible imports"""

    # Replace sys.path manipulation with Databricks approach
    content = re.sub(
        r'# Add project root to path\s+project_root = Path\(__file__\)\.parent\.parent\s+sys\.path\.append\(str\(project_root\)\)',
        '''# Import Databricks configuration
import sys
sys.path.append('/Workspace/Repos/YOUR_EMAIL/nuar_mini_project')

from config.databricks_settings import *''',
        content,
        flags=re.DOTALL
    )

    # Replace local config loading
    content = re.sub(
        r"with open\(project_root / 'config' / 'stockport\.json'\) as f:\s+config = json\.load\(f\)",
        "# Configuration loaded from databricks_settings\nconfig = STOCKPORT_CONFIG",
        content
    )

    return content

def convert_file_paths(content):
    """Convert local file paths to DBFS/Delta Lake paths"""

    # Bronze layer - save to Delta tables instead of local files
    content = re.sub(
        r"output_dir = project_root / 'data' / 'bronze' / 'stockport' / '(\w+)'",
        r"output_dir = BRONZE_BASE_PATH + '/stockport/\1'",
        content
    )

    # Add Delta Lake save commands
    if 'save_to_bronze_parquet' in content:
        content = content.replace(
            'save_to_bronze_parquet(',
            '''# Save to local parquet first
parquet_path = save_to_bronze_parquet('''
        )

        # Add Delta Lake save after parquet
        content = content.replace(
            'print(f"✅ Bronze Layer Complete")',
            '''# Also save to Delta Lake table
if parquet_path:
    import pandas as pd
    df = pd.read_parquet(parquet_path)
    spark_df = spark.createDataFrame(df)

    # Determine table name based on data type
    if 'infrastructure' in parquet_path:
        table_name = BRONZE_TABLES['infrastructure']
        partition_cols = ['ingestion_date', 'infrastructure_type']
    elif 'crime' in parquet_path:
        table_name = BRONZE_TABLES['crime']
        partition_cols = ['ingestion_date', 'month']
    elif 'weather' in parquet_path:
        table_name = BRONZE_TABLES['weather']
        partition_cols = ['ingestion_date']
    elif 'postcode' in parquet_path:
        table_name = BRONZE_TABLES['postcodes']
        partition_cols = ['ingestion_date']

    spark_df.write \\
        .format("delta") \\
        .mode("append") \\
        .partitionBy(*partition_cols) \\
        .option("mergeSchema", "true") \\
        .saveAsTable(table_name)

    print(f"✅ Saved to Delta table: {table_name}")

print(f"✅ Bronze Layer Complete")'''
        )

    # Silver layer - read from Delta, write to Delta
    if 'pd.read_parquet' in content and 'bronze' in content:
        content = re.sub(
            r"df = pd\.read_parquet\('data/bronze/[^']+'\)",
            "df = spark.table(BRONZE_TABLES['infrastructure']).toPandas()",
            content
        )

    return content

def add_display_commands(content):
    """Add Databricks display() commands for better visualization"""

    # After data loading, add display
    content = re.sub(
        r'print\(f"Total \w+ records: \{len\(df\)\}"\)',
        lambda m: m.group(0) + '\n\n# COMMAND ----------\n# Display sample data\ndisplay(spark.createDataFrame(df.head(100)))',
        content
    )

    return content

def split_into_cells(content):
    """Split long code blocks into Databricks cells"""

    # Add cell separators at logical points
    markers = [
        ('# CONFIGURATION', '# COMMAND ----------\n# MAGIC %md\n# MAGIC ## Configuration\n\n# COMMAND ----------'),
        ('# STEP 1:', '# COMMAND ----------\n# MAGIC %md\n# MAGIC ## Step 1: Data Collection\n\n# COMMAND ----------'),
        ('# STEP 2:', '# COMMAND ----------\n# MAGIC %md\n# MAGIC ## Step 2: Data Processing\n\n# COMMAND ----------'),
        ('# STEP 3:', '# COMMAND ----------\n# MAGIC %md\n# MAGIC ## Step 3: Save to Bronze Layer\n\n# COMMAND ----------'),
        ('if __name__ == "__main__":', '# COMMAND ----------\n# MAGIC %md\n# MAGIC ## Main Execution\n\n# COMMAND ----------'),
    ]

    for marker, replacement in markers:
        content = content.replace(marker, replacement)

    return content

def convert_notebook(input_path, output_path):
    """Convert a single notebook to Databricks format"""

    print(f"Converting: {input_path.name}")

    # Read original notebook
    with open(input_path, 'r') as f:
        original_content = f.read()

    # Get metadata
    metadata = get_notebook_metadata(input_path.name)

    # Create header
    header = DATABRICKS_HEADER.format(
        title=metadata['title'],
        description=metadata['description'],
        original_filename=input_path.name
    )

    # Strip docstring from original
    content = re.sub(r'^"""[\s\S]*?"""', '', original_content, count=1).lstrip()

    # Apply conversions
    content = convert_imports(content)
    content = convert_file_paths(content)
    content = add_display_commands(content)
    content = split_into_cells(content)

    # Combine header and converted content
    final_content = header + content

    # Add final verification cell
    final_content += """

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------
# Show data statistics
print("="*80)
print("Data Processing Complete! ✅")
print("="*80)

# COMMAND ----------
"""

    # Write converted notebook
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(final_content)

    print(f"  ✅ Created: {output_path}")

def main():
    """Main conversion function"""

    print("="*80)
    print("🔄 Converting Notebooks to Databricks Format")
    print("="*80)
    print()

    # Setup paths
    project_root = Path(__file__).parent
    notebooks_dir = project_root / 'notebooks'
    databricks_dir = project_root / 'databricks_notebooks'

    # Check if notebooks directory exists
    if not notebooks_dir.exists():
        print("❌ Error: notebooks/ directory not found")
        return

    # Get all notebook files
    notebook_files = sorted(notebooks_dir.glob('*.py'))

    if not notebook_files:
        print("❌ No notebook files found in notebooks/")
        return

    print(f"📁 Found {len(notebook_files)} notebooks to convert")
    print()

    # Convert each notebook
    for notebook_file in notebook_files:
        # Determine output directory based on notebook type
        if 'bronze' in notebook_file.name:
            output_dir = databricks_dir / 'bronze'
        elif 'silver' in notebook_file.name:
            output_dir = databricks_dir / 'silver'
        elif 'gold' in notebook_file.name:
            output_dir = databricks_dir / 'gold'
        else:
            output_dir = databricks_dir

        output_path = output_dir / notebook_file.name

        convert_notebook(notebook_file, output_path)

    print()
    print("="*80)
    print("✅ Conversion Complete!")
    print("="*80)
    print()
    print("Next steps:")
    print("  1. Review converted notebooks in databricks_notebooks/")
    print("  2. Update paths: Replace 'YOUR_EMAIL' with your Databricks email")
    print("  3. Test in Databricks workspace")
    print()
    print("Files created:")
    for subdir in ['bronze', 'silver', 'gold']:
        dir_path = databricks_dir / subdir
        if dir_path.exists():
            files = list(dir_path.glob('*.py'))
            if files:
                print(f"\n  {subdir}/:")
                for f in sorted(files):
                    print(f"    - {f.name}")

if __name__ == '__main__':
    main()
