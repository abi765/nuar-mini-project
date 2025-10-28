# Databricks notebook source
# MAGIC %md
# MAGIC # Fix Table Schema - Move Tables from default to bronze
# MAGIC
# MAGIC **Purpose**: Move tables from `nuar_catalog.default` to `nuar_catalog.bronze`

# COMMAND ----------

print("🔧 FIXING TABLE SCHEMA LOCATION")
print("=" * 80)
print()

# Tables that need to be moved
tables_to_fix = ['infrastructure', 'crime', 'weather', 'postcodes']

# Target schema
TARGET_SCHEMA = "nuar_catalog.bronze"

# Ensure bronze schema exists
spark.sql("CREATE SCHEMA IF NOT EXISTS nuar_catalog.bronze")
print("✅ Schema nuar_catalog.bronze ready")
print()

# COMMAND ----------

print("📊 Moving tables to bronze schema...")
print()

for table in tables_to_fix:
    old_table = f"nuar_catalog.default.{table}"
    new_table = f"nuar_catalog.bronze.{table}"

    print(f"📦 {table}")
    print(f"   From: {old_table}")
    print(f"   To:   {new_table}")

    try:
        # Check if old table exists
        spark.sql(f"DESCRIBE TABLE {old_table}")

        # Drop new table if exists
        spark.sql(f"DROP TABLE IF EXISTS {new_table}")

        # Create new table in bronze schema pointing to same data location
        spark.sql(f"""
            CREATE TABLE {new_table}
            USING DELTA
            AS SELECT * FROM {old_table}
        """)

        # Drop old table
        spark.sql(f"DROP TABLE {old_table}")

        print(f"   ✅ Moved successfully")

    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            print(f"   ⚠️  Table not found in default schema")
        else:
            print(f"   ❌ Error: {e}")

    print()

# COMMAND ----------

print("=" * 80)
print("✅ SCHEMA FIX COMPLETE")
print("=" * 80)
print()

# Verify bronze schema tables
print("📋 Tables in nuar_catalog.bronze:")
print()

try:
    tables = spark.sql("SHOW TABLES IN nuar_catalog.bronze").collect()
    for row in tables:
        print(f"   ✅ {row.tableName}")
except Exception as e:
    print(f"   ❌ Error listing tables: {e}")

print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data

# COMMAND ----------

print("🔍 Verifying Bronze tables...")
print()

for table in tables_to_fix:
    full_table = f"nuar_catalog.bronze.{table}"

    try:
        df = spark.table(full_table)
        count = df.count()
        columns = len(df.columns)

        print(f"✅ {table}: {count} records, {columns} columns")

    except Exception as e:
        print(f"❌ {table}: {e}")

print()
print("✅ Bronze schema is ready!")
print()
print("Next: Run Silver layer notebooks")
