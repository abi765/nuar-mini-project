# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Crime Hotspot Analysis
# MAGIC
# MAGIC **Purpose**: Identify crime hotspots in Stockport area
# MAGIC
# MAGIC **Aggregations**:
# MAGIC - Crime count by grid cell and category
# MAGIC - Hotspot identification
# MAGIC - Temporal patterns

# COMMAND ----------

from pyspark.sql import functions as F, Window
from datetime import datetime

print("=" * 80)
print("🥇 GOLD LAYER: CRIME HOTSPOT ANALYSIS")
print("=" * 80)
print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Silver Crime Data

# COMMAND ----------

print("📥 Loading Silver crime data...")
df_crime = spark.table("nuar_catalog.silver.crime")

record_count = df_crime.count()
print(f"✅ Loaded {record_count} crime records")
print()

# Show sample
print("Sample data:")
df_crime.select("category", "month", "easting_bng", "northing_bng").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Grid Cells (500m x 500m)

# COMMAND ----------

print("📐 Creating 500m grid cells for crime analysis...")
print()

# Smaller grid for crime (500m cells)
grid_size = 500  # meters

df_grid = df_crime.withColumn(
    "grid_cell_x",
    (F.col("easting_bng") / grid_size).cast("int")
).withColumn(
    "grid_cell_y",
    (F.col("northing_bng") / grid_size).cast("int")
).withColumn(
    "grid_cell_id",
    F.concat(
        F.lit("E"),
        F.col("grid_cell_x"),
        F.lit("N"),
        F.col("grid_cell_y")
    )
).withColumn(
    "grid_center_easting",
    (F.col("grid_cell_x") * grid_size) + (grid_size / 2)
).withColumn(
    "grid_center_northing",
    (F.col("grid_cell_y") * grid_size) + (grid_size / 2)
)

grid_count = df_grid.select("grid_cell_id").distinct().count()
print(f"✅ Created grid: {grid_count} cells of {grid_size}m x {grid_size}m")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Calculate Crime Density

# COMMAND ----------

print("📊 Calculating crime density by grid cell...")
print()

# Count by grid cell and category
df_hotspots = df_grid.groupBy(
    "grid_cell_id",
    "grid_center_easting",
    "grid_center_northing",
    "category"
).agg(
    F.count("*").alias("crime_count"),
    F.countDistinct("month").alias("months_with_crimes"),
    F.collect_set("month").alias("crime_months")
)

# Calculate density per km²
grid_area_km2 = (grid_size / 1000) ** 2
df_hotspots = df_hotspots.withColumn(
    "density_per_km2",
    F.col("crime_count") / grid_area_km2
)

# Add hotspot classification
df_hotspots = df_hotspots.withColumn(
    "hotspot_level",
    F.when(F.col("density_per_km2") >= 20, "high")
     .when(F.col("density_per_km2") >= 10, "medium")
     .otherwise("low")
)

# Add timestamp
df_hotspots = df_hotspots.withColumn(
    "analysis_date",
    F.lit(datetime.now().strftime("%Y-%m-%d"))
).withColumn(
    "analysis_timestamp",
    F.lit(datetime.now().isoformat())
)

print(f"✅ Analyzed {df_hotspots.count()} grid cell + category combinations")
print()

# Show hotspots
print("Top 5 crime hotspots:")
df_hotspots.orderBy(F.desc("density_per_km2")).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Calculate Summary by Category

# COMMAND ----------

print("📈 Calculating crime statistics by category...")
print()

df_crime_stats = df_grid.groupBy("category").agg(
    F.count("*").alias("total_crimes"),
    F.countDistinct("grid_cell_id").alias("grid_cells_affected"),
    F.countDistinct("month").alias("months_observed"),
    F.countDistinct("postcode").alias("postcodes_affected")
).withColumn(
    "analysis_date",
    F.lit(datetime.now().strftime("%Y-%m-%d"))
)

print("Crime statistics by category:")
df_crime_stats.orderBy(F.desc("total_crimes")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Save to Gold Layer

# COMMAND ----------

print("💾 Saving Gold layer tables...")
print()

# Save hotspots table
table_name_hotspots = "nuar_catalog.gold.crime_hotspots"
df_hotspots.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name_hotspots)

print(f"✅ Saved: {table_name_hotspots}")
print(f"   Records: {df_hotspots.count()}")
print()

# Save stats table
table_name_stats = "nuar_catalog.gold.crime_stats"
df_crime_stats.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name_stats)

print(f"✅ Saved: {table_name_stats}")
print(f"   Records: {df_crime_stats.count()}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 80)
print("✅ GOLD LAYER COMPLETE: CRIME HOTSPOTS")
print("=" * 80)
print()

print("Tables created:")
print(f"  1. {table_name_hotspots}")
print(f"  2. {table_name_stats}")
print()

print("Key metrics:")
print(f"  - Total crimes: {record_count}")
print(f"  - Grid cells analyzed: {grid_count}")
print(f"  - Grid resolution: {grid_size}m x {grid_size}m")
print()

print("Hotspot levels:")
hotspot_summary = df_hotspots.groupBy("hotspot_level").count().collect()
for row in hotspot_summary:
    print(f"  - {row['hotspot_level']}: {row['count']} areas")
print()

print("Next steps:")
print("  1. Create visualization of hotspots")
print("  2. Cross-reference with infrastructure density")
print()

print(f"⏱️  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)
