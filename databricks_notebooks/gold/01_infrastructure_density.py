# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Infrastructure Density Analysis
# MAGIC
# MAGIC **Purpose**: Calculate infrastructure density metrics for Stockport area
# MAGIC
# MAGIC **Aggregations**:
# MAGIC - Infrastructure count by type and grid cell
# MAGIC - Density per square kilometer
# MAGIC - Quality distribution analysis

# COMMAND ----------

from pyspark.sql import functions as F, Window
from datetime import datetime
import math

print("=" * 80)
print("🥇 GOLD LAYER: INFRASTRUCTURE DENSITY ANALYSIS")
print("=" * 80)
print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Silver Data

# COMMAND ----------

print("📥 Loading Silver infrastructure data...")
df_infrastructure = spark.table("nuar_catalog.silver.infrastructure")

record_count = df_infrastructure.count()
print(f"✅ Loaded {record_count} infrastructure records")
print()

# Show sample
print("Sample data:")
df_infrastructure.select("infrastructure_type", "easting_bng", "northing_bng", "quality_tier").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Grid Cells (1km x 1km)

# COMMAND ----------

print("📐 Creating 1km grid cells...")
print()

# Create grid cell IDs (1000m = 1km cells)
grid_size = 1000  # meters

df_grid = df_infrastructure.withColumn(
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
)

# Calculate cell center coordinates
df_grid = df_grid.withColumn(
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
# MAGIC ## Step 3: Calculate Infrastructure Density by Grid Cell

# COMMAND ----------

print("📊 Calculating infrastructure density...")
print()

# Count by grid cell and type
df_density = df_grid.groupBy(
    "grid_cell_id",
    "grid_center_easting",
    "grid_center_northing",
    "infrastructure_type"
).agg(
    F.count("*").alias("infrastructure_count"),
    F.avg("metadata_quality_score").alias("avg_quality_score"),
    F.collect_set("quality_tier").alias("quality_tiers")
)

# Add density per km²
grid_area_km2 = (grid_size / 1000) ** 2  # Convert to km²
df_density = df_density.withColumn(
    "density_per_km2",
    F.col("infrastructure_count") / grid_area_km2
)

# Add analysis timestamp
df_density = df_density.withColumn(
    "analysis_date",
    F.lit(datetime.now().strftime("%Y-%m-%d"))
).withColumn(
    "analysis_timestamp",
    F.lit(datetime.now().isoformat())
)

print(f"✅ Calculated density for {df_density.count()} grid cells")
print()

# Show top density areas
print("Top 5 highest density grid cells:")
df_density.orderBy(F.desc("density_per_km2")).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Calculate Summary Statistics

# COMMAND ----------

print("📈 Calculating summary statistics...")
print()

# Overall statistics by infrastructure type
df_stats = df_grid.groupBy("infrastructure_type").agg(
    F.count("*").alias("total_count"),
    F.countDistinct("grid_cell_id").alias("grid_cells_covered"),
    F.avg("metadata_quality_score").alias("avg_quality"),
    F.sum(F.when(F.col("quality_tier") == "high", 1).otherwise(0)).alias("high_quality_count"),
    F.sum(F.when(F.col("quality_tier") == "medium", 1).otherwise(0)).alias("medium_quality_count"),
    F.sum(F.when(F.col("quality_tier") == "basic", 1).otherwise(0)).alias("basic_quality_count")
).withColumn(
    "analysis_date",
    F.lit(datetime.now().strftime("%Y-%m-%d"))
)

print("Infrastructure type summary:")
df_stats.orderBy(F.desc("total_count")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Save to Gold Layer

# COMMAND ----------

print("💾 Saving Gold layer tables...")
print()

# Save density table
table_name_density = "nuar_catalog.gold.infrastructure_density"
df_density.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name_density)

print(f"✅ Saved: {table_name_density}")
print(f"   Records: {df_density.count()}")
print()

# Save stats table
table_name_stats = "nuar_catalog.gold.infrastructure_stats"
df_stats.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name_stats)

print(f"✅ Saved: {table_name_stats}")
print(f"   Records: {df_stats.count()}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 80)
print("✅ GOLD LAYER COMPLETE: INFRASTRUCTURE DENSITY")
print("=" * 80)
print()

print("Tables created:")
print(f"  1. {table_name_density}")
print(f"  2. {table_name_stats}")
print()

print("Key metrics:")
print(f"  - Total infrastructure: {record_count}")
print(f"  - Grid cells analyzed: {grid_count}")
print(f"  - Grid resolution: {grid_size}m x {grid_size}m")
print(f"  - Density metric: items per km²")
print()

print("Next steps:")
print("  1. Query gold.infrastructure_density for spatial analysis")
print("  2. Create visualizations in next notebook")
print("  3. Run crime hotspot analysis")
print()

print(f"⏱️  Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)
