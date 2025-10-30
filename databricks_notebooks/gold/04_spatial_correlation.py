# Databricks notebook source
"""
Gold Layer: Spatial Correlation Analysis
========================================
Analyzes spatial relationships between infrastructure and crime patterns.

Creates tables:
- nuar_catalog.gold.infrastructure_crime_correlation: Grid-based correlation analysis
- nuar_catalog.gold.high_risk_infrastructure_zones: Areas with high crime near infrastructure

Author: NUAR Mini Project
"""

# COMMAND ----------

print("=" * 80)
print("🗺️  GOLD LAYER: Spatial Correlation Analysis")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import math

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Configuration
CATALOG = "nuar_catalog"
SOURCE_SCHEMA = "gold"
TARGET_SCHEMA = "gold"

# Grid configuration (use 1km grid for correlation)
GRID_SIZE = 1000  # meters

print(f"📦 Catalog: {CATALOG}")
print(f"📥 Sources: infrastructure_density, crime_hotspots")
print(f"📤 Target: infrastructure_crime_correlation, high_risk_infrastructure_zones")
print(f"🔲 Grid Size: {GRID_SIZE}m ({GRID_SIZE/1000}km)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Gold Layer Data

# COMMAND ----------

print("\n" + "=" * 80)
print("📊 Loading Gold Layer Data")
print("=" * 80)

try:
    # Load infrastructure density (1km grid)
    df_infra_density = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.infrastructure_density")
    infra_count = df_infra_density.count()
    print(f"✅ Loaded {infra_count:,} infrastructure density records")

    # Load crime hotspots (500m grid - will aggregate to 1km)
    df_crime_hotspots = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.crime_hotspots")
    crime_count = df_crime_hotspots.count()
    print(f"✅ Loaded {crime_count:,} crime hotspot records")

except Exception as e:
    print(f"❌ ERROR loading data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Normalize Crime Data to 1km Grid

# COMMAND ----------

print("\n" + "=" * 80)
print("🔄 Normalizing Crime Data to 1km Grid")
print("=" * 80)

# Crime data is in 500m grid, aggregate to 1km grid to match infrastructure
df_crime_1km = df_crime_hotspots.withColumn(
    "grid_1km_easting",
    (F.col("grid_center_easting") / GRID_SIZE).cast("int") * GRID_SIZE
).withColumn(
    "grid_1km_northing",
    (F.col("grid_center_northing") / GRID_SIZE).cast("int") * GRID_SIZE
)

# Aggregate crime counts to 1km grid
df_crime_agg = df_crime_1km.groupBy(
    "grid_1km_easting",
    "grid_1km_northing"
).agg(
    F.sum("crime_count").alias("total_crime_count"),
    F.sum("density_per_km2").alias("total_crime_density"),
    F.collect_set("category").alias("crime_categories"),
    F.max(F.when(F.col("hotspot_level") == "high", 1)
           .when(F.col("hotspot_level") == "medium", 0.5)
           .otherwise(0)).alias("max_hotspot_score")
).withColumn(
    "crime_category_count",
    F.size("crime_categories")
).withColumn(
    "is_crime_hotspot",
    F.col("max_hotspot_score") >= 0.5
)

print(f"✅ Aggregated crime data to {df_crime_agg.count():,} 1km grid cells")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aggregate Infrastructure to 1km Grid

# COMMAND ----------

print("\n" + "=" * 80)
print("🏗️  Aggregating Infrastructure to 1km Grid")
print("=" * 80)

# Infrastructure density already has grid centers, aggregate by location
df_infra_agg = df_infra_density.groupBy(
    "grid_center_easting",
    "grid_center_northing"
).agg(
    F.sum("infrastructure_count").alias("total_infrastructure_count"),
    F.sum("density_per_km2").alias("total_infrastructure_density"),
    F.collect_set("infrastructure_type").alias("infrastructure_types"),
    F.avg("avg_quality_score").alias("avg_quality_score")  # This comes from infrastructure_density table
).withColumn(
    "infrastructure_type_count",
    F.size("infrastructure_types")
).withColumn(
    "infrastructure_diversity_score",
    F.col("infrastructure_type_count") / 10.0  # Normalize (assuming max ~10 types)
)

# Rename columns for join
df_infra_agg = df_infra_agg.withColumnRenamed("grid_center_easting", "grid_easting") \
                           .withColumnRenamed("grid_center_northing", "grid_northing")

print(f"✅ Aggregated infrastructure to {df_infra_agg.count():,} 1km grid cells")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Join Infrastructure and Crime Data

# COMMAND ----------

print("\n" + "=" * 80)
print("🔗 Joining Infrastructure and Crime Data")
print("=" * 80)

# Rename crime grid columns for join
df_crime_agg = df_crime_agg.withColumnRenamed("grid_1km_easting", "grid_easting") \
                           .withColumnRenamed("grid_1km_northing", "grid_northing")

# Full outer join to capture all grid cells
df_correlation = df_infra_agg.join(
    df_crime_agg,
    on=["grid_easting", "grid_northing"],
    how="full_outer"
)

# Fill nulls with 0 for counts
df_correlation = df_correlation.fillna({
    "total_infrastructure_count": 0,
    "total_infrastructure_density": 0.0,
    "infrastructure_type_count": 0,
    "infrastructure_diversity_score": 0.0,
    "avg_quality_score": 0.0,
    "total_crime_count": 0,
    "total_crime_density": 0.0,
    "crime_category_count": 0,
    "max_hotspot_score": 0.0,
    "is_crime_hotspot": False
})

# Create grid cell ID
df_correlation = df_correlation.withColumn(
    "grid_cell_id",
    F.concat(
        F.lit("grid_"),
        F.col("grid_easting"),
        F.lit("_"),
        F.col("grid_northing")
    )
)

print(f"✅ Joined data for {df_correlation.count():,} grid cells")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Calculate Correlation Metrics

# COMMAND ----------

print("\n" + "=" * 80)
print("📈 Calculating Correlation Metrics")
print("=" * 80)

# Calculate correlation score (normalized 0-1)
# Higher infrastructure density + higher crime density = higher correlation
df_correlation = df_correlation.withColumn(
    "normalized_infra_density",
    F.col("total_infrastructure_density") / 100.0  # Normalize assuming max ~100
).withColumn(
    "normalized_crime_density",
    F.col("total_crime_density") / 50.0  # Normalize assuming max ~50
)

# Simple correlation score: product of normalized densities
df_correlation = df_correlation.withColumn(
    "correlation_score",
    F.least(
        (F.col("normalized_infra_density") * F.col("normalized_crime_density")),
        F.lit(1.0)
    )
)

# Classification based on correlation
df_correlation = df_correlation.withColumn(
    "correlation_level",
    F.when(F.col("correlation_score") >= 0.7, "high")
     .when(F.col("correlation_score") >= 0.4, "medium")
     .when(F.col("correlation_score") >= 0.1, "low")
     .otherwise("none")
)

# Risk classification (high crime + high infrastructure)
df_correlation = df_correlation.withColumn(
    "risk_level",
    F.when(
        (F.col("is_crime_hotspot") == True) &
        (F.col("total_infrastructure_count") > 10),
        "high_risk"
    ).when(
        (F.col("total_crime_count") > 5) &
        (F.col("total_infrastructure_count") > 5),
        "medium_risk"
    ).when(
        (F.col("total_crime_count") > 0) |
        (F.col("total_infrastructure_count") > 0),
        "low_risk"
    ).otherwise("no_data")
)

# Calculate infrastructure vulnerability score
# Higher crime + lower quality infrastructure = higher vulnerability
df_correlation = df_correlation.withColumn(
    "infrastructure_vulnerability",
    F.when(
        F.col("total_infrastructure_count") > 0,
        (F.col("normalized_crime_density") * (1 - F.col("avg_quality_score")))
    ).otherwise(0.0)
)

# Add safety index (inverse of risk)
# High infrastructure + low crime = high safety
df_correlation = df_correlation.withColumn(
    "safety_index",
    F.when(
        F.col("total_infrastructure_count") > 0,
        (F.col("normalized_infra_density") * (1 - F.col("normalized_crime_density")))
    ).otherwise(0.0)
)

print("✅ Calculated correlation metrics")

# Show sample
print("\n📊 Sample correlation analysis:")
df_correlation.select(
    "grid_cell_id",
    "total_infrastructure_count",
    "total_crime_count",
    "correlation_score",
    "correlation_level",
    "risk_level",
    "safety_index"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Save Correlation Analysis to Gold

# COMMAND ----------

print("\n" + "=" * 80)
print("💾 Saving Correlation Analysis to Gold")
print("=" * 80)

try:
    table_name = f"{CATALOG}.{TARGET_SCHEMA}.infrastructure_crime_correlation"

    df_correlation.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)

    # Verify
    saved_count = spark.table(table_name).count()
    print(f"✅ Saved {saved_count:,} correlation records to {table_name}")

except Exception as e:
    print(f"❌ ERROR saving correlation analysis: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Identify High-Risk Infrastructure Zones

# COMMAND ----------

print("\n" + "=" * 80)
print("⚠️  Identifying High-Risk Infrastructure Zones")
print("=" * 80)

# Filter for high-risk areas
df_high_risk = df_correlation.filter(
    F.col("risk_level") == "high_risk"
).select(
    "grid_cell_id",
    "grid_easting",
    "grid_northing",
    "total_infrastructure_count",
    "infrastructure_types",
    "infrastructure_type_count",
    "avg_quality_score",
    "total_crime_count",
    "crime_categories",
    "crime_category_count",
    "max_hotspot_score",
    "correlation_score",
    "infrastructure_vulnerability",
    "safety_index"
).orderBy(F.col("infrastructure_vulnerability").desc())

# Add priority ranking
window = Window.orderBy(F.col("infrastructure_vulnerability").desc())
df_high_risk = df_high_risk.withColumn(
    "priority_rank",
    F.row_number().over(window)
)

# Add action recommendation
df_high_risk = df_high_risk.withColumn(
    "recommended_action",
    F.when(
        (F.col("avg_quality_score") < 0.5) & (F.col("max_hotspot_score") >= 1.0),
        "URGENT: Upgrade infrastructure security and quality"
    ).when(
        F.col("avg_quality_score") < 0.5,
        "Improve infrastructure quality and monitoring"
    ).when(
        F.col("max_hotspot_score") >= 1.0,
        "Enhance security measures in crime hotspot"
    ).otherwise("Regular monitoring and maintenance")
)

print(f"✅ Identified {df_high_risk.count():,} high-risk zones")

# Show top 10 highest priority zones
print("\n🚨 Top 10 High-Risk Infrastructure Zones:")
df_high_risk.select(
    "priority_rank",
    "grid_cell_id",
    "total_infrastructure_count",
    "total_crime_count",
    "infrastructure_vulnerability",
    "recommended_action"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Save High-Risk Zones to Gold

# COMMAND ----------

print("\n" + "=" * 80)
print("💾 Saving High-Risk Zones to Gold")
print("=" * 80)

try:
    table_name = f"{CATALOG}.{TARGET_SCHEMA}.high_risk_infrastructure_zones"

    df_high_risk.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)

    # Verify
    saved_count = spark.table(table_name).count()
    print(f"✅ Saved {saved_count:,} high-risk zone records to {table_name}")

except Exception as e:
    print(f"❌ ERROR saving high-risk zones: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary Statistics

# COMMAND ----------

print("\n" + "=" * 80)
print("📊 SUMMARY - Spatial Correlation Analysis")
print("=" * 80)

# Overall statistics
total_cells = df_correlation.count()
cells_with_infra = df_correlation.filter(F.col("total_infrastructure_count") > 0).count()
cells_with_crime = df_correlation.filter(F.col("total_crime_count") > 0).count()
high_correlation = df_correlation.filter(F.col("correlation_level") == "high").count()
high_risk_count = df_high_risk.count()

print(f"\n🔲 Grid Analysis:")
print(f"   Total Grid Cells: {total_cells:,}")
print(f"   Cells with Infrastructure: {cells_with_infra:,} ({cells_with_infra/total_cells*100:.1f}%)")
print(f"   Cells with Crime: {cells_with_crime:,} ({cells_with_crime/total_cells*100:.1f}%)")

print(f"\n📈 Correlation:")
print(f"   High Correlation Cells: {high_correlation:,}")

print(f"\n⚠️  Risk Assessment:")
print(f"   High-Risk Zones: {high_risk_count:,}")

# Correlation distribution
print("\n📊 Correlation Level Distribution:")
df_correlation.groupBy("correlation_level").count().orderBy(F.col("count").desc()).show()

# Risk distribution
print("\n⚡ Risk Level Distribution:")
df_correlation.groupBy("risk_level").count().orderBy(F.col("count").desc()).show()

print(f"\n💾 Output Tables:")
print(f"   {CATALOG}.{TARGET_SCHEMA}.infrastructure_crime_correlation")
print(f"   {CATALOG}.{TARGET_SCHEMA}.high_risk_infrastructure_zones")
print(f"   Storage: Unity Catalog managed tables")

print("\n" + "=" * 80)
print("✅ Spatial Correlation Analysis Complete!")
print("=" * 80)
