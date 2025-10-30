# Databricks notebook source
"""
Gold Layer: Postcode Enrichment Analysis
========================================
Enriches infrastructure and crime data with postcode-level aggregations.

Creates tables:
- nuar_catalog.gold.postcode_infrastructure_summary: Infrastructure counts per postcode
- nuar_catalog.gold.postcode_crime_summary: Crime counts per postcode
- nuar_catalog.gold.postcode_area_profile: Combined area profiles with risk scores

Author: NUAR Mini Project
"""

# COMMAND ----------

print("=" * 80)
print("📮 GOLD LAYER: Postcode Enrichment Analysis")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Configuration
CATALOG = "nuar_catalog"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
TARGET_SCHEMA = "gold"

print(f"📦 Catalog: {CATALOG}")
print(f"📥 Sources: silver.infrastructure, silver.crime, silver.postcodes")
print(f"📤 Target: postcode_infrastructure_summary, postcode_crime_summary, postcode_area_profile")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Silver Data

# COMMAND ----------

print("\n" + "=" * 80)
print("📊 Loading Silver Layer Data")
print("=" * 80)

try:
    # Load infrastructure
    df_infrastructure = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.infrastructure")
    infra_count = df_infrastructure.count()
    print(f"✅ Loaded {infra_count:,} infrastructure records")

    # Load crime
    df_crime = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.crime")
    crime_count = df_crime.count()
    print(f"✅ Loaded {crime_count:,} crime records")

    # Load postcodes
    df_postcodes = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.postcodes")
    postcode_count = df_postcodes.count()
    print(f"✅ Loaded {postcode_count:,} postcode records")

except Exception as e:
    print(f"❌ ERROR loading data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Infrastructure Summary by Postcode

# COMMAND ----------

print("\n" + "=" * 80)
print("🏗️  Calculating Infrastructure Summary by Postcode")
print("=" * 80)

# Filter infrastructure with postcodes
df_infra_with_postcode = df_infrastructure.filter(
    F.col("postcode").isNotNull()
)

print(f"Infrastructure records with postcodes: {df_infra_with_postcode.count():,}")

# Aggregate by postcode
df_postcode_infra = df_infra_with_postcode.groupBy("postcode").agg(
    # Counts
    F.count("*").alias("total_infrastructure_count"),
    F.countDistinct("infrastructure_type").alias("infrastructure_type_count"),

    # By type
    F.sum(F.when(F.col("infrastructure_type") == "power", 1).otherwise(0)).alias("power_count"),
    F.sum(F.when(F.col("infrastructure_type") == "communication", 1).otherwise(0)).alias("communication_count"),
    F.sum(F.when(F.col("infrastructure_type") == "water", 1).otherwise(0)).alias("water_count"),
    F.sum(F.when(F.col("infrastructure_type") == "gas", 1).otherwise(0)).alias("gas_count"),
    F.sum(F.when(F.col("infrastructure_type") == "sewerage", 1).otherwise(0)).alias("sewerage_count"),
    F.sum(F.when(F.col("infrastructure_type") == "fuel", 1).otherwise(0)).alias("fuel_count"),

    # Quality metrics
    F.avg("metadata_quality_score").alias("avg_quality_score"),
    F.sum(F.when(F.col("quality_tier") == "high", 1).otherwise(0)).alias("high_quality_count"),
    F.sum(F.when(F.col("quality_tier") == "medium", 1).otherwise(0)).alias("medium_quality_count"),
    F.sum(F.when(F.col("quality_tier") == "basic", 1).otherwise(0)).alias("basic_quality_count"),

    # Coverage
    F.sum(F.when(F.col("has_coordinates").cast("int") == 1, 1).otherwise(0)).alias("with_coordinates_count"),
    F.sum(F.when(F.col("has_bng_coords").cast("int") == 1, 1).otherwise(0)).alias("with_bng_coords_count"),

    # Infrastructure types present
    F.collect_set("infrastructure_type").alias("infrastructure_types")
)

# Calculate percentages
df_postcode_infra = df_postcode_infra.withColumn(
    "high_quality_percent",
    F.round((F.col("high_quality_count") / F.col("total_infrastructure_count")) * 100, 1)
).withColumn(
    "coordinate_coverage_percent",
    F.round((F.col("with_coordinates_count") / F.col("total_infrastructure_count")) * 100, 1)
)

# Infrastructure diversity score
df_postcode_infra = df_postcode_infra.withColumn(
    "infrastructure_diversity_score",
    F.col("infrastructure_type_count") / 6.0  # Normalize by max types (6)
)

print(f"✅ Summarized infrastructure for {df_postcode_infra.count():,} postcodes")

# Show sample
print("\n📊 Sample postcode infrastructure summary:")
df_postcode_infra.select(
    "postcode",
    "total_infrastructure_count",
    "infrastructure_type_count",
    "avg_quality_score",
    "infrastructure_diversity_score"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Crime Summary by Postcode

# COMMAND ----------

print("\n" + "=" * 80)
print("🚨 Calculating Crime Summary by Postcode")
print("=" * 80)

# Filter crime with postcodes
df_crime_with_postcode = df_crime.filter(
    F.col("postcode").isNotNull()
)

print(f"Crime records with postcodes: {df_crime_with_postcode.count():,}")

# Aggregate by postcode
df_postcode_crime = df_crime_with_postcode.groupBy("postcode").agg(
    # Total crime count
    F.count("*").alias("total_crime_count"),
    F.countDistinct("category").alias("crime_category_count"),

    # By category
    F.sum(F.when(F.col("category").contains("violence"), 1).otherwise(0)).alias("violence_count"),
    F.sum(F.when(F.col("category").contains("burglary"), 1).otherwise(0)).alias("burglary_count"),
    F.sum(F.when(F.col("category").contains("vehicle"), 1).otherwise(0)).alias("vehicle_crime_count"),
    F.sum(F.when(F.col("category").contains("theft"), 1).otherwise(0)).alias("theft_count"),
    F.sum(F.when(F.col("category").contains("drugs"), 1).otherwise(0)).alias("drugs_count"),
    F.sum(F.when(F.col("category").contains("antisocial"), 1).otherwise(0)).alias("antisocial_count"),

    # Outcome analysis
    F.sum(F.when(F.col("outcome_category").isNotNull(), 1).otherwise(0)).alias("crimes_with_outcome"),

    # Crime categories list
    F.collect_set("category").alias("crime_categories")
)

# Calculate outcome rate
df_postcode_crime = df_postcode_crime.withColumn(
    "outcome_rate_percent",
    F.round((F.col("crimes_with_outcome") / F.col("total_crime_count")) * 100, 1)
)

# Crime severity score (weighted by type)
df_postcode_crime = df_postcode_crime.withColumn(
    "crime_severity_score",
    (
        (F.col("violence_count") * 3) +  # High severity
        (F.col("burglary_count") * 2) +  # Medium-high severity
        (F.col("vehicle_crime_count") * 2) +
        (F.col("theft_count") * 1) +  # Medium severity
        (F.col("drugs_count") * 1.5) +
        (F.col("antisocial_count") * 1)
    ) / F.col("total_crime_count")
)

print(f"✅ Summarized crime for {df_postcode_crime.count():,} postcodes")

# Show sample
print("\n📊 Sample postcode crime summary:")
df_postcode_crime.select(
    "postcode",
    "total_crime_count",
    "crime_category_count",
    "violence_count",
    "crime_severity_score"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Join with Postcode Reference Data

# COMMAND ----------

print("\n" + "=" * 80)
print("🔗 Joining with Postcode Reference Data")
print("=" * 80)

# Select relevant postcode attributes
df_postcode_ref = df_postcodes.select(
    "postcode",
    "admin_district",
    "admin_ward",
    "parish",
    "constituency",
    "region",
    "country",
    "easting_bng",
    "northing_bng",
    "quality_score"  # Postcode quality score
).withColumnRenamed("quality_score", "postcode_quality_score")

# Join infrastructure summary
df_postcode_profile = df_postcode_ref.join(
    df_postcode_infra,
    on="postcode",
    how="left"
)

# Join crime summary
df_postcode_profile = df_postcode_profile.join(
    df_postcode_crime,
    on="postcode",
    how="left"
)

# Fill nulls for areas with no infrastructure or crime
df_postcode_profile = df_postcode_profile.fillna({
    "total_infrastructure_count": 0,
    "infrastructure_type_count": 0,
    "infrastructure_diversity_score": 0.0,
    "avg_quality_score": 0.0,
    "total_crime_count": 0,
    "crime_category_count": 0,
    "crime_severity_score": 0.0,
    "outcome_rate_percent": 0.0
})

print(f"✅ Created profile for {df_postcode_profile.count():,} postcodes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Calculate Risk Scores

# COMMAND ----------

print("\n" + "=" * 80)
print("📊 Calculating Risk Scores")
print("=" * 80)

# Normalize crime count (higher = higher risk)
max_crime = df_postcode_profile.agg(F.max("total_crime_count")).collect()[0][0]
df_postcode_profile = df_postcode_profile.withColumn(
    "normalized_crime_risk",
    F.when(max_crime > 0, F.col("total_crime_count") / max_crime).otherwise(0.0)
)

# Normalize infrastructure vulnerability (higher crime + lower quality = higher risk)
df_postcode_profile = df_postcode_profile.withColumn(
    "infrastructure_vulnerability_score",
    F.when(
        F.col("total_infrastructure_count") > 0,
        F.col("normalized_crime_risk") * (1 - F.col("avg_quality_score"))
    ).otherwise(0.0)
)

# Combined risk score (0-100)
df_postcode_profile = df_postcode_profile.withColumn(
    "area_risk_score",
    F.round(
        (F.col("normalized_crime_risk") * 50) +  # Crime contributes 50 points
        (F.col("infrastructure_vulnerability_score") * 50),  # Vulnerability contributes 50 points
        1
    )
)

# Risk classification
df_postcode_profile = df_postcode_profile.withColumn(
    "risk_classification",
    F.when(F.col("area_risk_score") >= 70, "high_risk")
     .when(F.col("area_risk_score") >= 40, "medium_risk")
     .when(F.col("area_risk_score") >= 10, "low_risk")
     .otherwise("minimal_risk")
)

# Safety index (inverse of risk)
df_postcode_profile = df_postcode_profile.withColumn(
    "safety_index",
    100 - F.col("area_risk_score")
)

print("✅ Calculated risk scores")

# Show risk distribution
print("\n📊 Risk Classification Distribution:")
df_postcode_profile.groupBy("risk_classification").count().orderBy(F.col("count").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Save Summaries to Gold

# COMMAND ----------

print("\n" + "=" * 80)
print("💾 Saving Postcode Summaries to Gold")
print("=" * 80)

# Save infrastructure summary
try:
    table_name = f"{CATALOG}.{TARGET_SCHEMA}.postcode_infrastructure_summary"
    df_postcode_infra.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    print(f"✅ Saved postcode_infrastructure_summary: {df_postcode_infra.count():,} records")
except Exception as e:
    print(f"❌ ERROR saving infrastructure summary: {str(e)}")
    raise

# Save crime summary
try:
    table_name = f"{CATALOG}.{TARGET_SCHEMA}.postcode_crime_summary"
    df_postcode_crime.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    print(f"✅ Saved postcode_crime_summary: {df_postcode_crime.count():,} records")
except Exception as e:
    print(f"❌ ERROR saving crime summary: {str(e)}")
    raise

# Save area profile
try:
    table_name = f"{CATALOG}.{TARGET_SCHEMA}.postcode_area_profile"
    df_postcode_profile.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)
    print(f"✅ Saved postcode_area_profile: {df_postcode_profile.count():,} records")
except Exception as e:
    print(f"❌ ERROR saving area profile: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. High-Risk Postcode Analysis

# COMMAND ----------

print("\n" + "=" * 80)
print("🚨 High-Risk Postcode Analysis")
print("=" * 80)

# Filter high-risk postcodes
df_high_risk_postcodes = df_postcode_profile.filter(
    F.col("risk_classification") == "high_risk"
).orderBy(F.col("area_risk_score").desc())

print(f"High-risk postcodes: {df_high_risk_postcodes.count():,}")

# Show top 20 highest risk
print("\n🚨 Top 20 Highest Risk Postcodes:")
df_high_risk_postcodes.select(
    "postcode",
    "admin_district",
    "admin_ward",
    "total_infrastructure_count",
    "total_crime_count",
    "area_risk_score",
    "safety_index"
).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary Statistics

# COMMAND ----------

print("\n" + "=" * 80)
print("📊 SUMMARY - Postcode Enrichment Analysis")
print("=" * 80)

# Overall statistics
total_postcodes = df_postcode_profile.count()
postcodes_with_infra = df_postcode_profile.filter(F.col("total_infrastructure_count") > 0).count()
postcodes_with_crime = df_postcode_profile.filter(F.col("total_crime_count") > 0).count()

print(f"\n📮 Postcode Coverage:")
print(f"   Total Postcodes: {total_postcodes:,}")
print(f"   With Infrastructure: {postcodes_with_infra:,} ({postcodes_with_infra/total_postcodes*100:.1f}%)")
print(f"   With Crime Records: {postcodes_with_crime:,} ({postcodes_with_crime/total_postcodes*100:.1f}%)")

# Average metrics
avg_stats = df_postcode_profile.agg(
    F.avg("total_infrastructure_count").alias("avg_infra"),
    F.avg("total_crime_count").alias("avg_crime"),
    F.avg("area_risk_score").alias("avg_risk"),
    F.avg("safety_index").alias("avg_safety")
).collect()[0]

print(f"\n📊 Average Metrics:")
print(f"   Infrastructure per Postcode: {avg_stats['avg_infra']:.1f}")
print(f"   Crime per Postcode: {avg_stats['avg_crime']:.1f}")
print(f"   Risk Score: {avg_stats['avg_risk']:.1f}/100")
print(f"   Safety Index: {avg_stats['avg_safety']:.1f}/100")

# Top districts
print("\n🏛️  Top Districts by Infrastructure:")
df_postcode_profile.groupBy("admin_district").agg(
    F.sum("total_infrastructure_count").alias("total_infra"),
    F.count("*").alias("postcodes")
).orderBy(F.col("total_infra").desc()).show(5, truncate=False)

print("\n🚨 Top Districts by Crime:")
df_postcode_profile.groupBy("admin_district").agg(
    F.sum("total_crime_count").alias("total_crime"),
    F.count("*").alias("postcodes")
).orderBy(F.col("total_crime").desc()).show(5, truncate=False)

print(f"\n💾 Output Tables:")
print(f"   {CATALOG}.{TARGET_SCHEMA}.postcode_infrastructure_summary")
print(f"   {CATALOG}.{TARGET_SCHEMA}.postcode_crime_summary")
print(f"   {CATALOG}.{TARGET_SCHEMA}.postcode_area_profile")
print(f"   Storage: Unity Catalog managed tables")

print("\n" + "=" * 80)
print("✅ Postcode Enrichment Analysis Complete!")
print("=" * 80)
