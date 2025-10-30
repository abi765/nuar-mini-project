# Databricks notebook source
"""
Gold Layer: Weather Trends Analysis
===================================
Analyzes weather patterns and trends from Silver weather data.

Creates tables:
- nuar_catalog.gold.weather_daily_stats: Daily weather aggregations
- nuar_catalog.gold.weather_monthly_trends: Monthly weather trends

Author: NUAR Mini Project
"""

# COMMAND ----------

print("=" * 80)
print("🌤️  GOLD LAYER: Weather Trends Analysis")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Configuration
CATALOG = "nuar_catalog"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

print(f"📦 Catalog: {CATALOG}")
print(f"📥 Source: {SOURCE_SCHEMA}.weather")
print(f"📤 Target: {TARGET_SCHEMA}.weather_daily_stats, weather_monthly_trends")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Silver Weather Data

# COMMAND ----------

print("\n" + "=" * 80)
print("📊 Loading Silver Weather Data")
print("=" * 80)

try:
    df_weather = spark.table(f"{CATALOG}.{SOURCE_SCHEMA}.weather")
    record_count = df_weather.count()
    print(f"✅ Loaded {record_count:,} weather records")

    # Show schema
    print("\n📋 Schema:")
    df_weather.printSchema()

    # Show sample
    print("\n📄 Sample data:")
    df_weather.show(5, truncate=False)

except Exception as e:
    print(f"❌ ERROR loading weather data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Daily Weather Statistics

# COMMAND ----------

print("\n" + "=" * 80)
print("📈 Calculating Daily Weather Statistics")
print("=" * 80)

# Extract date from timestamp
df_daily = df_weather.withColumn(
    "date",
    F.to_date(F.col("timestamp"))
)

# Calculate daily aggregations
df_daily_stats = df_daily.groupBy("date").agg(
    # Temperature stats (Celsius)
    F.avg("temperature_celsius").alias("avg_temperature"),
    F.min("temperature_celsius").alias("min_temperature"),
    F.max("temperature_celsius").alias("max_temperature"),

    # Feels like temperature
    F.avg("feels_like_celsius").alias("avg_feels_like"),

    # Humidity stats
    F.avg("humidity_percent").alias("avg_humidity"),
    F.min("humidity_percent").alias("min_humidity"),
    F.max("humidity_percent").alias("max_humidity"),

    # Pressure stats (hPa)
    F.avg("pressure_hpa").alias("avg_pressure"),
    F.min("pressure_hpa").alias("min_pressure"),
    F.max("pressure_hpa").alias("max_pressure"),

    # Wind stats (m/s)
    F.avg("wind_speed_ms").alias("avg_wind_speed"),
    F.max("wind_speed_ms").alias("max_wind_speed"),

    # Wind direction (degrees)
    F.avg("wind_direction_deg").alias("avg_wind_direction"),

    # Cloud coverage
    F.avg("clouds_percent").alias("avg_cloud_coverage"),

    # Visibility (meters)
    F.avg("visibility_meters").alias("avg_visibility"),

    # Rainfall (mm) - sum for daily total
    F.sum(F.coalesce(F.col("rain_1h_mm"), F.lit(0))).alias("total_rainfall_mm"),
    F.max(F.coalesce(F.col("rain_1h_mm"), F.lit(0))).alias("max_hourly_rainfall_mm"),

    # Snow (mm) - sum for daily total
    F.sum(F.coalesce(F.col("snow_1h_mm"), F.lit(0))).alias("total_snowfall_mm"),

    # Weather conditions - most common
    F.collect_list("weather_main").alias("weather_conditions_list"),

    # Record count
    F.count("*").alias("observation_count")
).orderBy("date")

# Add most common weather condition
df_daily_stats = df_daily_stats.withColumn(
    "dominant_weather",
    F.expr("array_max(weather_conditions_list)")  # Simplified - could use mode
)

# Calculate temperature range
df_daily_stats = df_daily_stats.withColumn(
    "temperature_range",
    F.col("max_temperature") - F.col("min_temperature")
)

# Add comfort classification
df_daily_stats = df_daily_stats.withColumn(
    "comfort_level",
    F.when((F.col("avg_temperature") >= 15) & (F.col("avg_temperature") <= 25) &
           (F.col("avg_humidity") >= 30) & (F.col("avg_humidity") <= 60) &
           (F.col("avg_wind_speed") < 5), "comfortable")
     .when((F.col("avg_temperature") < 5) | (F.col("avg_temperature") > 30), "extreme")
     .when((F.col("avg_humidity") > 80) | (F.col("avg_wind_speed") > 10), "uncomfortable")
     .otherwise("moderate")
)

# Drop intermediate columns
df_daily_stats = df_daily_stats.drop("weather_conditions_list")

print(f"✅ Calculated daily statistics for {df_daily_stats.count():,} days")

# Show sample
print("\n📊 Sample daily statistics:")
df_daily_stats.select(
    "date",
    "avg_temperature",
    "min_temperature",
    "max_temperature",
    "total_rainfall_mm",
    "dominant_weather",
    "comfort_level"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Save Daily Statistics to Gold

# COMMAND ----------

print("\n" + "=" * 80)
print("💾 Saving Daily Weather Statistics to Gold")
print("=" * 80)

try:
    table_name = f"{CATALOG}.{TARGET_SCHEMA}.weather_daily_stats"

    df_daily_stats.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)

    # Verify
    saved_count = spark.table(table_name).count()
    print(f"✅ Saved {saved_count:,} daily records to {table_name}")

except Exception as e:
    print(f"❌ ERROR saving daily statistics: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Monthly Weather Trends

# COMMAND ----------

print("\n" + "=" * 80)
print("📅 Calculating Monthly Weather Trends")
print("=" * 80)

# Extract year and month
df_monthly = df_daily_stats.withColumn(
    "year",
    F.year(F.col("date"))
).withColumn(
    "month",
    F.month(F.col("date"))
).withColumn(
    "year_month",
    F.date_format(F.col("date"), "yyyy-MM")
)

# Calculate monthly aggregations
df_monthly_trends = df_monthly.groupBy("year", "month", "year_month").agg(
    # Temperature trends
    F.avg("avg_temperature").alias("avg_temperature"),
    F.avg("min_temperature").alias("avg_min_temperature"),
    F.avg("max_temperature").alias("avg_max_temperature"),
    F.avg("temperature_range").alias("avg_temperature_range"),

    # Extreme temperatures
    F.min("min_temperature").alias("extreme_min_temperature"),
    F.max("max_temperature").alias("extreme_max_temperature"),

    # Humidity trends
    F.avg("avg_humidity").alias("avg_humidity"),

    # Pressure trends
    F.avg("avg_pressure").alias("avg_pressure"),

    # Wind trends
    F.avg("avg_wind_speed").alias("avg_wind_speed"),
    F.max("max_wind_speed").alias("max_wind_speed"),

    # Precipitation
    F.sum("total_rainfall_mm").alias("total_rainfall_mm"),
    F.sum("total_snowfall_mm").alias("total_snowfall_mm"),
    F.avg("total_rainfall_mm").alias("avg_daily_rainfall_mm"),

    # Days with rain/snow
    F.sum(F.when(F.col("total_rainfall_mm") > 0, 1).otherwise(0)).alias("rainy_days"),
    F.sum(F.when(F.col("total_snowfall_mm") > 0, 1).otherwise(0)).alias("snowy_days"),

    # Cloud coverage
    F.avg("avg_cloud_coverage").alias("avg_cloud_coverage"),

    # Visibility
    F.avg("avg_visibility").alias("avg_visibility"),

    # Comfort distribution
    F.sum(F.when(F.col("comfort_level") == "comfortable", 1).otherwise(0)).alias("comfortable_days"),
    F.sum(F.when(F.col("comfort_level") == "uncomfortable", 1).otherwise(0)).alias("uncomfortable_days"),
    F.sum(F.when(F.col("comfort_level") == "extreme", 1).otherwise(0)).alias("extreme_days"),

    # Total days
    F.count("*").alias("total_days")
).orderBy("year", "month")

# Add month name
df_monthly_trends = df_monthly_trends.withColumn(
    "month_name",
    F.date_format(F.to_date(F.concat_ws("-", F.col("year_month"), F.lit("01"))), "MMMM")
)

# Add percentage of rainy days
df_monthly_trends = df_monthly_trends.withColumn(
    "rainy_days_percent",
    F.round((F.col("rainy_days") / F.col("total_days")) * 100, 1)
)

# Add percentage of comfortable days
df_monthly_trends = df_monthly_trends.withColumn(
    "comfortable_days_percent",
    F.round((F.col("comfortable_days") / F.col("total_days")) * 100, 1)
)

# Add season classification (Northern Hemisphere)
df_monthly_trends = df_monthly_trends.withColumn(
    "season",
    F.when(F.col("month").isin(12, 1, 2), "Winter")
     .when(F.col("month").isin(3, 4, 5), "Spring")
     .when(F.col("month").isin(6, 7, 8), "Summer")
     .otherwise("Autumn")
)

print(f"✅ Calculated monthly trends for {df_monthly_trends.count():,} months")

# Show sample
print("\n📊 Sample monthly trends:")
df_monthly_trends.select(
    "year_month",
    "month_name",
    "season",
    "avg_temperature",
    "total_rainfall_mm",
    "rainy_days",
    "comfortable_days_percent"
).show(12, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Save Monthly Trends to Gold

# COMMAND ----------

print("\n" + "=" * 80)
print("💾 Saving Monthly Weather Trends to Gold")
print("=" * 80)

try:
    table_name = f"{CATALOG}.{TARGET_SCHEMA}.weather_monthly_trends"

    df_monthly_trends.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table_name)

    # Verify
    saved_count = spark.table(table_name).count()
    print(f"✅ Saved {saved_count:,} monthly records to {table_name}")

except Exception as e:
    print(f"❌ ERROR saving monthly trends: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary Statistics

# COMMAND ----------

print("\n" + "=" * 80)
print("📊 SUMMARY - Weather Trends Gold Layer")
print("=" * 80)

# Overall statistics
overall_stats = df_daily_stats.agg(
    F.min("date").alias("earliest_date"),
    F.max("date").alias("latest_date"),
    F.avg("avg_temperature").alias("overall_avg_temp"),
    F.min("min_temperature").alias("record_low_temp"),
    F.max("max_temperature").alias("record_high_temp"),
    F.sum("total_rainfall_mm").alias("total_rainfall"),
    F.avg("total_rainfall_mm").alias("avg_daily_rainfall")
).collect()[0]

print(f"\n📅 Date Range:")
print(f"   Earliest: {overall_stats['earliest_date']}")
print(f"   Latest: {overall_stats['latest_date']}")

print(f"\n🌡️  Temperature:")
print(f"   Average: {overall_stats['overall_avg_temp']:.1f}°C")
print(f"   Record Low: {overall_stats['record_low_temp']:.1f}°C")
print(f"   Record High: {overall_stats['record_high_temp']:.1f}°C")

print(f"\n🌧️  Rainfall:")
print(f"   Total: {overall_stats['total_rainfall']:.1f} mm")
print(f"   Daily Average: {overall_stats['avg_daily_rainfall']:.2f} mm")

print(f"\n💾 Output Tables:")
print(f"   {CATALOG}.{TARGET_SCHEMA}.weather_daily_stats")
print(f"   {CATALOG}.{TARGET_SCHEMA}.weather_monthly_trends")
print(f"   Storage: Unity Catalog managed tables")

print("\n" + "=" * 80)
print("✅ Weather Trends Analysis Complete!")
print("=" * 80)
