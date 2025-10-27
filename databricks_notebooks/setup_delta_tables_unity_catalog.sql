-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Lake Table Setup - Unity Catalog Compatible
-- MAGIC
-- MAGIC Creates all Bronze, Silver, and Gold layer tables for Unity Catalog
-- MAGIC
-- MAGIC **Note:** Unity Catalog manages table locations automatically

-- COMMAND ----------
-- Create Catalog
CREATE CATALOG IF NOT EXISTS nuar_catalog;
USE CATALOG nuar_catalog;

-- COMMAND ----------
-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw data from APIs - Parquet format';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Cleaned and transformed data - Delta format';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Aggregated analytics - Delta format';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------
-- Bronze Infrastructure Table (Unity Catalog - Managed Table)
CREATE TABLE IF NOT EXISTS bronze.infrastructure (
  id BIGINT COMMENT 'OSM element ID',
  type STRING COMMENT 'Element type (node/way)',
  lat DOUBLE COMMENT 'Latitude (WGS84)',
  lon DOUBLE COMMENT 'Longitude (WGS84)',
  infrastructure_type STRING COMMENT 'Type of infrastructure',
  substance STRING COMMENT 'Substance carried (e.g., water, gas)',
  location STRING COMMENT 'Location context',
  operator STRING COMMENT 'Infrastructure operator',
  name STRING COMMENT 'Element name',
  diameter STRING COMMENT 'Pipe/cable diameter',
  material STRING COMMENT 'Construction material',
  all_tags STRING COMMENT 'All OSM tags as JSON',
  node_count INT COMMENT 'Number of nodes in way',
  geometry_points INT COMMENT 'Number of geometry points',
  first_lat DOUBLE COMMENT 'First point latitude',
  first_lon DOUBLE COMMENT 'First point longitude',
  last_lat DOUBLE COMMENT 'Last point latitude',
  last_lon DOUBLE COMMENT 'Last point longitude',
  geometry_json STRING COMMENT 'Full geometry as JSON',
  ingestion_timestamp TIMESTAMP COMMENT 'Data ingestion timestamp',
  ingestion_date DATE COMMENT 'Data ingestion date',
  area STRING COMMENT 'Geographic area (e.g., stockport)'
)
USING DELTA
PARTITIONED BY (ingestion_date, infrastructure_type)
COMMENT 'Raw infrastructure data from Overpass API';

-- COMMAND ----------
-- Bronze Crime Table
CREATE TABLE IF NOT EXISTS bronze.crime (
  crime_id STRING COMMENT 'Crime ID',
  persistent_id STRING COMMENT 'Persistent crime ID',
  category STRING COMMENT 'Crime category',
  month DATE COMMENT 'Month of crime',
  lat DOUBLE COMMENT 'Latitude',
  lon DOUBLE COMMENT 'Longitude',
  street_id STRING COMMENT 'Street ID',
  street_name STRING COMMENT 'Street name',
  location_type STRING COMMENT 'Location type',
  location_subtype STRING COMMENT 'Location subtype',
  outcome_category STRING COMMENT 'Outcome category',
  outcome_date STRING COMMENT 'Outcome date',
  context STRING COMMENT 'Crime context',
  ingestion_timestamp TIMESTAMP COMMENT 'Data ingestion timestamp',
  ingestion_date DATE COMMENT 'Data ingestion date',
  area STRING COMMENT 'Geographic area'
)
USING DELTA
PARTITIONED BY (ingestion_date, month)
COMMENT 'Raw crime data from UK Police API';

-- COMMAND ----------
-- Bronze Weather Table
CREATE TABLE IF NOT EXISTS bronze.weather (
  location_name STRING COMMENT 'Location name',
  country STRING COMMENT 'Country code',
  lat DOUBLE COMMENT 'Latitude',
  lon DOUBLE COMMENT 'Longitude',
  datetime TIMESTAMP COMMENT 'Weather datetime',
  timezone_offset INT COMMENT 'Timezone offset',
  weather_id INT COMMENT 'Weather ID',
  weather_main STRING COMMENT 'Main weather condition',
  weather_description STRING COMMENT 'Weather description',
  weather_icon STRING COMMENT 'Weather icon code',
  temp_celsius DOUBLE COMMENT 'Temperature in Celsius',
  feels_like_celsius DOUBLE COMMENT 'Feels like temperature',
  temp_min_celsius DOUBLE COMMENT 'Min temperature',
  temp_max_celsius DOUBLE COMMENT 'Max temperature',
  pressure_hpa DOUBLE COMMENT 'Pressure in hPa',
  humidity_percent DOUBLE COMMENT 'Humidity percentage',
  visibility_meters DOUBLE COMMENT 'Visibility in meters',
  wind_speed_ms DOUBLE COMMENT 'Wind speed m/s',
  wind_direction_deg DOUBLE COMMENT 'Wind direction degrees',
  wind_gust_ms DOUBLE COMMENT 'Wind gust m/s',
  clouds_percent DOUBLE COMMENT 'Cloud coverage percentage',
  sunrise TIMESTAMP COMMENT 'Sunrise time',
  sunset TIMESTAMP COMMENT 'Sunset time',
  ingestion_timestamp TIMESTAMP COMMENT 'Data ingestion timestamp',
  ingestion_date DATE COMMENT 'Data ingestion date',
  area STRING COMMENT 'Geographic area'
)
USING DELTA
PARTITIONED BY (ingestion_date)
COMMENT 'Raw weather snapshots from OpenWeatherMap';

-- COMMAND ----------
-- Bronze Postcodes Table
CREATE TABLE IF NOT EXISTS bronze.postcodes (
  postcode STRING COMMENT 'Full postcode',
  postcode_type STRING COMMENT 'Type: outward_code or full_postcode',
  quality INT COMMENT 'Positional quality indicator',
  eastings BIGINT COMMENT 'OS Eastings',
  northings BIGINT COMMENT 'OS Northings',
  country STRING COMMENT 'Country',
  nhs_ha STRING COMMENT 'NHS Health Authority',
  longitude DOUBLE COMMENT 'Longitude',
  latitude DOUBLE COMMENT 'Latitude',
  european_electoral_region STRING COMMENT 'European Electoral Region',
  primary_care_trust STRING COMMENT 'Primary Care Trust',
  region STRING COMMENT 'Region',
  lsoa STRING COMMENT 'Lower Super Output Area',
  msoa STRING COMMENT 'Middle Super Output Area',
  incode STRING COMMENT 'Incode',
  outcode STRING COMMENT 'Outcode',
  parliamentary_constituency STRING COMMENT 'Parliamentary Constituency',
  admin_district STRING COMMENT 'Administrative District',
  parish STRING COMMENT 'Parish',
  admin_county STRING COMMENT 'Administrative County',
  admin_ward STRING COMMENT 'Administrative Ward',
  ced STRING COMMENT 'County Electoral Division',
  ccg STRING COMMENT 'Clinical Commissioning Group',
  nuts STRING COMMENT 'Nomenclature of Territorial Units',
  codes_admin_district STRING COMMENT 'Admin district code',
  codes_admin_county STRING COMMENT 'Admin county code',
  codes_admin_ward STRING COMMENT 'Admin ward code',
  codes_parish STRING COMMENT 'Parish code',
  codes_ccg STRING COMMENT 'CCG code',
  codes_nuts STRING COMMENT 'NUTS code',
  ingestion_timestamp TIMESTAMP COMMENT 'Data ingestion timestamp',
  ingestion_date DATE COMMENT 'Data ingestion date',
  area STRING COMMENT 'Geographic area'
)
USING DELTA
PARTITIONED BY (ingestion_date, postcode_type)
COMMENT 'Raw postcode reference data from Postcodes.io';

-- COMMAND ----------
-- Verify Bronze tables
DESCRIBE EXTENDED bronze.infrastructure;

-- COMMAND ----------
SHOW TABLES IN bronze;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Silver Layer Tables

-- COMMAND ----------
-- Silver Infrastructure Table
CREATE TABLE IF NOT EXISTS silver.infrastructure (
  id BIGINT,
  type STRING,
  lat DOUBLE,
  lon DOUBLE,
  easting_bng BIGINT COMMENT 'British National Grid Easting',
  northing_bng BIGINT COMMENT 'British National Grid Northing',
  infrastructure_type STRING,
  substance STRING,
  location STRING,
  operator STRING,
  name STRING,
  diameter STRING,
  material STRING,
  length_meters DOUBLE COMMENT 'Length of linear features in meters',
  postcode STRING COMMENT 'Nearest postcode',
  admin_district STRING COMMENT 'Administrative district',
  admin_ward STRING COMMENT 'Administrative ward',
  has_coordinates BOOLEAN COMMENT 'Has valid WGS84 coordinates',
  has_bng_coords BOOLEAN COMMENT 'Has valid BNG coordinates',
  has_postcode BOOLEAN COMMENT 'Has postcode enrichment',
  has_admin_data BOOLEAN COMMENT 'Has admin area data',
  is_valid_location BOOLEAN COMMENT 'Within Stockport bounding box',
  transformation_timestamp TIMESTAMP,
  transformation_date DATE,
  data_layer STRING,
  source_system STRING,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date, infrastructure_type)
COMMENT 'Cleaned infrastructure data with BNG coordinates and enrichment';

-- COMMAND ----------
-- Silver Crime Table
CREATE TABLE IF NOT EXISTS silver.crime (
  crime_id STRING,
  persistent_id STRING,
  category STRING,
  month DATE,
  lat DOUBLE,
  lon DOUBLE,
  easting_bng BIGINT COMMENT 'British National Grid Easting',
  northing_bng BIGINT COMMENT 'British National Grid Northing',
  street_id STRING,
  street_name STRING,
  location_type STRING,
  location_subtype STRING,
  outcome_category STRING,
  outcome_date STRING,
  context STRING,
  postcode STRING COMMENT 'Nearest postcode',
  admin_district STRING COMMENT 'Administrative district',
  admin_ward STRING COMMENT 'Administrative ward',
  has_coordinates BOOLEAN,
  has_bng_coords BOOLEAN,
  has_postcode BOOLEAN,
  is_valid_location BOOLEAN,
  transformation_timestamp TIMESTAMP,
  transformation_date DATE,
  data_layer STRING,
  source_system STRING,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date, month)
COMMENT 'Cleaned crime data with spatial enrichment';

-- COMMAND ----------
-- Silver Weather Table
CREATE TABLE IF NOT EXISTS silver.weather (
  location_name STRING,
  country STRING,
  lat DOUBLE,
  lon DOUBLE,
  datetime TIMESTAMP,
  weather_main STRING,
  weather_description STRING,
  temp_celsius DOUBLE,
  feels_like_celsius DOUBLE,
  pressure_hpa DOUBLE,
  humidity_percent DOUBLE,
  visibility_meters DOUBLE,
  wind_speed_ms DOUBLE,
  wind_direction_deg DOUBLE,
  clouds_percent DOUBLE,
  sunrise TIMESTAMP,
  sunset TIMESTAMP,
  has_complete_data BOOLEAN,
  transformation_timestamp TIMESTAMP,
  transformation_date DATE,
  data_layer STRING,
  source_system STRING,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date)
COMMENT 'Cleaned weather data';

-- COMMAND ----------
-- Silver Postcodes Table
CREATE TABLE IF NOT EXISTS silver.postcodes (
  postcode STRING,
  eastings BIGINT,
  northings BIGINT,
  longitude DOUBLE,
  latitude DOUBLE,
  admin_district STRING,
  admin_ward STRING,
  admin_county STRING,
  region STRING,
  country STRING,
  parliamentary_constituency STRING,
  lsoa STRING,
  msoa STRING,
  has_coordinates BOOLEAN,
  transformation_timestamp TIMESTAMP,
  transformation_date DATE,
  data_layer STRING,
  source_system STRING,
  ingestion_date DATE
)
USING DELTA
PARTITIONED BY (ingestion_date)
COMMENT 'Cleaned postcode reference data';

-- COMMAND ----------
-- Show all Silver tables
SHOW TABLES IN silver;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Gold Layer Tables (Placeholder)

-- COMMAND ----------
-- Gold Infrastructure Density (example for future use)
CREATE TABLE IF NOT EXISTS gold.infrastructure_density (
  grid_id STRING COMMENT 'Grid cell identifier',
  easting_min BIGINT,
  easting_max BIGINT,
  northing_min BIGINT,
  northing_max BIGINT,
  infrastructure_type STRING,
  density DOUBLE COMMENT 'Infrastructure density per km²',
  element_count INT COMMENT 'Number of elements in grid',
  avg_depth DOUBLE COMMENT 'Average depth if available',
  calculation_date DATE
)
USING DELTA
PARTITIONED BY (calculation_date, infrastructure_type)
COMMENT 'Infrastructure density analysis by grid cell';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Verification

-- COMMAND ----------
-- Show all catalogs
SHOW CATALOGS;

-- COMMAND ----------
-- Show all schemas
SHOW SCHEMAS IN nuar_catalog;

-- COMMAND ----------
-- Show all tables
SELECT
  'bronze' as layer,
  table_name,
  table_type
FROM information_schema.tables
WHERE table_schema = 'bronze'

UNION ALL

SELECT
  'silver' as layer,
  table_name,
  table_type
FROM information_schema.tables
WHERE table_schema = 'silver'

UNION ALL

SELECT
  'gold' as layer,
  table_name,
  table_type
FROM information_schema.tables
WHERE table_schema = 'gold'

ORDER BY layer, table_name;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Summary

-- COMMAND ----------
SELECT
  '✅ Unity Catalog Setup Complete!' as status,
  'All tables created as managed tables' as note,
  'Unity Catalog will manage storage locations automatically' as info;
