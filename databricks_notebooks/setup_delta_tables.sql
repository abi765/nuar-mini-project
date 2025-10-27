-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Lake Table Setup
-- MAGIC
-- MAGIC Creates all Bronze, Silver, and Gold layer tables

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
-- Bronze Infrastructure Table
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
LOCATION 'dbfs:/FileStore/nuar/bronze/infrastructure'
COMMENT 'Raw infrastructure data from Overpass API';

-- COMMAND ----------
-- Verify table creation
DESCRIBE EXTENDED bronze.infrastructure;

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
LOCATION 'dbfs:/FileStore/nuar/silver/infrastructure'
COMMENT 'Cleaned infrastructure data with BNG coordinates and enrichment';

-- COMMAND ----------
-- Show all tables
SHOW TABLES IN bronze;
SHOW TABLES IN silver;

-- COMMAND ----------
SELECT 'Setup Complete! ✅' as status;
