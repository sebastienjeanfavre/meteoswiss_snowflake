-- ============================================================================
-- MeteoSwiss Station Metadata Loading Setup
-- ============================================================================
-- This script sets up the infrastructure to load weather station metadata
-- from CSV file into Snowflake table.
--
-- Station metadata includes:
-- - Station identifiers (abbreviation, name, WIGOS ID)
-- - Location (canton, coordinates in LV95 and WGS84, elevation)
-- - Station type and operational details
-- - Data availability information
--
-- Prerequisites:
-- - Run scripts 01-09 first (REQUIRED - script 07 creates the CSV file format)
-- - Station metadata is fetched automatically via stored procedure
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

-- ============================================================================
-- 1. CREATE INTERNAL STAGE FOR STATION METADATA
-- ============================================================================
-- Reuses the same file format (meteoswiss_csv_format) created in script 07
CREATE OR REPLACE STAGE bronze.meteoswiss_stations_stage
    FILE_FORMAT = bronze.meteoswiss_csv_format
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for MeteoSwiss station metadata CSV file';

-- ============================================================================
-- 2. CREATE STATION METADATA TABLE
-- ============================================================================
CREATE OR REPLACE TABLE bronze.weather_stations (
    -- Station identifiers
    station_abbr VARCHAR(10),
    station_name VARCHAR(200),
    station_canton VARCHAR(10),
    station_wigos_id VARCHAR(50),

    -- Station type (multilingual)
    station_type_de VARCHAR(100),
    station_type_fr VARCHAR(100),
    station_type_it VARCHAR(100),
    station_type_en VARCHAR(100),

    -- Operational details
    station_dataowner VARCHAR(100),
    station_data_since DATE,

    -- Elevation (meters above sea level)
    station_height_masl NUMBER(10,2),
    station_height_barometer_masl NUMBER(10,2),

    -- Coordinates - Swiss LV95 system
    station_coordinates_lv95_east NUMBER(12,2),
    station_coordinates_lv95_north NUMBER(12,2),

    -- Coordinates - WGS84 system
    station_coordinates_wgs84_lat NUMBER(10,6),
    station_coordinates_wgs84_lon NUMBER(10,6),

    -- Station exposition/terrain type (multilingual)
    station_exposition_de VARCHAR(200),
    station_exposition_fr VARCHAR(200),
    station_exposition_it VARCHAR(200),
    station_exposition_en VARCHAR(200),

    -- Station information URLs (multilingual)
    station_url_de VARCHAR(500),
    station_url_fr VARCHAR(500),
    station_url_it VARCHAR(500),
    station_url_en VARCHAR(500),

    -- Audit columns
    file_name VARCHAR(500),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add table and column comments
COMMENT ON TABLE bronze.weather_stations IS
    'MeteoSwiss automatic weather station metadata including location, coordinates, elevation, and operational details';

COMMENT ON COLUMN bronze.weather_stations.station_abbr IS 'Station abbreviation code (primary identifier)';
COMMENT ON COLUMN bronze.weather_stations.station_name IS 'Full station name';
COMMENT ON COLUMN bronze.weather_stations.station_wigos_id IS 'WMO Integrated Global Observing System identifier';
COMMENT ON COLUMN bronze.weather_stations.station_height_masl IS 'Station elevation in meters above sea level';
COMMENT ON COLUMN bronze.weather_stations.station_coordinates_wgs84_lat IS 'Latitude in WGS84 coordinate system';
COMMENT ON COLUMN bronze.weather_stations.station_coordinates_wgs84_lon IS 'Longitude in WGS84 coordinate system';
COMMENT ON COLUMN bronze.weather_stations.file_name IS 'Source CSV filename for audit trail';

-- ============================================================================
-- 3. LOAD DATA FROM STAGE TO TABLE (OPTIONAL - FOR INITIAL SETUP ONLY)
-- ============================================================================

-- After initial setup, station metadata is automatically fetched and loaded
-- by the Snowpark stored procedure sp_fetch_and_load_stations.sql
-- and scheduled via task_bronze_stations.sql (weekly on Sundays)

-- For INITIAL SETUP ONLY, manually load if CSV is already staged:
/*
TRUNCATE TABLE bronze.weather_stations;

COPY INTO bronze.weather_stations
FROM (
    SELECT
        $1::VARCHAR as station_abbr,
        $2::VARCHAR as station_name,
        $3::VARCHAR as station_canton,
        $4::VARCHAR as station_wigos_id,
        $5::VARCHAR as station_type_de,
        $6::VARCHAR as station_type_fr,
        $7::VARCHAR as station_type_it,
        $8::VARCHAR as station_type_en,
        $9::VARCHAR as station_dataowner,
        TRY_TO_DATE($10, 'YYYY-MM-DD') as station_data_since,
        TRY_CAST($11 AS NUMBER(10,2)) as station_height_masl,
        TRY_CAST($12 AS NUMBER(10,2)) as station_height_barometer_masl,
        TRY_CAST($13 AS NUMBER(12,2)) as station_coordinates_lv95_east,
        TRY_CAST($14 AS NUMBER(12,2)) as station_coordinates_lv95_north,
        TRY_CAST($15 AS NUMBER(10,6)) as station_coordinates_wgs84_lat,
        TRY_CAST($16 AS NUMBER(10,6)) as station_coordinates_wgs84_lon,
        $17::VARCHAR as station_exposition_de,
        $18::VARCHAR as station_exposition_fr,
        $19::VARCHAR as station_exposition_it,
        $20::VARCHAR as station_exposition_en,
        $21::VARCHAR as station_url_de,
        $22::VARCHAR as station_url_fr,
        $23::VARCHAR as station_url_it,
        $24::VARCHAR as station_url_en,
        METADATA$FILENAME as file_name,
        CURRENT_TIMESTAMP() as loaded_at
    FROM @bronze.meteoswiss_stations_stage
)
PATTERN = '.*meta_stations\\.csv'
ON_ERROR = CONTINUE
FORCE = TRUE;
*/

-- ============================================================================
-- 4. DATA VALIDATION QUERIES
-- ============================================================================

-- Count total stations
SELECT COUNT(*) as total_stations
FROM bronze.weather_stations;

-- List all stations with key attributes
SELECT
    station_abbr,
    station_name,
    station_canton,
    station_height_masl,
    station_coordinates_wgs84_lat,
    station_coordinates_wgs84_lon,
    station_data_since
FROM bronze.weather_stations
ORDER BY station_abbr;

-- Check for missing coordinates or elevation
SELECT
    COUNT(*) as total_stations,
    COUNT(station_coordinates_wgs84_lat) as stations_with_lat,
    COUNT(station_coordinates_wgs84_lon) as stations_with_lon,
    COUNT(station_height_masl) as stations_with_elevation
FROM bronze.weather_stations;

-- Group stations by canton
SELECT
    station_canton,
    COUNT(*) as station_count
FROM bronze.weather_stations
GROUP BY station_canton
ORDER BY station_count DESC;

-- Sample station data
SELECT *
FROM bronze.weather_stations
LIMIT 10;

-- ============================================================================
-- 5. DEPLOY AUTOMATION (RECOMMENDED)
-- ============================================================================

-- After initial setup, deploy automation to eliminate manual data fetching.
-- The automation uses Snowpark Python stored procedure to:
-- - Fetch station metadata CSV from MeteoSwiss STAC API
-- - Upload to internal stage
-- - Load into table
-- - Run weekly (stations rarely change)

-- Step 1: Deploy the stored procedure
-- Run the SQL file: src/ddl/bronze/sp_fetch_and_load_stations.sql

-- Step 2: Deploy the scheduled task
-- Run the SQL file: src/ddl/common/task_bronze_stations.sql

-- Step 3: Activate the task (tasks are created in SUSPENDED state)
-- ALTER TASK common.task_bronze_stations RESUME;

-- Step 4: Verify task is running
-- SHOW TASKS LIKE 'task_bronze_stations' IN SCHEMA common;

-- To manually test the automation:
-- CALL bronze.sp_fetch_and_load_stations();
