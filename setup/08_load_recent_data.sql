-- ============================================================================
-- MeteoSwiss Recent Data Loading Setup
-- ============================================================================
-- This script sets up the infrastructure to load recent weather data
-- from CSV files into Snowflake tables.
--
-- Recent data timeline:
-- - Covers: January 1st of current year until yesterday
-- - Updated: Daily at 12:00 UTC by MeteoSwiss
-- - Granularity: 10-minute intervals
--
-- Prerequisites:
-- - Run scripts 01-07 first (REQUIRED - script 07 creates the CSV file format)
-- - Download recent data using scripts/fetch_recent_data.py (optional for initial load)
-- - Files should be in meteoswiss_data/recent/{station_id}/*.csv
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

-- ============================================================================
-- 1. CREATE INTERNAL STAGE FOR RECENT DATA
-- ============================================================================
-- Reuses the same file format (meteoswiss_csv_format) created in script 07
CREATE OR REPLACE STAGE bronze.meteoswiss_recent_stage
    FILE_FORMAT = bronze.meteoswiss_csv_format
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for MeteoSwiss recent weather data CSV files (current year to yesterday)';

-- ============================================================================
-- 2. CREATE RECENT MEASUREMENTS TABLE
-- ============================================================================
-- Same schema as historical table, but for recent data (current year)
CREATE OR REPLACE TABLE bronze.weather_measurements_10min_recent (
    -- Metadata
    station_abbr VARCHAR(10),
    reference_timestamp TIMESTAMP_NTZ,

    -- Temperature measurements (°C)
    tre200s0 NUMBER(38,10),  -- Air temperature 2m above ground
    tre005s0 NUMBER(38,10),  -- Air temperature 5cm above ground
    tresurs0 NUMBER(38,10),  -- Surface temperature
    xchills0 NUMBER(38,10),  -- Wind chill temperature

    -- Humidity and dew point
    ure200s0 NUMBER(38,10),  -- Relative humidity 2m above ground (%)
    tde200s0 NUMBER(38,10),  -- Dew point 2m above ground (°C)
    pva200s0 NUMBER(38,10),  -- Water vapour pressure (hPa)

    -- Pressure measurements (hPa)
    prestas0 NUMBER(38,10),  -- Pressure at station level
    pp0qnhs0 NUMBER(38,10),  -- Pressure reduced to sea level QNH
    pp0qffs0 NUMBER(38,10),  -- Pressure reduced to sea level QFF
    ppz850s0 NUMBER(38,10),  -- Geopotential height 850 hPa
    ppz700s0 NUMBER(38,10),  -- Geopotential height 700 hPa

    -- Wind measurements
    fkl010z1 NUMBER(38,10),  -- Wind speed scalar 10m above ground (m/s)
    fve010z0 NUMBER(38,10),  -- Wind speed vector 10m above ground (m/s)
    fkl010z0 NUMBER(38,10),  -- Wind gust peak 10m above ground (m/s)
    dkl010z0 NUMBER(38,10),  -- Wind direction 10m above ground (degrees)
    wcc006s0 NUMBER(38,10),  -- Cloud cover

    -- Additional wind measurements
    fu3010z0 NUMBER(38,10),  -- Gust wind speed maximum (m/s)
    fkl010z3 NUMBER(38,10),  -- Wind speed scalar 10m above ground max (m/s)
    fu3010z1 NUMBER(38,10),  -- Gust wind speed 1s (m/s)
    fu3010z3 NUMBER(38,10),  -- Gust wind speed 3s (m/s)

    -- Precipitation
    rre150z0 NUMBER(38,10),  -- Precipitation (mm)

    -- Sunshine and radiation
    htoauts0 NUMBER(38,10),  -- Sunshine duration (min)
    gre000z0 NUMBER(38,10),  -- Global radiation (W/m²)

    -- Additional measurements
    ods000z0 NUMBER(38,10),  -- Snow depth (cm)
    oli000z0 NUMBER(38,10),  -- Lysimeter infiltration
    olo000z0 NUMBER(38,10),  -- Lysimeter outflow
    osr000z0 NUMBER(38,10),  -- Lysimeter storage
    sre000z0 NUMBER(38,10),  -- Reflected short-wave radiation (W/m²)

    -- Audit columns
    file_name VARCHAR(500),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add table and column comments
COMMENT ON TABLE bronze.weather_measurements_10min_recent IS
    'Recent 10-minute weather measurements from MeteoSwiss stations (current year to yesterday, updated daily at 12 UTC)';
COMMENT ON COLUMN bronze.weather_measurements_10min_recent.station_abbr IS 'Station abbreviation code';
COMMENT ON COLUMN bronze.weather_measurements_10min_recent.reference_timestamp IS 'Measurement timestamp (10-minute intervals)';
COMMENT ON COLUMN bronze.weather_measurements_10min_recent.gre000z0 IS 'Global solar radiation in W/m²';
COMMENT ON COLUMN bronze.weather_measurements_10min_recent.file_name IS 'Source CSV filename for audit trail';

-- ============================================================================
-- 3. DOWNLOAD RECENT DATA (OPTIONAL - FOR INITIAL SETUP ONLY)
-- ============================================================================

-- NOTE: After initial setup, recent data is automatically fetched and loaded
-- by the Snowpark stored procedure sp_fetch_and_load_recent_data.sql
-- and scheduled via task_refresh_recent_data.sql (daily at 13:00 UTC)
--
-- For INITIAL SETUP ONLY, manually download recent data:
-- From your terminal, cd to the root folder of the project and run:
-- python scripts/fetch_recent_data.py
--
-- This will download _t_recent.csv files for all stations
-- Files will be saved to: meteoswiss_data/recent/{station_id}/*.csv

-- ============================================================================
-- 4. UPLOAD FILES TO STAGE (OPTIONAL - FOR INITIAL SETUP ONLY)
-- ============================================================================

-- For INITIAL SETUP ONLY, manually upload files:
-- Install Snowflake CLI and configure connection
-- From Snowflake CLI, cd to the root folder of the project and run:
--
-- snow stage copy ./meteoswiss_data/recent/ @meteoswiss.bronze.meteoswiss_recent_stage --recursive
--
-- After upload completes, verify files are uploaded:
LIST @bronze.meteoswiss_recent_stage;


-- ============================================================================
-- 5. LOAD DATA FROM STAGE TO TABLE (OPTIONAL - FOR INITIAL SETUP ONLY)
-- ============================================================================

-- Truncate table first to remove old data (recent data is a complete snapshot)
TRUNCATE TABLE bronze.weather_measurements_10min_recent;

-- Load all CSV files from stage
COPY INTO bronze.weather_measurements_10min_recent
FROM (
    SELECT
        $1::VARCHAR as station_abbr,
        TO_TIMESTAMP_NTZ($2, 'DD.MM.YYYY HH24:MI') as reference_timestamp,
        TRY_CAST($3 AS NUMBER(38,10)) as tre200s0,
        TRY_CAST($4 AS NUMBER(38,10)) as tre005s0,
        TRY_CAST($5 AS NUMBER(38,10)) as tresurs0,
        TRY_CAST($6 AS NUMBER(38,10)) as xchills0,
        TRY_CAST($7 AS NUMBER(38,10)) as ure200s0,
        TRY_CAST($8 AS NUMBER(38,10)) as tde200s0,
        TRY_CAST($9 AS NUMBER(38,10)) as pva200s0,
        TRY_CAST($10 AS NUMBER(38,10)) as prestas0,
        TRY_CAST($11 AS NUMBER(38,10)) as pp0qnhs0,
        TRY_CAST($12 AS NUMBER(38,10)) as pp0qffs0,
        TRY_CAST($13 AS NUMBER(38,10)) as ppz850s0,
        TRY_CAST($14 AS NUMBER(38,10)) as ppz700s0,
        TRY_CAST($15 AS NUMBER(38,10)) as fkl010z1,
        TRY_CAST($16 AS NUMBER(38,10)) as fve010z0,
        TRY_CAST($17 AS NUMBER(38,10)) as fkl010z0,
        TRY_CAST($18 AS NUMBER(38,10)) as dkl010z0,
        TRY_CAST($19 AS NUMBER(38,10)) as wcc006s0,
        TRY_CAST($20 AS NUMBER(38,10)) as fu3010z0,
        TRY_CAST($21 AS NUMBER(38,10)) as fkl010z3,
        TRY_CAST($22 AS NUMBER(38,10)) as fu3010z1,
        TRY_CAST($23 AS NUMBER(38,10)) as fu3010z3,
        TRY_CAST($24 AS NUMBER(38,10)) as rre150z0,
        TRY_CAST($25 AS NUMBER(38,10)) as htoauts0,
        TRY_CAST($26 AS NUMBER(38,10)) as gre000z0,
        TRY_CAST($27 AS NUMBER(38,10)) as ods000z0,
        TRY_CAST($28 AS NUMBER(38,10)) as oli000z0,
        TRY_CAST($29 AS NUMBER(38,10)) as olo000z0,
        TRY_CAST($30 AS NUMBER(38,10)) as osr000z0,
        TRY_CAST($31 AS NUMBER(38,10)) as sre000z0,
        METADATA$FILENAME as file_name,
        CURRENT_TIMESTAMP() as loaded_at
    FROM @bronze.meteoswiss_recent_stage
)
PATTERN = '.*t_recent\\.csv'
ON_ERROR = ABORT_STATEMENT
FORCE = TRUE;

-- Check load results
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'WEATHER_MEASUREMENTS_10MIN_RECENT',
    START_TIME => DATEADD(HOURS, -1, CURRENT_TIMESTAMP())
));


-- ============================================================================
-- 6. DATA VALIDATION QUERIES
-- ============================================================================

-- Count total records
SELECT COUNT(*) as total_records
FROM bronze.weather_measurements_10min_recent;

-- Count records by station
SELECT
    station_abbr,
    COUNT(*) as record_count,
    MIN(reference_timestamp) as earliest_measurement,
    MAX(reference_timestamp) as latest_measurement,
    DATEDIFF(day, MIN(reference_timestamp), MAX(reference_timestamp)) as days_of_data
FROM bronze.weather_measurements_10min_recent
GROUP BY station_abbr
ORDER BY station_abbr;

-- Check for data quality - null percentages
SELECT
    COUNT(*) as total_rows,
    COUNT(gre000z0) as solar_radiation_count,
    ROUND((COUNT(gre000z0) / COUNT(*)) * 100, 2) as solar_radiation_fill_pct,
    COUNT(tre200s0) as temperature_count,
    ROUND((COUNT(tre200s0) / COUNT(*)) * 100, 2) as temperature_fill_pct,
    COUNT(rre150z0) as precipitation_count,
    ROUND((COUNT(rre150z0) / COUNT(*)) * 100, 2) as precipitation_fill_pct
FROM bronze.weather_measurements_10min_recent;

-- Verify date range is current year
SELECT
    MIN(reference_timestamp) as earliest_date,
    MAX(reference_timestamp) as latest_date,
    YEAR(MIN(reference_timestamp)) as earliest_year,
    YEAR(MAX(reference_timestamp)) as latest_year
FROM bronze.weather_measurements_10min_recent;

-- Sample data for verification
SELECT *
FROM bronze.weather_measurements_10min_recent
ORDER BY reference_timestamp DESC
LIMIT 100;

-- Check for duplicate timestamps per station
SELECT
    station_abbr,
    reference_timestamp,
    COUNT(*) as duplicate_count
FROM bronze.weather_measurements_10min_recent
GROUP BY station_abbr, reference_timestamp
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;


-- ============================================================================
-- 7. DEPLOY AUTOMATION (RECOMMENDED)
-- ============================================================================

-- After initial setup, deploy automation to eliminate manual data fetching.
-- The automation uses Snowpark Python stored procedures to:
-- - Fetch data from MeteoSwiss API
-- - Upload to internal stage
-- - Load into table
-- - Run daily at 13:00 UTC (1 hour after MeteoSwiss updates)

-- Step 1: Deploy the stored procedure
-- Run the SQL file: src/ddl/bronze/sp_fetch_and_load_recent_data.sql

-- Step 2: Deploy the scheduled task
-- Run the SQL file: src/ddl/bronze/task_refresh_recent_data.sql

-- Step 3: Activate the task (tasks are created in SUSPENDED state)
-- ALTER TASK bronze.task_refresh_recent_data RESUME;

-- Step 4: Verify task is running
-- SHOW TASKS LIKE 'task_refresh_recent_data' IN SCHEMA bronze;

-- To manually test the automation:
-- CALL bronze.sp_fetch_and_load_recent_data();

