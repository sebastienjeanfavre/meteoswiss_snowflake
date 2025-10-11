-- ============================================================================
-- MeteoSwiss Historical Data Loading Setup
-- ============================================================================
-- This script sets up the infrastructure to load historical weather data
-- from CSV files into Snowflake tables.
--
-- Prerequisites:
-- - Run scripts 01-06 first
-- - Download historical data using scripts/fetch_historical_data.py
-- - Files should be in meteoswiss_data/historical/{station_id}/*.csv
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

-- ============================================================================
-- 1. CREATE FILE FORMAT
-- ============================================================================
-- CSV files use semicolon (;) as delimiter
CREATE OR REPLACE FILE FORMAT bronze.meteoswiss_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ';'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    NULL_IF = ('', 'NULL', 'null', '-')
    EMPTY_FIELD_AS_NULL = TRUE
    DATE_FORMAT = 'DD.MM.YYYY HH24:MI'
    TIMESTAMP_FORMAT = 'DD.MM.YYYY HH24:MI';

COMMENT ON FILE FORMAT bronze.meteoswiss_csv_format IS
    'File format for MeteoSwiss semicolon-delimited CSV files';

-- ============================================================================
-- 2. CREATE INTERNAL STAGE
-- ============================================================================
CREATE OR REPLACE STAGE bronze.meteoswiss_historical_stage
    FILE_FORMAT = bronze.meteoswiss_csv_format
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for MeteoSwiss historical weather data CSV files';

-- ============================================================================
-- 3. CREATE HISTORICAL MEASUREMENTS TABLE
-- ============================================================================
-- Table structure based on MeteoSwiss 10-minute data columns
CREATE OR REPLACE TABLE bronze.weather_measurements_10min_historical (
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
COMMENT ON TABLE bronze.weather_measurements_10min_historical IS
    'Historical 10-minute weather measurements from MeteoSwiss stations (backfill data)';
COMMENT ON COLUMN bronze.weather_measurements_10min_historical.station_abbr IS 'Station abbreviation code';
COMMENT ON COLUMN bronze.weather_measurements_10min_historical.reference_timestamp IS 'Measurement timestamp (10-minute intervals)';
COMMENT ON COLUMN bronze.weather_measurements_10min_historical.gre000z0 IS 'Global solar radiation in W/m²';
COMMENT ON COLUMN bronze.weather_measurements_10min_historical.file_name IS 'Source CSV filename for audit trail';

-- ============================================================================
-- 4. DOWNLOAD HISTORICAL DATA (RUN FROM LOCAL MACHINE)
-- ============================================================================

-- Before uploading, download all historical CSV files from MeteoSwiss API
-- From your terminal, cd to the root folder of the project and run:
-- python scripts/fetch_historical_data.py
--
-- This will download all _t_historical_*.csv files (all decades) for all stations
-- Files will be saved to: meteoswiss_data/historical/{station_id}/*.csv

-- ============================================================================
-- 5. UPLOAD FILES TO STAGE (RUN FROM LOCAL MACHINE)
-- ============================================================================

-- Install Snowflake CLI and configure connection
-- From Snowflake CLI, cd to the root folder of the project and run:
--
-- snow stage copy ./meteoswiss_data/historical/ @meteoswiss.bronze.meteoswiss_historical_stage --recursive
--
-- After upload completes, verify files are uploaded:
LIST @bronze.meteoswiss_historical_stage;


-- ============================================================================
-- 6. LOAD DATA FROM STAGE TO TABLE
-- ============================================================================

-- Load all CSV files from stage
COPY INTO bronze.weather_measurements_10min_historical
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
    FROM @bronze.meteoswiss_historical_stage
)
PATTERN = '.*t_historical_.*\\.csv'
ON_ERROR = ABORT_STATEMENT
FORCE = FALSE;

-- Check load results
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'WEATHER_MEASUREMENTS_10MIN_HISTORICAL',
    START_TIME => DATEADD(HOURS, -1, CURRENT_TIMESTAMP())
));


-- ============================================================================
-- 7. DATA VALIDATION QUERIES
-- ============================================================================

-- Count total records
SELECT COUNT(*) as total_records
FROM bronze.weather_measurements_10min_historical;

-- Count records by station
SELECT
    station_abbr,
    COUNT(*) as record_count,
    MIN(reference_timestamp) as earliest_measurement,
    MAX(reference_timestamp) as latest_measurement,
    DATEDIFF(day, MIN(reference_timestamp), MAX(reference_timestamp)) as days_of_data
FROM bronze.weather_measurements_10min_historical
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
FROM bronze.weather_measurements_10min_historical;

-- Sample data for verification
SELECT *
FROM bronze.weather_measurements_10min_historical
LIMIT 100;

-- Check for duplicate timestamps per station
SELECT
    station_abbr,
    reference_timestamp,
    COUNT(*) as duplicate_count
FROM bronze.weather_measurements_10min_historical
GROUP BY station_abbr, reference_timestamp
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
