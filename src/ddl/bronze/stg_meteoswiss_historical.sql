-- ============================================================================
-- Bronze Stage: Historical Weather Data
-- ============================================================================
-- Internal stage for MeteoSwiss historical weather data CSV files
--
-- Data coverage: Measurement start → Dec 31 last year
-- File pattern: *_t_historical_*.csv (decade files: 1980-1989, 1990-1999, etc.)
-- Update frequency: Manual/yearly backfill
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

CREATE STAGE IF NOT EXISTS bronze.stg_meteoswiss_historical
    FILE_FORMAT = bronze.ff_meteoswiss_csv
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for MeteoSwiss historical weather data CSV files (backfill data from measurement start to Dec 31 last year)';

-- To list files in stage:
-- LIST @bronze.stg_meteoswiss_historical;

-- To remove all files from stage (use with caution):
-- REMOVE @bronze.stg_meteoswiss_historical;
