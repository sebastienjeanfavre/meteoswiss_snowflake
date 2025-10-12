-- ============================================================================
-- Bronze Stage: Realtime Now Weather Data
-- ============================================================================
-- Internal stage for MeteoSwiss realtime weather data CSV files
--
-- Data coverage: Yesterday 12:00 UTC → Now (current time)
-- File pattern: *_t_now.csv
-- Update frequency: Every 10 minutes (automated via stored procedure)
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

CREATE STAGE IF NOT EXISTS bronze.stg_meteoswiss_now
    FILE_FORMAT = bronze.ff_meteoswiss_csv
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for MeteoSwiss realtime weather data CSV files (yesterday 12:00 UTC to now, updated every 10 minutes)';

-- To list files in stage:
-- LIST @bronze.stg_meteoswiss_now;

-- To remove all files from stage (use with caution):
-- REMOVE @bronze.stg_meteoswiss_now;
