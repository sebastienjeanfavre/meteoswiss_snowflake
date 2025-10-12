-- ============================================================================
-- Bronze Stage: Recent Weather Data
-- ============================================================================
-- Internal stage for MeteoSwiss recent weather data CSV files
--
-- Data coverage: Jan 1 current year → Yesterday
-- File pattern: *_t_recent.csv
-- Update frequency: Daily at 13:00 UTC (automated via stored procedure)
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

CREATE STAGE IF NOT EXISTS bronze.stg_meteoswiss_recent
    FILE_FORMAT = bronze.ff_meteoswiss_csv
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for MeteoSwiss recent weather data CSV files (Jan 1 current year to yesterday, updated daily)';

-- To list files in stage:
-- LIST @bronze.stg_meteoswiss_recent;

-- To remove all files from stage (use with caution):
-- REMOVE @bronze.stg_meteoswiss_recent;
