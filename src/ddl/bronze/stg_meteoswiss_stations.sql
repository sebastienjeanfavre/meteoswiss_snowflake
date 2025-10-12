-- ============================================================================
-- Bronze Stage: Station Metadata
-- ============================================================================
-- Internal stage for MeteoSwiss station metadata CSV file
--
-- Data coverage: All active weather stations (~180 stations)
-- File pattern: ogd-smn_meta_stations.csv
-- Update frequency: Weekly on Sunday (automated via stored procedure)
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

CREATE STAGE IF NOT EXISTS bronze.stg_meteoswiss_stations
    FILE_FORMAT = bronze.ff_meteoswiss_csv
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for MeteoSwiss station metadata CSV file (updated weekly)';

-- To list files in stage:
-- LIST @bronze.stg_meteoswiss_stations;

-- To remove all files from stage (use with caution):
-- REMOVE @bronze.stg_meteoswiss_stations;
