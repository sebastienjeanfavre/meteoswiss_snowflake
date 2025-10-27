-- ============================================================================
-- Bronze Stage: ICON Forecast Data
-- ============================================================================
-- Internal stage for ICON-CH1/CH2 forecast data CSV files
--
-- Data coverage: Numerical weather prediction forecasts
-- File types:
--   1. Grid reference: icon_ch1_grid.csv, icon_ch2_grid.csv
--      - Static grid cell coordinates (cell, lon, lat)
--   2. Forecast data: icon_ch1_forecast_*.csv, icon_ch2_forecast_*.csv
--      - Time-varying forecast values (cell, lead_time_0h, lead_time_1h, ...)
--
-- Models:
--   - ICON-CH1-EPS: 1 km resolution, 33h horizon, hourly output
--   - ICON-CH2-EPS: 2.1 km resolution, 120h horizon, hourly output
--
-- Update frequency: Variable (can be updated per forecast run)
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;
USE WAREHOUSE METEOSWISS_WH;

CREATE STAGE IF NOT EXISTS bronze.stg_icon_ch1
    FILE_FORMAT = bronze.ff_icon_forecast_csv
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for ICON-CH1/CH2 numerical weather forecast data CSV files (grid reference and forecast data)';

-- To list files in stage:
-- LIST @bronze.stg_icon_forecasts;

-- To remove all files from stage (use with caution):
-- REMOVE @bronze.stg_icon_forecasts;
