-- ============================================================================
-- NOTE: ICON-CH1 Forecast Data - Manual Approach Required
-- ============================================================================
-- LIMITATION: Stored procedure approach is NOT VIABLE due to system dependencies
--
-- The meteodatalab library requires ecCodes (a C library for GRIB2 decoding)
-- which cannot be installed via Snowflake's PACKAGES parameter. ecCodes requires
-- system-level compilation and is not available in Snowflake's Python runtime.
--
-- RECOMMENDED APPROACH:
-- 1. Run Python script locally: scripts/fetch_icon_ch1_forecast.py
--    (Requires local environment with meteodatalab, xarray, earthkit-data, ecCodes)
-- 2. Upload CSVs to stage: scripts/upload_icon_ch1_to_snowflake.sh
-- 3. Load data into tables using standard COPY INTO (see table definitions)
--
-- This file is kept for reference only and should NOT be deployed.
-- ============================================================================

/*
USE ROLE SYSADMIN;
USE WAREHOUSE METEOSWISS_WH;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

-- This procedure requires PyPI repository access
-- Run: GRANT DATABASE ROLE SNOWFLAKE.PYPI_REPOSITORY_USER TO ROLE SYSADMIN;

CREATE OR REPLACE PROCEDURE bronze.sp_load_icon_ch1_forecast(
    variable VARCHAR DEFAULT 'ASWDIR_S'
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python', 'meteodata-lab', 'xarray', 'pandas', 'numpy')
EXTERNAL_ACCESS_INTEGRATIONS = (meteoswiss_integration)
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
# Python code here - commented out until PyPI access is available
$$;
*/

-- ============================================================================
-- Recommended Workflow (Without Stored Procedure)
-- ============================================================================
-- Step 1: Fetch data locally
--   python3 scripts/fetch_icon_ch1_forecast.py
--
-- Step 2: Upload to Snowflake
--   bash scripts/upload_icon_ch1_to_snowflake.sh
--   OR manually:
--   snow stage copy ./meteoswiss_data/icon_ch1_grid.csv @bronze.stg_icon_forecasts --overwrite
--   snow stage copy ./meteoswiss_data/icon_ch1_forecast_aswdir_s.csv @bronze.stg_icon_forecasts --overwrite
--
-- Step 3: Load into tables (once tables are created)
--   COPY INTO bronze.t_icon_ch1_grid FROM @bronze.stg_icon_forecasts/icon_ch1_grid.csv ...
--   COPY INTO bronze.t_icon_ch1_forecast_aswdir_s FROM @bronze.stg_icon_forecasts/icon_ch1_forecast_aswdir_s.csv ...
-- ============================================================================
