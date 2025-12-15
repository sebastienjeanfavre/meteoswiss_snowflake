-- ============================================================================
-- File Format: ICON Forecast CSV Format
-- ============================================================================
-- Defines the CSV file format for ICON-CH1/CH2 forecast data files.
-- Used for both grid reference files and forecast data files.
--
-- Format specifications:
-- - Type: CSV (Comma-Separated Values)
-- - Delimiter: Comma (,)
-- - Encoding: UTF-8
-- - Header: First row contains column names (skip it during load)
-- - Compression: None (files are not compressed)
-- - Null handling: Empty strings and 'NULL' treated as NULL
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;
USE WAREHOUSE METEOSWISS_WH;

CREATE OR REPLACE FILE FORMAT bronze.ff_icon_forecast_csv
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ESCAPE = 'NONE'
    ESCAPE_UNENCLOSED_FIELD = 'NONE'
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    NULL_IF = ('', 'NULL')
    ENCODING = 'UTF8'
    COMMENT = 'CSV file format for ICON forecast data files (comma-delimited, UTF-8 encoded, with header row)';

-- Verify file format creation
DESC FILE FORMAT bronze.ff_icon_forecast_csv;
