-- ============================================================================
-- File Format: MeteoSwiss Stations CSV Format
-- ============================================================================
-- Defines the CSV file format for all MeteoSwiss station metadata files.
-- Used in bronze layer.
--
-- Format specifications:
-- - Type: CSV (Comma-Separated Values, but using semicolon delimiter)
-- - Delimiter: Semicolon (;)
-- - Encoding: ISO-8859-1 (because of french accents)
-- - Header: First row contains column names (skip it during load)
-- - Null handling: Empty strings, 'NULL', and '-' treated as NULL
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

CREATE OR REPLACE FILE FORMAT bronze.ff_csv_latin1
    TYPE = 'CSV'
    FIELD_DELIMITER = ';'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ESCAPE = 'NONE'
    ESCAPE_UNENCLOSED_FIELD = 'NONE'
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    ENCODING = 'ISO-8859-1'
    NULL_IF = ('', 'NULL', '-')
    COMMENT = 'CSV file format for MeteoSwiss station metadata files';

-- Verify file format creation
DESC FILE FORMAT bronze.ff_csv_latin1;
