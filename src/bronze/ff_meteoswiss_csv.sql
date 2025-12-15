-- ============================================================================
-- File Format: MeteoSwiss CSV Format
-- ============================================================================
-- Defines the CSV file format for all MeteoSwiss data files.
-- Used by all bronze layer stages (historical, recent, now, stations).
--
-- Format specifications:
-- - Type: CSV (Comma-Separated Values, but using semicolon delimiter)
-- - Delimiter: Semicolon (;)
-- - Encoding: UTF-8
-- - Header: First row contains column names (skip it during load)
-- - Compression: None (files are not compressed)
-- - Null handling: Empty strings, 'NULL', and '-' treated as NULL
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

CREATE OR REPLACE FILE FORMAT bronze.ff_meteoswiss_csv
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
    NULL_IF = ('', 'NULL', '-')
    ENCODING = 'UTF8'
    COMMENT = 'CSV file format for MeteoSwiss data files (semicolon-delimited, UTF-8 encoded, with header row)';

-- Verify file format creation
DESC FILE FORMAT bronze.ff_meteoswiss_csv;
