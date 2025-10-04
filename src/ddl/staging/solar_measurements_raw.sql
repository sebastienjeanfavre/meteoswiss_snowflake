USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA STAGING;

CREATE OR REPLACE TABLE staging.solar_measurements_raw (
    json_data VARIANT,
    measurement_type VARCHAR(50),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _metadata VARIANT
);

-- Add comments for documentation
ALTER TABLE staging.solar_measurements_raw
ADD COLUMN COMMENT 'Raw solar measurement data from MeteoSwiss API endpoints';

COMMENT ON COLUMN staging.solar_measurements_raw.json_data IS 'Raw JSON data from MeteoSwiss API response';
COMMENT ON COLUMN staging.solar_measurements_raw.measurement_type IS 'Type of measurement: GLOBAL_RADIATION or SUNSHINE_DURATION';
COMMENT ON COLUMN staging.solar_measurements_raw.ingestion_timestamp IS 'Timestamp when data was ingested into Snowflake';
COMMENT ON COLUMN staging.solar_measurements_raw._metadata IS 'API call metadata including response time and status';