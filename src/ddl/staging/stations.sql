USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA STAGING;

CREATE OR REPLACE TABLE staging.stations (
    station_id VARCHAR(20) PRIMARY KEY,
    station_name VARCHAR(200),
    latitude FLOAT,
    longitude FLOAT,
    elevation FLOAT,
    canton VARCHAR(10),
    station_type VARCHAR(50),
    active_since DATE,
    active_until DATE,
    data_source VARCHAR(50) DEFAULT 'MeteoSwiss',
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Add comments for documentation
COMMENT ON TABLE staging.stations IS 'Weather stations metadata from MeteoSwiss network';
COMMENT ON COLUMN staging.stations.station_id IS 'Unique identifier for weather station';
COMMENT ON COLUMN staging.stations.station_name IS 'Human-readable name of the weather station';
COMMENT ON COLUMN staging.stations.latitude IS 'Latitude coordinate in decimal degrees';
COMMENT ON COLUMN staging.stations.longitude IS 'Longitude coordinate in decimal degrees';
COMMENT ON COLUMN staging.stations.elevation IS 'Elevation above sea level in meters';
COMMENT ON COLUMN staging.stations.canton IS 'Swiss canton abbreviation';
COMMENT ON COLUMN staging.stations.station_type IS 'Type of weather station (e.g., automatic, manual)';
COMMENT ON COLUMN staging.stations.active_since IS 'Date when station became active';
COMMENT ON COLUMN staging.stations.active_until IS 'Date when station was deactivated (NULL if still active)';