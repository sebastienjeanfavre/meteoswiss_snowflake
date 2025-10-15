-- ============================================================================
-- Bronze Table: Weather Station Metadata
-- ============================================================================
-- MeteoSwiss automatic weather station metadata including location,
-- coordinates, elevation, and operational details
--
-- Data coverage: All active MeteoSwiss stations (~180 stations)
-- Update frequency: Weekly on Sunday (automated)
-- Data source: MeteoSwiss STAC API (collection-level asset)
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

CREATE TABLE IF NOT EXISTS bronze.t_weather_stations (
    -- Station identifiers
    station_abbr VARCHAR(10),
    station_name VARCHAR(200),
    station_canton VARCHAR(10),
    station_wigos_id VARCHAR(50),

    -- Station type (multilingual)
    station_type_de VARCHAR(100),
    station_type_fr VARCHAR(100),
    station_type_it VARCHAR(100),
    station_type_en VARCHAR(100),

    -- Operational details
    station_dataowner VARCHAR(100),
    station_data_since DATE,

    -- Elevation (meters above sea level)
    station_height_masl NUMBER(10,2),
    station_height_barometer_masl NUMBER(10,2),

    -- Coordinates - Swiss LV95 system
    station_coordinates_lv95_east NUMBER(12,2),
    station_coordinates_lv95_north NUMBER(12,2),

    -- Coordinates - WGS84 system
    station_coordinates_wgs84_lat NUMBER(10,6),
    station_coordinates_wgs84_lon NUMBER(10,6),

    -- Station exposition/terrain type (multilingual)
    station_exposition_de VARCHAR(200),
    station_exposition_fr VARCHAR(200),
    station_exposition_it VARCHAR(200),
    station_exposition_en VARCHAR(200),

    -- Station information URLs (multilingual)
    station_url_de VARCHAR(500),
    station_url_fr VARCHAR(500),
    station_url_it VARCHAR(500),
    station_url_en VARCHAR(500),

    -- Audit columns
    file_name VARCHAR(500),
    loaded_at TIMESTAMP_NTZ DEFAULT SYSDATE()
);

COMMENT ON TABLE bronze.t_weather_stations IS
    'MeteoSwiss automatic weather station metadata including location, coordinates, elevation, and operational details (updated weekly)';

COMMENT ON COLUMN bronze.t_weather_stations.station_abbr IS 'Station abbreviation code (primary identifier)';
COMMENT ON COLUMN bronze.t_weather_stations.station_name IS 'Full station name';
COMMENT ON COLUMN bronze.t_weather_stations.station_wigos_id IS 'WMO Integrated Global Observing System identifier';
COMMENT ON COLUMN bronze.t_weather_stations.station_height_masl IS 'Station elevation in meters above sea level';
COMMENT ON COLUMN bronze.t_weather_stations.station_coordinates_wgs84_lat IS 'Latitude in WGS84 coordinate system';
COMMENT ON COLUMN bronze.t_weather_stations.station_coordinates_wgs84_lon IS 'Longitude in WGS84 coordinate system';
COMMENT ON COLUMN bronze.t_weather_stations.file_name IS 'Source CSV filename for audit trail';
