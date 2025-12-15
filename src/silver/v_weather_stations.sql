-- ============================================================================
-- Silver Layer - Weather Station Metadata View
-- ============================================================================
-- Provides clean, business-friendly access to weather station metadata
-- from the bronze layer.
--
-- Architecture:
-- Bronze → Silver → Gold
-- - Bronze: Raw station metadata (t_weather_stations)
-- - Silver: Cleansed, business-ready view
-- - Gold: Business-specific aggregations
--
-- Data characteristics:
-- - ~180 weather stations across Switzerland
-- - Updated weekly on Sunday via automated stored procedure
-- - Static reference data for joining with measurement data
--
-- Prerequisites:
-- - Bronze layer table bronze.t_weather_stations must exist
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA SILVER;

-- ============================================================================
-- WEATHER STATIONS VIEW
-- ============================================================================
-- Lightweight view for station reference data
-- Use this for joining with measurement data or station lookups

CREATE OR REPLACE VIEW silver.v_weather_stations AS
SELECT
    -- Station identifiers
    station_abbr,
    station_name,
    station_canton,
    station_wigos_id,

    -- Station type (multilingual)
    station_type_de,
    station_type_fr,
    station_type_it,
    station_type_en,

    -- Operational details
    station_dataowner,
    station_data_since,

    -- Elevation (meters above sea level)
    station_height_masl,
    station_height_barometer_masl,

    -- Coordinates - Swiss LV95 system (Swiss projection)
    station_coordinates_lv95_east,
    station_coordinates_lv95_north,

    -- Coordinates - WGS84 system (latitude/longitude)
    station_coordinates_wgs84_lat,
    station_coordinates_wgs84_lon,

    -- Station exposition/terrain type (multilingual)
    station_exposition_de,
    station_exposition_fr,
    station_exposition_it,
    station_exposition_en,

    -- Station information URLs (multilingual)
    station_url_de,
    station_url_fr,
    station_url_it,
    station_url_en,

    -- Audit columns (for traceability)
    file_name,
    loaded_at
FROM bronze.t_weather_stations;

COMMENT ON VIEW silver.v_weather_stations IS
    'Clean view of MeteoSwiss weather station metadata including location, coordinates, elevation, and operational details. Updated weekly. Use for joining with measurement data or station lookups.';

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

-- Basic station lookup
-- SELECT * FROM silver.v_weather_stations WHERE station_abbr = 'BAS';

-- Join with measurement data
-- SELECT
--     s.station_name,
--     s.station_canton,
--     s.station_height_masl,
--     m.reference_timestamp,
--     m.tre200s0 as temperature_2m
-- FROM silver.dt_weather_measurements_10min m
-- JOIN silver.v_weather_stations s
--     ON m.station_abbr = s.station_abbr
-- WHERE m.reference_timestamp >= CURRENT_DATE - 7
--     AND s.station_canton = 'GE'
-- ORDER BY m.reference_timestamp DESC;

-- Count stations by canton
-- SELECT
--     station_canton,
--     COUNT(*) as station_count
-- FROM silver.v_weather_stations
-- GROUP BY station_canton
-- ORDER BY station_count DESC;

-- Find stations at high elevation
-- SELECT
--     station_abbr,
--     station_name,
--     station_height_masl,
--     station_canton
-- FROM silver.v_weather_stations
-- WHERE station_height_masl > 2000
-- ORDER BY station_height_masl DESC;

-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================

-- Count total stations
-- SELECT COUNT(*) as total_stations FROM silver.v_weather_stations;

-- Check for stations with missing coordinates
-- SELECT
--     station_abbr,
--     station_name,
--     station_coordinates_wgs84_lat,
--     station_coordinates_wgs84_lon
-- FROM silver.v_weather_stations
-- WHERE station_coordinates_wgs84_lat IS NULL
--     OR station_coordinates_wgs84_lon IS NULL;

-- View most recently loaded data
-- SELECT
--     COUNT(*) as station_count,
--     MAX(loaded_at) as last_refresh
-- FROM silver.v_weather_stations;

SELECT 'Silver layer v_weather_stations created successfully' AS result;
