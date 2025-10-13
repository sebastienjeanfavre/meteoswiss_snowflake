-- ============================================================================
-- Gold Layer - Weather Station Dimension
-- ============================================================================
-- Business-friendly dimension table for weather station master data.
-- Provides clean, denormalized station metadata for analytics and reporting.
--
-- Usage: Join with fact_weather_measurements_10min for dimensional analysis
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA GOLD;

-- ============================================================================
-- DIMENSION: WEATHER STATIONS
-- ============================================================================
-- Type I Slowly Changing Dimension (SCD1) design
-- Overwrites station attributes when they change (no history tracking)

CREATE OR REPLACE VIEW gold.dim_weather_stations AS
SELECT
    -- Surrogate key (station abbreviation as natural key)
    station_abbr AS station_key,

    -- Station identification
    station_abbr,
    station_name,
    station_canton,
    station_wigos_id,

    -- Station classification (default language: English)
    station_type_en AS station_type,
    station_type_de,
    station_type_fr,
    station_type_it,

    -- Operational metadata
    station_dataowner AS data_owner,
    station_data_since AS operational_since,

    -- Geographic attributes - Elevation
    station_height_masl AS elevation_meters,
    station_height_barometer_masl AS barometer_elevation_meters,

    -- Geographic attributes - Coordinates (WGS84)
    station_coordinates_wgs84_lat AS latitude,
    station_coordinates_wgs84_lon AS longitude,

    -- Geographic attributes - Swiss Coordinates (LV95)
    station_coordinates_lv95_east AS lv95_east,
    station_coordinates_lv95_north AS lv95_north,

    -- Terrain characteristics (default language: English)
    station_exposition_en AS terrain_exposition,
    station_exposition_de AS terrain_exposition_de,
    station_exposition_fr AS terrain_exposition_fr,
    station_exposition_it AS terrain_exposition_it,

    -- Information URLs (multilingual)
    station_url_en AS info_url,
    station_url_de AS info_url_de,
    station_url_fr AS info_url_fr,
    station_url_it AS info_url_it,

    -- Data quality attributes
    CASE
        WHEN station_coordinates_wgs84_lat IS NULL OR station_coordinates_wgs84_lon IS NULL
        THEN 'Incomplete'
        ELSE 'Complete'
    END AS data_quality_flag,

    -- Audit fields
    file_name AS source_file,
    loaded_at AS last_updated

FROM silver.v_weather_stations;

COMMENT ON VIEW gold.dim_weather_stations IS
    'Business-optimized weather station dimension. Contains clean, denormalized station master data with business-friendly column names. Use as reference data for weather measurement analytics.';

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

-- Basic dimension lookup
-- SELECT * FROM gold.dim_weather_stations WHERE station_key = 'BAS';

-- Join with fact table for enriched analysis
-- SELECT
--     d.station_name,
--     d.station_canton,
--     d.elevation_meters,
--     f.measurement_timestamp,
--     f.temperature_2m
-- FROM gold.fact_weather_measurements f
-- JOIN gold.dim_weather_stations d
--     ON f.station_key = d.station_key
-- WHERE f.measurement_date >= CURRENT_DATE - 7;

-- Station inventory by canton
-- SELECT
--     station_canton,
--     COUNT(*) as station_count,
--     AVG(elevation_meters) as avg_elevation,
--     MIN(operational_since) as oldest_station
-- FROM gold.dim_weather_stations
-- GROUP BY station_canton
-- ORDER BY station_count DESC;

-- High elevation stations
-- SELECT
--     station_name,
--     station_canton,
--     elevation_meters,
--     terrain_exposition
-- FROM gold.dim_weather_stations
-- WHERE elevation_meters > 2000
-- ORDER BY elevation_meters DESC;

-- Data quality check
-- SELECT
--     data_quality_flag,
--     COUNT(*) as station_count
-- FROM gold.dim_weather_stations
-- GROUP BY data_quality_flag;

-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================

-- Count total stations
-- SELECT COUNT(*) as total_stations FROM gold.dim_weather_stations;

-- Check for missing coordinates
-- SELECT
--     station_key,
--     station_name,
--     data_quality_flag
-- FROM gold.dim_weather_stations
-- WHERE data_quality_flag = 'Incomplete';

-- View most recently updated stations
-- SELECT
--     station_name,
--     station_canton,
--     last_updated
-- FROM gold.dim_weather_stations
-- ORDER BY last_updated DESC
-- LIMIT 10;

SELECT 'Gold layer dim_weather_stations created successfully' AS result;
