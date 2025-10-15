-- ============================================================================
-- Gold Layer - Weather Measurements Fact Table (Realtime)
-- ============================================================================
-- Business-friendly fact table for 10-minute weather measurements with realtime data.
-- Includes all three data tiers: historical, recent, and now (live updates).
-- Provides clean, denormalized measurement data with business-friendly names.
--
-- Coverage: All historical data + recent data + live data (last 24 hours)
-- Grain: One row per station per 10-minute interval
-- Usage: Premium analytics with realtime weather monitoring
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA GOLD;

-- ============================================================================
-- FACT TABLE: WEATHER MEASUREMENTS (REALTIME)
-- ============================================================================
-- Contains all weather measurements at 10-minute granularity including live data
-- Includes all three tiers: historical, recent, and now
-- Optimized for business analytics with descriptive column names

CREATE OR REPLACE VIEW gold.fact_weather_measurements_10min_realtime AS
SELECT
    -- Dimension keys
    station_abbr AS station_key,

    -- Time dimensions
    reference_timestamp AS measurement_timestamp,
    DATE(reference_timestamp) AS measurement_date,
    HOUR(reference_timestamp) AS measurement_hour,
    DAYOFWEEK(reference_timestamp) AS day_of_week,
    MONTH(reference_timestamp) AS measurement_month,
    YEAR(reference_timestamp) AS measurement_year,

    -- Temperature measurements (°C)
    tre200s0 AS temperature_2m,
    tre005s0 AS temperature_5cm,
    tresurs0 AS surface_temperature,
    xchills0 AS wind_chill_temperature,

    -- Humidity measurements
    ure200s0 AS relative_humidity_pct,
    tde200s0 AS dew_point_2m,
    pva200s0 AS water_vapor_pressure_hpa,

    -- Pressure measurements (hPa)
    prestas0 AS pressure_station_level_hpa,
    pp0qnhs0 AS pressure_sea_level_qnh_hpa,
    pp0qffs0 AS pressure_sea_level_qff_hpa,
    ppz850s0 AS geopotential_height_850hpa,
    ppz700s0 AS geopotential_height_700hpa,

    -- Wind measurements
    fkl010z1 AS wind_speed_mean_ms,
    fve010z0 AS wind_speed_vector_mean_ms,
    fkl010z0 AS wind_gust_peak_ms,
    dkl010z0 AS wind_direction_mean_degrees,
    fu3010z0 AS wind_speed_max_ms,
    fkl010z3 AS wind_speed_scalar_max_ms,
    fu3010z1 AS wind_gust_1s_ms,
    fu3010z3 AS wind_gust_3s_ms,

    -- Cloud cover
    wcc006s0 AS cloud_cover,

    -- Precipitation (mm) - 10-minute total
    rre150z0 AS precipitation_total_mm,

    -- Solar radiation (W/m²)
    htoauts0 AS sunshine_duration_total_min,
    gre000z0 AS global_radiation_mean_wm2,
    sre000z0 AS reflected_radiation_mean_wm2,

    -- Snow depth (cm)
    ods000z0 AS snow_depth_cm,

    -- Lysimeter measurements
    oli000z0 AS lysimeter_infiltration,
    olo000z0 AS lysimeter_outflow,
    osr000z0 AS lysimeter_storage,

    -- Metadata
    data_tier AS source_tier,

    -- Audit fields
    file_name AS source_file,
    loaded_at AS last_updated

FROM silver.dt_weather_measurements_10min;

COMMENT ON VIEW gold.fact_weather_measurements_10min_realtime IS
    'Business-optimized weather measurements fact table with realtime data at 10-minute granularity. Contains unified, deduplicated measurements from all tiers (historical/recent/now) with business-friendly column names. Includes live data updates for realtime weather monitoring and analytics.';

-- ============================================================================
-- USAGE EXAMPLES
-- ============================================================================

-- Basic fact table query (realtime data)
-- SELECT
--     station_key,
--     measurement_timestamp,
--     temperature_2m,
--     precipitation_total_mm,
--     wind_speed_mean_ms
-- FROM gold.fact_weather_measurements_10min_realtime
-- WHERE measurement_timestamp >= DATEADD('hour', -1, CURRENT_TIMESTAMP());

-- Join with dimension for enriched analysis
-- SELECT
--     d.station_name,
--     d.station_canton,
--     d.elevation_meters,
--     f.measurement_timestamp,
--     f.temperature_2m,
--     f.precipitation_total_mm,
--     f.wind_speed_mean_ms
-- FROM gold.fact_weather_measurements_10min_realtime f
-- JOIN gold.dim_weather_stations d
--     ON f.station_key = d.station_key
-- WHERE f.measurement_date >= CURRENT_DATE - 7
--     AND d.station_canton = 'GE'
-- ORDER BY f.measurement_timestamp DESC;

-- Daily aggregations by station
-- SELECT
--     station_key,
--     measurement_date,
--     AVG(temperature_2m) as avg_temperature,
--     MAX(temperature_2m) as max_temperature,
--     MIN(temperature_2m) as min_temperature,
--     SUM(precipitation_total_mm) as total_precipitation,
--     AVG(wind_speed_mean_ms) as avg_wind_speed
-- FROM gold.fact_weather_measurements_10min
-- WHERE measurement_date >= CURRENT_DATE - 30
-- GROUP BY station_key, measurement_date
-- ORDER BY station_key, measurement_date;

-- Hourly analysis with dimension join
-- SELECT
--     d.station_name,
--     d.station_canton,
--     f.measurement_date,
--     f.measurement_hour,
--     AVG(f.temperature_2m) as avg_temp,
--     AVG(f.relative_humidity_pct) as avg_humidity,
--     SUM(f.precipitation_total_mm) as total_precip
-- FROM gold.fact_weather_measurements_10min_realtime f
-- JOIN gold.dim_weather_stations d
--     ON f.station_key = d.station_key
-- WHERE f.measurement_date = CURRENT_DATE - 1
-- GROUP BY d.station_name, d.station_canton, f.measurement_date, f.measurement_hour
-- ORDER BY d.station_name, f.measurement_hour;

-- Canton-level daily aggregations
-- SELECT
--     d.station_canton,
--     f.measurement_date,
--     COUNT(DISTINCT f.station_key) as active_stations,
--     AVG(f.temperature_2m) as avg_temperature,
--     SUM(f.precipitation_total_mm) as total_precipitation,
--     MAX(f.wind_gust_peak_ms) as max_wind_gust
-- FROM gold.fact_weather_measurements_10min_realtime f
-- JOIN gold.dim_weather_stations d
--     ON f.station_key = d.station_key
-- WHERE f.measurement_date >= CURRENT_DATE - 7
-- GROUP BY d.station_canton, f.measurement_date
-- ORDER BY d.station_canton, f.measurement_date;

-- Time series analysis - last 24 hours
-- SELECT
--     d.station_name,
--     f.measurement_timestamp,
--     f.temperature_2m,
--     f.relative_humidity_pct,
--     f.precipitation_total_mm,
--     f.wind_speed_mean_ms
-- FROM gold.fact_weather_measurements_10min_realtime f
-- JOIN gold.dim_weather_stations d
--     ON f.station_key = d.station_key
-- WHERE f.measurement_timestamp >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
--     AND f.station_key IN ('BAS', 'GVE', 'ZRH')
-- ORDER BY f.station_key, f.measurement_timestamp;

-- ============================================================================
-- AGGREGATION QUERIES
-- ============================================================================

-- Monthly statistics by station
-- SELECT
--     d.station_name,
--     d.station_canton,
--     f.measurement_year,
--     f.measurement_month,
--     COUNT(*) as measurement_count,
--     AVG(f.temperature_2m) as avg_temperature,
--     MAX(f.temperature_2m) as max_temperature,
--     MIN(f.temperature_2m) as min_temperature,
--     SUM(f.precipitation_total_mm) as total_precipitation,
--     AVG(f.global_radiation_mean_wm2) as avg_solar_radiation
-- FROM gold.fact_weather_measurements_10min_realtime f
-- JOIN gold.dim_weather_stations d
--     ON f.station_key = d.station_key
-- WHERE f.measurement_year = YEAR(CURRENT_DATE)
-- GROUP BY d.station_name, d.station_canton, f.measurement_year, f.measurement_month
-- ORDER BY d.station_name, f.measurement_month;

-- Weather extremes (last 30 days)
-- SELECT
--     d.station_name,
--     d.station_canton,
--     MAX(f.temperature_2m) as max_temperature,
--     MIN(f.temperature_2m) as min_temperature,
--     MAX(f.wind_gust_peak_ms) as max_wind_gust,
--     MAX(f.precipitation_total_mm) as max_10min_precipitation,
--     MAX(f.snow_depth_cm) as max_snow_depth
-- FROM gold.fact_weather_measurements_10min_realtime f
-- JOIN gold.dim_weather_stations d
--     ON f.station_key = d.station_key
-- WHERE f.measurement_date >= CURRENT_DATE - 30
-- GROUP BY d.station_name, d.station_canton
-- ORDER BY max_temperature DESC;

-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================

-- Count measurements by date
-- SELECT
--     measurement_date,
--     COUNT(*) as measurement_count,
--     COUNT(DISTINCT station_key) as station_count
-- FROM gold.fact_weather_measurements_10min
-- WHERE measurement_date >= CURRENT_DATE - 7
-- GROUP BY measurement_date
-- ORDER BY measurement_date DESC;

-- Data completeness by measurement type
-- SELECT
--     measurement_date,
--     COUNT(*) as total_measurements,
--     COUNT(temperature_2m) as temp_measurements,
--     COUNT(precipitation_total_mm) as precip_measurements,
--     COUNT(wind_speed_mean_ms) as wind_measurements,
--     COUNT(global_radiation_mean_wm2) as solar_measurements,
--     ROUND(COUNT(temperature_2m) * 100.0 / COUNT(*), 2) as temp_completeness_pct
-- FROM gold.fact_weather_measurements_10min
-- WHERE measurement_date >= CURRENT_DATE - 7
-- GROUP BY measurement_date
-- ORDER BY measurement_date DESC;

-- Check for duplicates (should be 0)
-- SELECT
--     station_key,
--     measurement_timestamp,
--     COUNT(*) as duplicate_count
-- FROM gold.fact_weather_measurements_10min
-- GROUP BY station_key, measurement_timestamp
-- HAVING COUNT(*) > 1;

SELECT 'Gold layer fact_weather_measurements_10min_realtime created successfully' AS result;
