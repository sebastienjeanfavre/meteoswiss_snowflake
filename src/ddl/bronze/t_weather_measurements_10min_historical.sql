-- ============================================================================
-- Bronze Table: Historical Weather Measurements (10-minute intervals)
-- ============================================================================
-- Historical 10-minute weather measurements from MeteoSwiss stations
--
-- Data coverage: Measurement start → Dec 31 last year
-- Granularity: 10-minute intervals
-- Update frequency: Manual/yearly backfill
-- Data source: MeteoSwiss STAC API (decade files)
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

CREATE TABLE IF NOT EXISTS bronze.t_weather_measurements_10min_historical (
    -- Metadata
    station_abbr VARCHAR(10),
    reference_timestamp TIMESTAMP_NTZ,

    -- Temperature measurements (°C)
    tre200s0 NUMBER(38,10),  -- Air temperature 2m above ground
    tre005s0 NUMBER(38,10),  -- Air temperature 5cm above ground
    tresurs0 NUMBER(38,10),  -- Surface temperature
    xchills0 NUMBER(38,10),  -- Wind chill temperature

    -- Humidity and dew point
    ure200s0 NUMBER(38,10),  -- Relative humidity 2m above ground (%)
    tde200s0 NUMBER(38,10),  -- Dew point 2m above ground (°C)
    pva200s0 NUMBER(38,10),  -- Water vapour pressure (hPa)

    -- Pressure measurements (hPa)
    prestas0 NUMBER(38,10),  -- Pressure at station level
    pp0qnhs0 NUMBER(38,10),  -- Pressure reduced to sea level QNH
    pp0qffs0 NUMBER(38,10),  -- Pressure reduced to sea level QFF
    ppz850s0 NUMBER(38,10),  -- Geopotential height 850 hPa
    ppz700s0 NUMBER(38,10),  -- Geopotential height 700 hPa

    -- Wind measurements
    fkl010z1 NUMBER(38,10),  -- Wind speed scalar 10m above ground (m/s)
    fve010z0 NUMBER(38,10),  -- Wind speed vector 10m above ground (m/s)
    fkl010z0 NUMBER(38,10),  -- Wind gust peak 10m above ground (m/s)
    dkl010z0 NUMBER(38,10),  -- Wind direction 10m above ground (degrees)
    wcc006s0 NUMBER(38,10),  -- Cloud cover

    -- Additional wind measurements
    fu3010z0 NUMBER(38,10),  -- Gust wind speed maximum (m/s)
    fkl010z3 NUMBER(38,10),  -- Wind speed scalar 10m above ground max (m/s)
    fu3010z1 NUMBER(38,10),  -- Gust wind speed 1s (m/s)
    fu3010z3 NUMBER(38,10),  -- Gust wind speed 3s (m/s)

    -- Precipitation
    rre150z0 NUMBER(38,10),  -- Precipitation (mm)

    -- Sunshine and radiation
    htoauts0 NUMBER(38,10),  -- Sunshine duration (min)
    gre000z0 NUMBER(38,10),  -- Global radiation (W/m²)

    -- Additional measurements
    ods000z0 NUMBER(38,10),  -- Snow depth (cm)
    oli000z0 NUMBER(38,10),  -- Lysimeter infiltration
    olo000z0 NUMBER(38,10),  -- Lysimeter outflow
    osr000z0 NUMBER(38,10),  -- Lysimeter storage
    sre000z0 NUMBER(38,10),  -- Reflected short-wave radiation (W/m²)

    -- Audit columns
    file_name VARCHAR(500),
    loaded_at TIMESTAMP_NTZ DEFAULT SYSDATE()
);

COMMENT ON TABLE bronze.t_weather_measurements_10min_historical IS
    'Historical 10-minute weather measurements from MeteoSwiss stations (backfill data from measurement start to Dec 31 last year)';
COMMENT ON COLUMN bronze.t_weather_measurements_10min_historical.station_abbr IS 'Station abbreviation code';
COMMENT ON COLUMN bronze.t_weather_measurements_10min_historical.reference_timestamp IS 'Measurement timestamp (10-minute intervals)';
COMMENT ON COLUMN bronze.t_weather_measurements_10min_historical.gre000z0 IS 'Global solar radiation in W/m²';
COMMENT ON COLUMN bronze.t_weather_measurements_10min_historical.file_name IS 'Source CSV filename for audit trail';
