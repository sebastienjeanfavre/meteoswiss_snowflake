USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA STAGING;

CREATE OR REPLACE PROCEDURE staging.sp_refresh_staging()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    global_radiation_url STRING := 'https://data.geo.admin.ch/ch.meteoschweiz.messwerte-globalstrahlung-10min/ch.meteoschweiz.messwerte-globalstrahlung-10min_en.json';
    sunshine_duration_url STRING := 'https://data.geo.admin.ch/ch.meteoschweiz.messwerte-sonnenscheindauer-10min/ch.meteoschweiz.messwerte-sonnenscheindauer-10min_en.json';
    result STRING := '';
BEGIN

    -- Log start of refresh
    result := 'Starting MeteoSwiss data refresh at ' || CURRENT_TIMESTAMP()::STRING || '\n';

    -- Refresh global radiation data
    result := result || 'Fetching global radiation data...\n';
    INSERT INTO staging.solar_measurements_raw (json_data, measurement_type, _metadata)
    SELECT
        PARSE_JSON(json) as json_data,
        'GLOBAL_RADIATION' as measurement_type,
        PARSE_JSON(json):_metadata as _metadata
    FROM TABLE(utils.get_meteoswiss_data(:global_radiation_url));

    result := result || 'Global radiation records inserted: ' || ROW_COUNT::STRING || '\n';

    -- Refresh sunshine duration data
    result := result || 'Fetching sunshine duration data...\n';
    INSERT INTO staging.solar_measurements_raw (json_data, measurement_type, _metadata)
    SELECT
        PARSE_JSON(json) as json_data,
        'SUNSHINE_DURATION' as measurement_type,
        PARSE_JSON(json):_metadata as _metadata
    FROM TABLE(utils.get_meteoswiss_data(:sunshine_duration_url));

    result := result || 'Sunshine duration records inserted: ' || ROW_COUNT::STRING || '\n';

    -- Update stations table with any new stations found
    result := result || 'Updating stations metadata...\n';
    MERGE INTO staging.stations s
    USING (
        SELECT DISTINCT
            json_data:id::STRING as station_id,
            json_data:properties:station_name::STRING as station_name,
            json_data:geometry:coordinates[1]::FLOAT as latitude,
            json_data:geometry:coordinates[0]::FLOAT as longitude,
            json_data:properties:altitude::FLOAT as elevation
        FROM staging.solar_measurements_raw
        WHERE ingestion_timestamp >= CURRENT_DATE()
        AND json_data:id IS NOT NULL
    ) src ON s.station_id = src.station_id
    WHEN NOT MATCHED THEN
        INSERT (station_id, station_name, latitude, longitude, elevation, updated_at)
        VALUES (src.station_id, src.station_name, src.latitude, src.longitude, src.elevation, CURRENT_TIMESTAMP())
    WHEN MATCHED THEN
        UPDATE SET
            station_name = src.station_name,
            latitude = src.latitude,
            longitude = src.longitude,
            elevation = src.elevation,
            updated_at = CURRENT_TIMESTAMP();

    result := result || 'Stations updated: ' || ROW_COUNT::STRING || '\n';
    result := result || 'MeteoSwiss data refresh completed at ' || CURRENT_TIMESTAMP()::STRING;

    RETURN result;
END;
$$;