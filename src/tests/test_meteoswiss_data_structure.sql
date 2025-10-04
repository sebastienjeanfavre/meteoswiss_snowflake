-- Test MeteoSwiss Data Structure
-- Verify the JSON structure from API responses matches our expectations

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;

-- Sample API call to inspect data structure
WITH sample_data AS (
    SELECT json as api_response
    FROM TABLE(utils.get_meteoswiss_data('https://data.geo.admin.ch/ch.meteoschweiz.messwerte-globalstrahlung-10min/ch.meteoschweiz.messwerte-globalstrahlung-10min_en.json'))
    LIMIT 5
)

SELECT
    'Global Radiation Data Structure Test' as test_category,
    api_response:id::STRING as station_id,
    api_response:properties:station_name::STRING as station_name,
    api_response:properties:reference_ts::STRING as measurement_timestamp,
    api_response:properties:value::FLOAT as measurement_value,
    api_response:properties:unit::STRING as measurement_unit,
    api_response:properties:altitude::STRING as altitude,
    api_response:geometry:coordinates[0]::FLOAT as longitude,
    api_response:geometry:coordinates[1]::FLOAT as latitude,
    api_response:_metadata:status_code::INT as api_status_code,
    api_response:_metadata:response_time::FLOAT as api_response_time,
    CASE
        WHEN api_response:id IS NOT NULL
         AND api_response:properties:reference_ts IS NOT NULL
         AND api_response:properties:value IS NOT NULL
         AND api_response:geometry:coordinates IS NOT NULL
        THEN 'PASS'
        ELSE 'FAIL'
    END as structure_test_result
FROM sample_data;

-- Test sunshine duration data structure
WITH sample_sunshine AS (
    SELECT json as api_response
    FROM TABLE(utils.get_meteoswiss_data('https://data.geo.admin.ch/ch.meteoschweiz.messwerte-sonnenscheindauer-10min/ch.meteoschweiz.messwerte-sonnenscheindauer-10min_en.json'))
    LIMIT 5
)

SELECT
    'Sunshine Duration Data Structure Test' as test_category,
    api_response:id::STRING as station_id,
    api_response:properties:station_name::STRING as station_name,
    api_response:properties:reference_ts::STRING as measurement_timestamp,
    api_response:properties:value::FLOAT as measurement_value,
    api_response:properties:unit::STRING as measurement_unit,
    api_response:properties:altitude::STRING as altitude,
    api_response:geometry:coordinates[0]::FLOAT as longitude,
    api_response:geometry:coordinates[1]::FLOAT as latitude,
    api_response:_metadata:status_code::INT as api_status_code,
    api_response:_metadata:response_time::FLOAT as api_response_time,
    CASE
        WHEN api_response:id IS NOT NULL
         AND api_response:properties:reference_ts IS NOT NULL
         AND api_response:properties:value IS NOT NULL
         AND api_response:geometry:coordinates IS NOT NULL
        THEN 'PASS'
        ELSE 'FAIL'
    END as structure_test_result
FROM sample_sunshine;