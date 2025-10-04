-- Test Full Data Retrieval from MeteoSwiss API
-- This script verifies that all data is being retrieved from the API

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;

-- Count total features returned from API
SELECT
    'Total features returned' as test_name,
    COUNT(*) as feature_count
FROM TABLE(utils.get_meteoswiss_data('https://data.geo.admin.ch/ch.meteoschweiz.messwerte-globalstrahlung-10min/ch.meteoschweiz.messwerte-globalstrahlung-10min_en.json'));

-- Show sample of all stations
SELECT
    json:id::STRING as station_id,
    json:properties:station_name::STRING as station_name,
    json:properties:value::FLOAT as radiation_value,
    json:properties:unit::STRING as unit,
    json:properties:reference_ts::STRING as timestamp
FROM TABLE(utils.get_meteoswiss_data('https://data.geo.admin.ch/ch.meteoschweiz.messwerte-globalstrahlung-10min/ch.meteoschweiz.messwerte-globalstrahlung-10min_en.json'))
ORDER BY station_id;

-- Check for any error messages in metadata
SELECT
    json:_metadata:status_code::INT as status_code,
    json:_metadata:error::STRING as error_message,
    json:_metadata:response_time::FLOAT as response_time,
    COUNT(*) as count
FROM TABLE(utils.get_meteoswiss_data('https://data.geo.admin.ch/ch.meteoschweiz.messwerte-globalstrahlung-10min/ch.meteoschweiz.messwerte-globalstrahlung-10min_en.json'))
GROUP BY status_code, error_message, response_time;