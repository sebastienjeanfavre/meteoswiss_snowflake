-- Test MeteoSwiss API Connection
-- Verify that the API integration is working correctly

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;

-- Test 1: Check if API function exists
SELECT 'Function get_meteoswiss_data exists' as test_name,
       CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END as result
FROM information_schema.functions
WHERE function_name = 'GET_METEOSWISS_DATA'
  AND function_schema = 'UTILS';

-- Test 2: Test API call to global radiation endpoint
SELECT 'API call to global radiation endpoint' as test_name,
       CASE
         WHEN json:_metadata:status_code = 200 THEN 'PASS'
         ELSE 'FAIL - Status: ' || json:_metadata:status_code::STRING
       END as result,
       json:_metadata:response_time::FLOAT as response_time_seconds
FROM TABLE(utils.get_meteoswiss_data('https://data.geo.admin.ch/ch.meteoschweiz.messwerte-globalstrahlung-10min/ch.meteoschweiz.messwerte-globalstrahlung-10min_en.json'))
LIMIT 1;

-- Test 3: Test API call to sunshine duration endpoint
SELECT 'API call to sunshine duration endpoint' as test_name,
       CASE
         WHEN json:_metadata:status_code = 200 THEN 'PASS'
         ELSE 'FAIL - Status: ' || json:_metadata:status_code::STRING
       END as result,
       json:_metadata:response_time::FLOAT as response_time_seconds
FROM TABLE(utils.get_meteoswiss_data('https://data.geo.admin.ch/ch.meteoschweiz.messwerte-sonnenscheindauer-10min/ch.meteoschweiz.messwerte-sonnenscheindauer-10min_en.json'))
LIMIT 1;

-- Test 4: Verify data structure
SELECT 'Data structure validation' as test_name,
       CASE
         WHEN json:properties:station:value IS NOT NULL
          AND json:properties:date:value IS NOT NULL
          AND json:properties:value IS NOT NULL
         THEN 'PASS'
         ELSE 'FAIL'
       END as result,
       json:properties:station:value::STRING as sample_station_id,
       json:properties:date:value::STRING as sample_timestamp
FROM TABLE(utils.get_meteoswiss_data('https://data.geo.admin.ch/ch.meteoschweiz.messwerte-globalstrahlung-10min/ch.meteoschweiz.messwerte-globalstrahlung-10min_en.json'))
LIMIT 1;