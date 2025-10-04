-- Test Complete Staging Workflow
-- End-to-end test of the data ingestion process

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;

-- Step 1: Test stored procedure execution
CALL staging.sp_refresh_staging();

-- Step 2: Verify data was ingested
SELECT 'Data Ingestion Test' as test_name,
       CASE
           WHEN COUNT(*) > 0 THEN 'PASS - ' || COUNT(*)::STRING || ' records ingested'
           ELSE 'FAIL - No data ingested'
       END as result
FROM staging.solar_measurements_raw
WHERE DATE(ingestion_timestamp) = CURRENT_DATE;

-- Step 3: Check measurement types
SELECT 'Measurement Types Test' as test_name,
       measurement_type,
       COUNT(*) as record_count,
       CASE
           WHEN COUNT(*) > 0 THEN 'PASS'
           ELSE 'FAIL'
       END as result
FROM staging.solar_measurements_raw
WHERE DATE(ingestion_timestamp) = CURRENT_DATE
GROUP BY measurement_type
ORDER BY measurement_type;

-- Step 4: Validate data quality
SELECT 'Data Quality Test' as test_name,
       measurement_type,
       json_data:properties:quality::STRING as quality_flag,
       COUNT(*) as count,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY measurement_type), 1) as percentage
FROM staging.solar_measurements_raw
WHERE DATE(ingestion_timestamp) = CURRENT_DATE
  AND json_data:properties:quality IS NOT NULL
GROUP BY measurement_type, quality_flag
ORDER BY measurement_type, quality_flag;

-- Step 5: Check station coverage
SELECT 'Station Coverage Test' as test_name,
       measurement_type,
       COUNT(DISTINCT json_data:properties:station:value::STRING) as unique_stations,
       CASE
           WHEN COUNT(DISTINCT json_data:properties:station:value::STRING) >= 50 THEN 'PASS'
           ELSE 'REVIEW - Only ' || COUNT(DISTINCT json_data:properties:station:value::STRING)::STRING || ' stations'
       END as result
FROM staging.solar_measurements_raw
WHERE DATE(ingestion_timestamp) = CURRENT_DATE
  AND json_data:properties:station:value IS NOT NULL
GROUP BY measurement_type
ORDER BY measurement_type;

-- Step 6: Verify timestamps are recent
SELECT 'Timestamp Freshness Test' as test_name,
       measurement_type,
       MAX(TO_TIMESTAMP(json_data:properties:date:value::STRING, 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"')) as latest_measurement,
       DATEDIFF('hour', MAX(TO_TIMESTAMP(json_data:properties:date:value::STRING, 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"')), CURRENT_TIMESTAMP()) as hours_ago,
       CASE
           WHEN DATEDIFF('hour', MAX(TO_TIMESTAMP(json_data:properties:date:value::STRING, 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"')), CURRENT_TIMESTAMP()) <= 24 THEN 'PASS'
           ELSE 'REVIEW - Data is ' || DATEDIFF('hour', MAX(TO_TIMESTAMP(json_data:properties:date:value::STRING, 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"')), CURRENT_TIMESTAMP())::STRING || ' hours old'
       END as result
FROM staging.solar_measurements_raw
WHERE DATE(ingestion_timestamp) = CURRENT_DATE
  AND json_data:properties:date:value IS NOT NULL
GROUP BY measurement_type
ORDER BY measurement_type;