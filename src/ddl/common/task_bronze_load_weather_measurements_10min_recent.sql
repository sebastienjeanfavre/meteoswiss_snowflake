-- ============================================================================
-- Scheduled Task: Refresh Bronze Recent Data
-- ============================================================================
-- This task automates the daily refresh of recent weather data by calling
-- the sp_load_weather_measurements_10min_recent stored procedure.
--
-- Schedule: Daily at 13:00 UTC (1 hour after MeteoSwiss updates at 12:00 UTC)
-- Warehouse: METEOSWISS_WH
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA COMMON;

-- Create the task
CREATE OR REPLACE TASK common.task_bronze_load_weather_measurements_10min_recent
    WAREHOUSE = METEOSWISS_WH
    SCHEDULE = 'USING CRON 0 13 * * * UTC'
    COMMENT = 'Daily task to fetch and load recent weather data from MeteoSwiss STAC API'
AS
    CALL bronze.sp_load_weather_measurements_10min_recent();

-- Task is created in SUSPENDED state by default
-- To enable the task, run:
-- ALTER TASK common.task_bronze_load_weather_measurements_10min_recent RESUME;

-- To check task status:
-- SHOW TASKS LIKE 'task_bronze_load_weather_measurements_10min_recent' IN SCHEMA common;

-- To view task run history:
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
--     TASK_NAME => 'TASK_BRONZE_LOAD_WEATHER_MEASUREMENTS_10MIN_RECENT'
-- ))
-- ORDER BY SCHEDULED_TIME DESC;

-- To manually execute the task (for testing):
-- EXECUTE TASK common.task_bronze_load_weather_measurements_10min_recent;
