-- ============================================================================
-- Scheduled Task: Refresh Bronze Now Data
-- ============================================================================
-- This task automates the frequent refresh of realtime weather data by calling
-- the sp_load_weather_measurements_10min_now stored procedure.
--
-- Schedule: Every 10 minutes (synchronized with MeteoSwiss update cycle)
-- Warehouse: METEOSWISS_WH
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA COMMON;

-- Create the task
CREATE OR REPLACE TASK common.task_bronze_now_data
    WAREHOUSE = METEOSWISS_WH
    SCHEDULE = 'USING CRON */10 * * * * UTC'
    COMMENT = 'Runs every 10 minutes to fetch and load realtime weather data from MeteoSwiss STAC API (synchronized with MeteoSwiss update frequency)'
AS
    CALL bronze.sp_load_weather_measurements_10min_now();

-- Task is created in SUSPENDED state by default
-- To enable the task, run:
-- ALTER TASK common.task_bronze_now_data RESUME;

-- To check task status:
-- SHOW TASKS LIKE 'task_bronze_now_data' IN SCHEMA common;

-- To view task run history:
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP()),
--     TASK_NAME => 'TASK_BRONZE_NOW_DATA'
-- ))
-- ORDER BY SCHEDULED_TIME DESC;

-- To manually execute the task (for testing):
-- EXECUTE TASK common.task_bronze_now_data;
