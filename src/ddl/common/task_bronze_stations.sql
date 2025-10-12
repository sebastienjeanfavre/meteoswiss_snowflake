-- ============================================================================
-- Scheduled Task: Refresh Bronze Station Metadata
-- ============================================================================
-- This task automates the weekly refresh of weather station metadata by calling
-- the sp_load_weather_stations stored procedure.
--
-- Schedule: Weekly on Sunday at 02:00 UTC (stations rarely change)
-- Warehouse: METEOSWISS_WH
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA COMMON;

-- Create the task
CREATE OR REPLACE TASK common.task_bronze_stations
    WAREHOUSE = METEOSWISS_WH
    SCHEDULE = 'USING CRON 0 2 * * 0 UTC'
    COMMENT = 'Weekly task to fetch and load station metadata from MeteoSwiss STAC API (runs every Sunday at 02:00 UTC)'
AS
    CALL bronze.sp_load_weather_stations();

-- Task is created in SUSPENDED state by default
-- To enable the task, run:
-- ALTER TASK common.task_bronze_stations RESUME;

-- To check task status:
-- SHOW TASKS LIKE 'task_bronze_stations' IN SCHEMA common;

-- To view task run history:
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     SCHEDULED_TIME_RANGE_START => DATEADD('day', -30, CURRENT_TIMESTAMP()),
--     TASK_NAME => 'TASK_BRONZE_STATIONS'
-- ))
-- ORDER BY SCHEDULED_TIME DESC;

-- To manually execute the task (for testing):
-- EXECUTE TASK common.task_bronze_stations;
