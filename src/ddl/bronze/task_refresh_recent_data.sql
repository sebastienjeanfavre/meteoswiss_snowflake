-- ============================================================================
-- Scheduled Task: Refresh Recent Weather Data
-- ============================================================================
-- This task automates the daily refresh of recent weather data by calling
-- the sp_fetch_and_load_recent_data stored procedure.
--
-- Schedule: Daily at 13:00 UTC (1 hour after MeteoSwiss updates at 12:00 UTC)
-- Warehouse: METEOSWISS_WH
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

-- Create the task
CREATE OR REPLACE TASK bronze.task_refresh_recent_data
    WAREHOUSE = METEOSWISS_WH
    SCHEDULE = 'USING CRON 0 13 * * * UTC'
    COMMENT = 'Daily task to fetch and load recent weather data from MeteoSwiss STAC API'
AS
    CALL bronze.sp_fetch_and_load_recent_data();

-- Task is created in SUSPENDED state by default
-- To enable the task, run:
-- ALTER TASK bronze.task_refresh_recent_data RESUME;

-- To check task status:
-- SHOW TASKS LIKE 'task_refresh_recent_data' IN SCHEMA bronze;

-- To view task run history:
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
--     TASK_NAME => 'TASK_REFRESH_RECENT_DATA'
-- ))
-- ORDER BY SCHEDULED_TIME DESC;

-- To manually execute the task (for testing):
-- EXECUTE TASK bronze.task_refresh_recent_data;
