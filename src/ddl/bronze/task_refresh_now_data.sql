-- ============================================================================
-- Scheduled Task: Refresh Realtime Now Weather Data
-- ============================================================================
-- This task automates the frequent refresh of realtime weather data by calling
-- the sp_fetch_and_load_now_data stored procedure.
--
-- Schedule: Every 10 minutes (synchronized with MeteoSwiss update cycle)
-- Warehouse: METEOSWISS_WH
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

-- Create the task
CREATE OR REPLACE TASK bronze.task_refresh_now_data
    WAREHOUSE = METEOSWISS_WH
    SCHEDULE = 'USING CRON */10 * * * * UTC'
    COMMENT = 'Runs every 10 minutes to fetch and load realtime weather data from MeteoSwiss STAC API (synchronized with MeteoSwiss update frequency)'
AS
    CALL bronze.sp_fetch_and_load_now_data();

-- Task is created in SUSPENDED state by default
-- To enable the task, run:
-- ALTER TASK bronze.task_refresh_now_data RESUME;

-- To check task status:
-- SHOW TASKS LIKE 'task_refresh_now_data' IN SCHEMA bronze;

-- To view task run history:
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP()),
--     TASK_NAME => 'TASK_REFRESH_NOW_DATA'
-- ))
-- ORDER BY SCHEDULED_TIME DESC;

-- To manually execute the task (for testing):
-- EXECUTE TASK bronze.task_refresh_now_data;
