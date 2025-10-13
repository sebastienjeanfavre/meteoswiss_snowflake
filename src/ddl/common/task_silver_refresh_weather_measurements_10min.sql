-- ============================================================================
-- Scheduled Task: Refresh Silver Layer Measurements
-- ============================================================================
-- This task refreshes the unified weather measurements dynamic table after
-- the bronze layer now data has been updated.
--
-- Trigger: After common.task_bronze_now_data completes successfully
-- Warehouse: METEOSWISS_WH
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA COMMON;

-- Create the task with dependency on bronze task
CREATE OR REPLACE TASK common.task_silver_refresh_weather_measurements_10min
    WAREHOUSE = METEOSWISS_WH
    COMMENT = 'Refreshes the unified weather measurements dynamic table after bronze now data is updated'
    AFTER common.task_bronze_load_weather_measurements_10min_recent
AS
    ALTER DYNAMIC TABLE silver.dt_weather_measurements_10min REFRESH;

-- Task is created in SUSPENDED state by default
-- To enable the task, run:
ALTER TASK common.task_silver_refresh_weather_measurements_10min RESUME;

-- IMPORTANT: The parent task (common.task_bronze_load_weather_measurements_10min_now) must be running
-- for this child task to execute. Resume order:
-- 1. ALTER TASK common.task_bronze_load_weather_measurements_10min_now RESUME;
-- 2. ALTER TASK common.task_silver_refresh_weather_measurements_10min RESUME;

-- To check task status:
-- SHOW TASKS LIKE 'task_silver_refresh_weather_measurements_10min' IN SCHEMA common;

-- To view task run history:
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP()),
--     TASK_NAME => 'TASK_SILVER_REFRESH_WEATHER_MEASUREMENTS_10MIN'
-- ))
-- ORDER BY SCHEDULED_TIME DESC;

-- To manually execute the task (for testing):
-- EXECUTE TASK common.task_silver_refresh_weather_measurements_10min;

-- To check task dependency tree:
-- SELECT
--     name,
--     database_name,
--     schema_name,
--     state,
--     predecessors
-- FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
--     TASK_NAME => 'common.task_bronze_load_weather_measurements_10min_now',
--     RECURSIVE => TRUE
-- ));
