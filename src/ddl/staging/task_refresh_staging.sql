-- Create task to refresh staging data from MeteoSwiss API
-- Runs every 6 hours to align with MeteoSwiss data update frequency

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA STAGING;

CREATE OR REPLACE TASK staging.task_refresh_staging
WAREHOUSE = METEOSWISS_WH
SCHEDULE = 'USING CRON 0 */6 * * * UTC'  -- Every 6 hours
ERROR_ON_NONDETERMINISTIC_MERGE = FALSE
AS
CALL staging.sp_refresh_staging();

-- Enable the task (commented out - enable manually after deployment)
-- ALTER TASK staging.task_refresh_staging RESUME;

-- Task management commands (for reference):
-- To pause: ALTER TASK staging.task_refresh_staging SUSPEND;
-- To resume: ALTER TASK staging.task_refresh_staging RESUME;
-- To check status: SHOW TASKS LIKE 'task_refresh_staging';