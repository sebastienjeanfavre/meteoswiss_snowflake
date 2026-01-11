-- Create Git Repository in Snowflake for MeteoSwiss Platform
-- This script sets up git integration for version control of database objects
-- ACCOUNTADMIN PHASE - External integrations and security setup
--
-- ⚠️ PREREQUISITE: GitHub API Integration Required
-- Before running this script, you must create a GitHub API integration:
--
-- CREATE OR REPLACE API INTEGRATION GITHUB_API_INTEGRATION
--   API_PROVIDER = GIT_HTTPS_API
--   API_ALLOWED_PREFIXES = ('https://github.com/sebastienjeanfavre/')
--   ENABLED = TRUE;
--
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA UTILS;

-- Create git repository for MeteoSwiss platform
-- This assumes you have created the GITHUB_API_INTEGRATION above
CREATE OR REPLACE GIT REPOSITORY utils.meteoswiss_repo
  API_INTEGRATION = GITHUB_API_INTEGRATION
  ORIGIN = 'https://github.com/sebastienjeanfavre/meteoswiss_snowflake.git'
  COMMENT = 'Git repository for MeteoSwiss weather data platform source code';


-- Verify repository creation
SHOW GIT REPOSITORIES IN SCHEMA utils;

-- Fetch latest changes from repository
-- ALTER GIT REPOSITORY utils.meteoswiss_repo FETCH;

SELECT 'Git repository created successfully in utils schema' AS result;