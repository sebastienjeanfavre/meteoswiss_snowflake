-- Create Git Repository in Snowflake for MeteoSwiss Platform
-- This script sets up git integration for version control of database objects
-- ACCOUNTADMIN PHASE - External integrations and security setup
USE ROLE ACCOUNTADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA UTILS;

-- Create git repository for MeteoSwiss platform
-- Replace 'YOUR_GITHUB_USERNAME' and 'YOUR_REPO_URL' with actual values
CREATE OR REPLACE GIT REPOSITORY utils.meteoswiss_repo
  API_INTEGRATION = GITHUB_API_INTEGRATION  -- Assumes you have a git API integration configured
  ORIGIN = 'https://github.com/sebastienjeanfavre/meteoswiss_snowflake.git'
  COMMENT = 'Git repository for MeteoSwiss solar data platform source code';


-- Verify repository creation
SHOW GIT REPOSITORIES IN SCHEMA utils;

-- Fetch latest changes from repository
-- ALTER GIT REPOSITORY utils.meteoswiss_repo FETCH;

SELECT 'Git repository created successfully in utils schema' AS result;