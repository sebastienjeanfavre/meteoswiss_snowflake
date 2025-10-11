-- Create MeteoSwiss Database Schemas
-- This script creates all necessary schemas for the MeteoSwiss platform
-- SYSADMIN PHASE - Basic infrastructure setup
USE ROLE SYSADMIN;

-- Ensure we're using the correct database
USE DATABASE METEOSWISS;

-- Create bronze schema for raw data ingestion
CREATE SCHEMA IF NOT EXISTS bronze
    COMMENT = 'Bronze layer - Raw data ingestion from MeteoSwiss API endpoints (measurements and forecasts)';

-- Create utils schema for utility functions and procedures
CREATE SCHEMA IF NOT EXISTS utils
    COMMENT = 'Utility functions, procedures, and deployment tools for platform management';

-- Verify schema creation
SELECT 'All schemas created successfully' AS result;

-- Show all schemas in the database
SHOW SCHEMAS IN DATABASE METEOSWISS;