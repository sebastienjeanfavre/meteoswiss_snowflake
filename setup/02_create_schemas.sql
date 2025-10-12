-- Create MeteoSwiss Database Schemas
-- This script creates all necessary schemas for the MeteoSwiss platform
-- SYSADMIN PHASE - Basic infrastructure setup
USE ROLE SYSADMIN;

-- Ensure we're using the correct database
USE DATABASE METEOSWISS;

-- Create bronze schema for raw data ingestion
CREATE SCHEMA IF NOT EXISTS bronze
    COMMENT = 'Bronze layer - Raw data ingestion from MeteoSwiss API endpoints (measurements and forecasts)';

-- Create silver schema for cleansed, unified, and deduplicated data
CREATE SCHEMA IF NOT EXISTS silver
    COMMENT = 'Silver layer - Cleansed, unified, and deduplicated data';

-- Create gold schema for business-specific aggregations and analytics
CREATE SCHEMA IF NOT EXISTS gold
    COMMENT = 'Gold layer - Business-specific aggregations, analytics, and curated data products';

-- Create common schema for orchestration tasks and shared objects
CREATE SCHEMA IF NOT EXISTS common
    COMMENT = 'Common layer - Orchestration tasks, error handling, and shared objects used across data layers';

-- Create utils schema for utility functions and procedures
CREATE SCHEMA IF NOT EXISTS utils
    COMMENT = 'Utility functions, procedures, and deployment tools for platform management';

-- Verify schema creation
SELECT 'All schemas created successfully' AS result;

-- Show all schemas in the database
SHOW SCHEMAS IN DATABASE METEOSWISS;