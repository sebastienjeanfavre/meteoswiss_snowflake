-- Create MeteoSwiss Database
-- This script creates the main database for MeteoSwiss solar data analytics
-- SYSADMIN PHASE - Basic infrastructure setup
USE ROLE SYSADMIN;

-- Create database
CREATE DATABASE IF NOT EXISTS METEOSWISS
    COMMENT = 'MeteoSwiss Data Analytics Platform';

-- Use the database
USE DATABASE METEOSWISS;

-- Verify database creation
SELECT 'Database METEOSWISS created successfully' AS result;

-- Show database details
SHOW DATABASES LIKE 'METEOSWISS';