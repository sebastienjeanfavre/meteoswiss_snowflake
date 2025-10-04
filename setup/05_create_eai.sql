-- External Access Integration for MeteoSwiss API
-- This must be created by an ACCOUNTADMIN after creating the network rule
-- ACCOUNTADMIN PHASE - External integrations and security setup
USE ROLE ACCOUNTADMIN;

-- Create external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION meteoswiss_integration
ALLOWED_NETWORK_RULES = (staging.meteoswiss_network_rule)
ENABLED = true
COMMENT = 'External access integration for MeteoSwiss API endpoints';

-- Grant usage to appropriate roles
-- GRANT USAGE ON INTEGRATION meteoswiss_integration TO ROLE DATA_ENGINEER;
-- GRANT USAGE ON INTEGRATION meteoswiss_integration TO ROLE DEVELOPER;

-- Verify integration
SHOW EXTERNAL ACCESS INTEGRATIONS;