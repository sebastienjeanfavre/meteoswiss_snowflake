-- Network Rule for MeteoSwiss API
-- This must be created by an ACCOUNTADMIN before using the API functions
-- ACCOUNTADMIN PHASE - External integrations and security setup
USE ROLE ACCOUNTADMIN;
USE DATABASE METEOSWISS;

-- Create network rule for MeteoSwiss API endpoints
CREATE OR REPLACE NETWORK RULE staging.meteoswiss_network_rule
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('data.geo.admin.ch');

-- Verify network rule creation
SHOW NETWORK RULES IN DATABASE;