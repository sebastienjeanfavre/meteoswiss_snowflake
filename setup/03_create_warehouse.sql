-- Create MeteoSwiss Compute Warehouse
-- This script creates a dedicated warehouse for MeteoSwiss data processing
-- SYSADMIN PHASE - Basic infrastructure setup (requires ACCOUNTADMIN for warehouse creation)
USE ROLE ACCOUNTADMIN;

-- Create warehouse for MeteoSwiss operations
CREATE WAREHOUSE IF NOT EXISTS METEOSWISS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60  -- 1 minute
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Dedicated warehouse for MeteoSwiss solar data processing and analysis';

-- Set as default warehouse for the session
GRANT USAGE ON WAREHOUSE METEOSWISS_WH TO ROLE SYSADMIN;

-- Verify warehouse creation
SELECT 'Warehouse METEOSWISS_WH created successfully' AS result;

-- Show warehouse details
SHOW WAREHOUSES LIKE 'METEOSWISS_WH';