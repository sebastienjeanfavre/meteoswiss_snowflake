-- ============================================================================
-- Grant PyPI Repository Access
-- ============================================================================
-- Grants access to Snowflake's PyPI shared repository for using packages
-- from PyPI in Snowpark Python stored procedures and UDFs.
--
-- This must be run by ACCOUNTADMIN role.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Grant PyPI repository access to SYSADMIN role
GRANT DATABASE ROLE SNOWFLAKE.PYPI_REPOSITORY_USER TO ROLE SYSADMIN;

-- Verify the grant
SHOW GRANTS TO ROLE SYSADMIN;
