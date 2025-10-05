-- ============================================================================
-- Stored Procedure: Fetch Git Repository
-- ============================================================================
-- Fetches the latest changes from the MeteoSwiss git repository
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA UTILS;

CREATE OR REPLACE PROCEDURE utils.sp_fetch_git_repository()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Fetches latest changes from the MeteoSwiss git repository'
AS
$$
BEGIN
    -- Fetch latest changes from the repository
    ALTER GIT REPOSITORY utils.meteoswiss_repo FETCH;

    RETURN 'Git repository fetched successfully';
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error fetching git repository: ' || SQLERRM;
END;
$$;

SELECT 'Procedure sp_fetch_git_repository created successfully' AS result;
