-- ============================================================================
-- Stored Procedure: List Git Repository Files
-- ============================================================================
-- Returns a table of all files in the MeteoSwiss git repository
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA UTILS;

CREATE OR REPLACE PROCEDURE utils.sp_list_git_files()
RETURNS TABLE (file_path STRING, file_size NUMBER, last_modified TIMESTAMP_LTZ)
LANGUAGE SQL
COMMENT = 'Lists all files in the MeteoSwiss git repository'
AS
$$
BEGIN
    RETURN TABLE(
        SELECT
            file_path,
            size AS file_size,
            last_modified
        FROM TABLE(
            INFORMATION_SCHEMA.GIT_REPOSITORY_FILES(
                REPOSITORY_NAME => 'utils.meteoswiss_repo'
            )
        )
        ORDER BY file_path
    );
END;
$$;

SELECT 'Procedure sp_list_git_files created successfully' AS result;
