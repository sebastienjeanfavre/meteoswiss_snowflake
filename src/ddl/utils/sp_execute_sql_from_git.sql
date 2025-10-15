-- ============================================================================
-- Stored Procedure: Execute SQL from Git Repository
-- ============================================================================
-- Executes a SQL file from the MeteoSwiss git repository
--
-- Parameters:
--   file_path - Path to SQL file in git repository (e.g., 'src/ddl/bronze/table.sql')
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE METEOSWISS_WH;
USE DATABASE METEOSWISS;
USE SCHEMA UTILS;

CREATE OR REPLACE PROCEDURE utils.sp_execute_sql_from_git(file_path STRING)
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Executes a SQL file from the MeteoSwiss git repository'
AS
$$
DECLARE
    sql_content STRING;
    result_msg STRING;
BEGIN
    -- Get file content from git repository
    SELECT file_content INTO sql_content
    FROM TABLE(
        INFORMATION_SCHEMA.GIT_REPOSITORY_FILES(
            REPOSITORY_NAME => 'utils.meteoswiss_repo'
        )
    )
    WHERE file_path = :file_path;

    IF (sql_content IS NULL) THEN
        RETURN 'File not found: ' || :file_path;
    END IF;

    -- Execute the SQL content
    EXECUTE IMMEDIATE sql_content;

    RETURN 'Successfully executed: ' || :file_path;
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error executing ' || :file_path || ': ' || SQLERRM;
END;
$$;

SELECT 'Procedure sp_execute_sql_from_git created successfully' AS result;
