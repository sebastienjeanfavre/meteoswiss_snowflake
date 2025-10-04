-- Git Deployment Procedures for MeteoSwiss Platform
-- Stored procedures to manage deployment from git repository

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA UTILS;

-- Procedure to fetch latest changes from git repository
CREATE OR REPLACE PROCEDURE utils.fetch_git_repository()
RETURNS STRING
LANGUAGE SQL
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

-- Procedure to list files in git repository
CREATE OR REPLACE PROCEDURE utils.list_git_files()
RETURNS TABLE (file_path STRING, file_size NUMBER, last_modified TIMESTAMP_LTZ)
LANGUAGE SQL
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

-- Procedure to execute SQL files from git repository
CREATE OR REPLACE PROCEDURE utils.execute_sql_from_git(file_path STRING)
RETURNS STRING
LANGUAGE SQL
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

SELECT 'Git deployment procedures created successfully' AS result;