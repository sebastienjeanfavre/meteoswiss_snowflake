-- ============================================================================
-- Snowpark Python Stored Procedure: Fetch and Load Station Metadata
-- ============================================================================
-- This procedure automates the complete workflow for station metadata:
-- 1. Downloads ogd-smn_meta_stations.csv from MeteoSwiss STAC API collection
-- 2. Uploads file to internal stage using Snowpark FileOperation
-- 3. Loads data into weather_stations table
-- 4. Returns comprehensive statistics
--
-- This eliminates the need for external Python scripts and manual file uploads.
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

CREATE OR REPLACE PROCEDURE bronze.sp_fetch_and_load_stations()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (meteoswiss_integration)
HANDLER = 'main'
EXECUTE AS CALLER
AS
$$
import requests
import json
import logging
from io import BytesIO
from snowflake.snowpark import Session

# Initialize logger
logger = logging.getLogger("meteoswiss.bronze.station_metadata")

# MeteoSwiss station metadata URL (collection-level asset)
STATIONS_CSV_URL = "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_stations.csv"
STAGE_PATH = "@bronze.meteoswiss_stations_stage"
FILENAME = "ogd-smn_meta_stations.csv"

def download_and_upload_file(session, url, filename):
    """
    Download CSV file and upload to Snowflake stage
    """
    try:
        # Download file
        logger.info(f"Downloading file from {url}")
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        # Create file stream
        file_stream = BytesIO(response.content)

        # Upload to stage using put_stream
        stage_location = f"{STAGE_PATH}/{filename}"
        logger.info(f"Uploading file to stage: {stage_location}")
        session.file.put_stream(
            input_stream=file_stream,
            stage_location=stage_location,
            auto_compress=False,
            overwrite=True
        )

        return True
    except Exception as e:
        logger.error(f"Failed to download or upload file: {str(e)}")
        return False

def main(session: Session) -> dict:
    """
    Main procedure logic
    """
    stats = {
        "start_time": str(session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0]),
        "file_downloaded": False,
        "rows_loaded": 0,
        "errors": []
    }

    logger.info("Starting station metadata refresh procedure", extra={
        "procedure": "sp_fetch_and_load_stations"
    })

    try:
        # Step 1: Clean up old staged files
        logger.info("Cleaning up old staged files")
        try:
            session.sql(f"REMOVE {STAGE_PATH}").collect()
            logger.info("Stage cleanup completed")
        except Exception as e:
            logger.info("Stage was empty or cleanup skipped", extra={"reason": str(e)})

        # Step 2: Download and upload CSV file
        logger.info(f"Downloading station metadata from {STATIONS_CSV_URL}")
        if download_and_upload_file(session, STATIONS_CSV_URL, FILENAME):
            stats["file_downloaded"] = True
            logger.info("Station metadata file downloaded and uploaded successfully")
        else:
            raise Exception("Failed to download or upload station metadata file")

        # Step 3: Create temporary table (same structure as bronze table)
        logger.info("Creating temporary table for staging data")
        session.sql("CREATE OR REPLACE TEMPORARY TABLE bronze.temp_weather_stations LIKE bronze.weather_stations").collect()
        logger.info("Temporary table created successfully")

        # Step 4: Load data into temporary table using COPY INTO
        logger.info("Loading data into temporary table using COPY INTO")
        copy_sql = """
        COPY INTO bronze.temp_weather_stations
        FROM (
            SELECT
                $1::VARCHAR as station_abbr,
                $2::VARCHAR as station_name,
                $3::VARCHAR as station_canton,
                $4::VARCHAR as station_wigos_id,
                $5::VARCHAR as station_type_de,
                $6::VARCHAR as station_type_fr,
                $7::VARCHAR as station_type_it,
                $8::VARCHAR as station_type_en,
                $9::VARCHAR as station_dataowner,
                TRY_TO_DATE($10, 'YYYY-MM-DD') as station_data_since,
                TRY_CAST($11 AS NUMBER(10,2)) as station_height_masl,
                TRY_CAST($12 AS NUMBER(10,2)) as station_height_barometer_masl,
                TRY_CAST($13 AS NUMBER(12,2)) as station_coordinates_lv95_east,
                TRY_CAST($14 AS NUMBER(12,2)) as station_coordinates_lv95_north,
                TRY_CAST($15 AS NUMBER(10,6)) as station_coordinates_wgs84_lat,
                TRY_CAST($16 AS NUMBER(10,6)) as station_coordinates_wgs84_lon,
                $17::VARCHAR as station_exposition_de,
                $18::VARCHAR as station_exposition_fr,
                $19::VARCHAR as station_exposition_it,
                $20::VARCHAR as station_exposition_en,
                $21::VARCHAR as station_url_de,
                $22::VARCHAR as station_url_fr,
                $23::VARCHAR as station_url_it,
                $24::VARCHAR as station_url_en,
                METADATA$FILENAME as file_name,
                CURRENT_TIMESTAMP() as loaded_at
            FROM @bronze.meteoswiss_stations_stage
        )
        PATTERN = '.*meta_stations\\.csv'
        ON_ERROR = CONTINUE
        FORCE = TRUE
        """

        copy_result = session.sql(copy_sql).collect()
        logger.info("COPY INTO temporary table completed successfully")

        # Step 5: Atomic replacement - INSERT OVERWRITE ensures bronze table is never empty
        logger.info("Performing atomic replacement with INSERT OVERWRITE")
        session.sql("INSERT OVERWRITE INTO bronze.weather_stations SELECT * FROM bronze.temp_weather_stations").collect()
        logger.info("INSERT OVERWRITE completed - bronze table updated atomically")

        # Step 6: Clean up temporary table
        logger.info("Cleaning up temporary table")
        session.sql("DROP TABLE IF EXISTS bronze.temp_weather_stations").collect()

        # Get row count
        row_count = session.sql(
            "SELECT COUNT(*) FROM bronze.weather_stations"
        ).collect()[0][0]

        stats["rows_loaded"] = row_count
        stats["end_time"] = str(session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0])
        stats["status"] = "SUCCESS"

        logger.info("Station metadata refresh completed successfully", extra={
            "status": "SUCCESS",
            "file_downloaded": stats["file_downloaded"],
            "rows_loaded": stats["rows_loaded"],
            "start_time": stats["start_time"],
            "end_time": stats["end_time"]
        })

    except Exception as e:
        stats["status"] = "FAILED"
        error_msg = str(e)
        stats["errors"].append(error_msg)
        stats["end_time"] = str(session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0])

        logger.error("Station metadata refresh failed with error", extra={
            "status": "FAILED",
            "error": error_msg,
            "file_downloaded": stats["file_downloaded"],
            "start_time": stats["start_time"],
            "end_time": stats["end_time"]
        })

    return stats
$$;

COMMENT ON PROCEDURE bronze.sp_fetch_and_load_stations() IS
    'Automated procedure to fetch station metadata from MeteoSwiss STAC API collection asset, upload to stage, and load into table. Runs weekly.';
