-- ============================================================================
-- Snowpark Python Stored Procedure: Fetch and Load Realtime Now Data
-- ============================================================================
-- This procedure automates the complete workflow for realtime now data:
-- 1. Fetches station list from MeteoSwiss STAC API
-- 2. Downloads _t_now.csv files for all stations via HTTP
-- 3. Uploads files to internal stage using Snowpark FileOperation
-- 4. Loads data into weather_measurements_10min_now table
-- 5. Returns comprehensive statistics
--
-- This eliminates the need for external Python scripts and manual file uploads.
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA BRONZE;

CREATE OR REPLACE PROCEDURE bronze.sp_load_weather_measurements_10min_now()
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
from snowflake.snowpark.files import SnowflakeFile

# Initialize logger
logger = logging.getLogger("meteoswiss.bronze.now_data")

# MeteoSwiss STAC API configuration
STAC_API_BASE = "https://data.geo.admin.ch/api/stac/v1"
COLLECTION_ID = "ch.meteoschweiz.ogd-smn"
STAGE_PATH = "@bronze.stg_meteoswiss_now"

def fetch_all_stations():
    """
    Fetch all stations from STAC API using pagination
    """
    all_features = []
    url = f"{STAC_API_BASE}/search"

    payload = {
        "collections": [COLLECTION_ID],
        "limit": 100
    }

    while True:
        response = requests.post(url, json=payload, timeout=60)
        response.raise_for_status()
        data = response.json()

        features = data.get('features', [])
        all_features.extend(features)

        # Check for next page
        links = data.get('links', [])
        next_link = next((link for link in links if link.get('rel') == 'next'), None)

        if not next_link:
            break

        cursor = next_link.get('body', {}).get('cursor')
        if cursor:
            payload['cursor'] = cursor
        else:
            break

    return all_features

def download_and_upload_file(session, station_id, url, filename):
    """
    Download CSV file and upload to Snowflake stage
    """
    try:
        # Download file
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        # Create file stream
        file_stream = BytesIO(response.content)

        # Upload to stage using put_stream
        stage_location = f"{STAGE_PATH}/{station_id}/{filename}"
        session.file.put_stream(
            input_stream=file_stream,
            stage_location=stage_location,
            auto_compress=False,
            overwrite=True
        )

        return True
    except Exception as e:
        return False

def main(session: Session) -> dict:
    """
    Main procedure logic
    """
    stats = {
        "start_time": str(session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0]),
        "stations_total": 0,
        "stations_processed": 0,
        "files_uploaded": 0,
        "files_failed": 0,
        "rows_loaded": 0,
        "errors": []
    }

    logger.info("Starting now data refresh procedure", extra={
        "procedure": "sp_load_weather_measurements_10min_now",
        "data_tier": "now"
    })

    try:
        # Step 1: Clean up old staged files
        logger.info("Cleaning up old staged files")
        try:
            session.sql(f"REMOVE {STAGE_PATH}").collect()
            logger.info("Stage cleanup completed")
        except Exception as e:
            logger.info("Stage was empty or cleanup skipped", extra={"reason": str(e)})

        # Step 2: Fetch all stations
        logger.info("Fetching stations from MeteoSwiss STAC API")
        stations = fetch_all_stations()
        stats["stations_total"] = len(stations)
        logger.info(f"Fetched {len(stations)} stations from API", extra={
            "stations_total": len(stations)
        })

        # Step 3: Download and upload files
        logger.info("Starting file download and upload to stage")
        for station in stations:
            station_id = station.get('id')
            assets = station.get('assets', {})

            # Find _t_now.csv file
            now_file = None
            for name, asset in assets.items():
                if name.endswith('_t_now.csv'):
                    now_file = (name, asset.get('href'))
                    break

            if not now_file:
                continue

            filename, url = now_file

            # Download and upload
            if download_and_upload_file(session, station_id, url, filename):
                stats["files_uploaded"] += 1
                stats["stations_processed"] += 1

                # Log progress every 20 files
                if stats["files_uploaded"] % 20 == 0:
                    logger.info(f"Progress: {stats['files_uploaded']} files uploaded", extra={
                        "files_uploaded": stats["files_uploaded"],
                        "stations_processed": stats["stations_processed"]
                    })
            else:
                stats["files_failed"] += 1
                error_msg = f"Failed to upload {filename} for station {station_id}"
                stats["errors"].append(error_msg)
                logger.warning(error_msg, extra={
                    "station_id": station_id,
                    "filename": filename
                })

        logger.info(f"File upload completed: {stats['files_uploaded']} uploaded, {stats['files_failed']} failed", extra={
            "files_uploaded": stats["files_uploaded"],
            "files_failed": stats["files_failed"],
            "stations_processed": stats["stations_processed"]
        })

        # Step 4: Create temporary table (same structure as bronze table)
        logger.info("Creating temporary table for staging data")
        session.sql("CREATE OR REPLACE TEMPORARY TABLE bronze.temp_weather_measurements_10min_now LIKE bronze.t_weather_measurements_10min_now").collect()
        logger.info("Temporary table created successfully")

        # Step 5: Load data into temporary table using COPY INTO
        logger.info("Loading data into temporary table using COPY INTO")
        copy_sql = """
        COPY INTO bronze.temp_weather_measurements_10min_now
        FROM (
            SELECT
                $1::VARCHAR as station_abbr,
                TO_TIMESTAMP_NTZ($2, 'DD.MM.YYYY HH24:MI') as reference_timestamp,
                TRY_CAST($3 AS NUMBER(38,10)) as tre200s0,
                TRY_CAST($4 AS NUMBER(38,10)) as tre005s0,
                TRY_CAST($5 AS NUMBER(38,10)) as tresurs0,
                TRY_CAST($6 AS NUMBER(38,10)) as xchills0,
                TRY_CAST($7 AS NUMBER(38,10)) as ure200s0,
                TRY_CAST($8 AS NUMBER(38,10)) as tde200s0,
                TRY_CAST($9 AS NUMBER(38,10)) as pva200s0,
                TRY_CAST($10 AS NUMBER(38,10)) as prestas0,
                TRY_CAST($11 AS NUMBER(38,10)) as pp0qnhs0,
                TRY_CAST($12 AS NUMBER(38,10)) as pp0qffs0,
                TRY_CAST($13 AS NUMBER(38,10)) as ppz850s0,
                TRY_CAST($14 AS NUMBER(38,10)) as ppz700s0,
                TRY_CAST($15 AS NUMBER(38,10)) as fkl010z1,
                TRY_CAST($16 AS NUMBER(38,10)) as fve010z0,
                TRY_CAST($17 AS NUMBER(38,10)) as fkl010z0,
                TRY_CAST($18 AS NUMBER(38,10)) as dkl010z0,
                TRY_CAST($19 AS NUMBER(38,10)) as wcc006s0,
                TRY_CAST($20 AS NUMBER(38,10)) as fu3010z0,
                TRY_CAST($21 AS NUMBER(38,10)) as fkl010z3,
                TRY_CAST($22 AS NUMBER(38,10)) as fu3010z1,
                TRY_CAST($23 AS NUMBER(38,10)) as fu3010z3,
                TRY_CAST($24 AS NUMBER(38,10)) as rre150z0,
                TRY_CAST($25 AS NUMBER(38,10)) as htoauts0,
                TRY_CAST($26 AS NUMBER(38,10)) as gre000z0,
                TRY_CAST($27 AS NUMBER(38,10)) as ods000z0,
                TRY_CAST($28 AS NUMBER(38,10)) as oli000z0,
                TRY_CAST($29 AS NUMBER(38,10)) as olo000z0,
                TRY_CAST($30 AS NUMBER(38,10)) as osr000z0,
                TRY_CAST($31 AS NUMBER(38,10)) as sre000z0,
                METADATA$FILENAME as file_name,
                SYSDATE() as loaded_at
            FROM @bronze.stg_meteoswiss_now
        )
        PATTERN = '.*_t_now\\.csv'
        ON_ERROR = CONTINUE
        """

        copy_result = session.sql(copy_sql).collect()
        logger.info("COPY INTO temporary table completed successfully")

        # Step 6: Atomic replacement - INSERT OVERWRITE ensures bronze table is never empty
        logger.info("Performing atomic replacement with INSERT OVERWRITE")
        session.sql("INSERT OVERWRITE INTO bronze.t_weather_measurements_10min_now SELECT * FROM bronze.temp_weather_measurements_10min_now").collect()
        logger.info("INSERT OVERWRITE completed - bronze table updated atomically")

        # Step 7: Clean up temporary table
        logger.info("Cleaning up temporary table")
        session.sql("DROP TABLE IF EXISTS bronze.temp_weather_measurements_10min_now").collect()

        # Get row count
        row_count = session.sql(
            "SELECT COUNT(*) FROM bronze.t_weather_measurements_10min_now"
        ).collect()[0][0]

        stats["rows_loaded"] = row_count
        stats["end_time"] = str(session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0])
        stats["status"] = "SUCCESS"

        logger.info("Now data refresh completed successfully", extra={
            "status": "SUCCESS",
            "stations_total": stats["stations_total"],
            "stations_processed": stats["stations_processed"],
            "files_uploaded": stats["files_uploaded"],
            "files_failed": stats["files_failed"],
            "rows_loaded": stats["rows_loaded"],
            "start_time": stats["start_time"],
            "end_time": stats["end_time"]
        })

    except Exception as e:
        stats["status"] = "FAILED"
        error_msg = str(e)
        stats["errors"].append(error_msg)
        stats["end_time"] = str(session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0])

        logger.error("Now data refresh failed with error", extra={
            "status": "FAILED",
            "error": error_msg,
            "stations_total": stats["stations_total"],
            "stations_processed": stats["stations_processed"],
            "files_uploaded": stats["files_uploaded"],
            "files_failed": stats["files_failed"],
            "start_time": stats["start_time"],
            "end_time": stats["end_time"]
        })

    return stats
$$;

COMMENT ON PROCEDURE bronze.sp_load_weather_measurements_10min_now() IS
    'Automated procedure to fetch realtime weather data from MeteoSwiss STAC API, upload to stage, and load into table. Runs every 10 minutes.';
