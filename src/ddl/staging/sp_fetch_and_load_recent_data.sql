-- ============================================================================
-- Snowpark Python Stored Procedure: Fetch and Load Recent Data
-- ============================================================================
-- This procedure automates the complete workflow for recent data:
-- 1. Fetches station list from MeteoSwiss STAC API
-- 2. Downloads _t_recent.csv files for all stations via HTTP
-- 3. Uploads files to internal stage using Snowpark FileOperation
-- 4. Loads data into weather_measurements_10min_recent table
-- 5. Returns comprehensive statistics
--
-- This eliminates the need for external Python scripts and manual file uploads.
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;
USE SCHEMA STAGING;

CREATE OR REPLACE PROCEDURE staging.sp_fetch_and_load_recent_data()
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
from io import BytesIO
from snowflake.snowpark import Session
from snowflake.snowpark.files import SnowflakeFile

# MeteoSwiss STAC API configuration
STAC_API_BASE = "https://data.geo.admin.ch/api/stac/v1"
COLLECTION_ID = "ch.meteoschweiz.ogd-smn"
STAGE_PATH = "@staging.meteoswiss_recent_stage"

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

    try:
        # Step 1: Clean up old staged files
        try:
            session.sql(f"REMOVE {STAGE_PATH}").collect()
        except:
            pass  # Ignore if stage is empty

        # Step 2: Fetch all stations
        stations = fetch_all_stations()
        stats["stations_total"] = len(stations)

        # Step 3: Download and upload files
        for station in stations:
            station_id = station.get('id')
            assets = station.get('assets', {})

            # Find _t_recent.csv file
            recent_file = None
            for name, asset in assets.items():
                if name.endswith('_t_recent.csv'):
                    recent_file = (name, asset.get('href'))
                    break

            if not recent_file:
                continue

            filename, url = recent_file

            # Download and upload
            if download_and_upload_file(session, station_id, url, filename):
                stats["files_uploaded"] += 1
                stats["stations_processed"] += 1
            else:
                stats["files_failed"] += 1
                stats["errors"].append(f"Failed to upload {filename} for station {station_id}")

        # Step 4: Truncate table
        session.sql("TRUNCATE TABLE staging.weather_measurements_10min_recent").collect()

        # Step 5: Load data using COPY INTO
        copy_sql = """
        COPY INTO staging.weather_measurements_10min_recent
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
                CURRENT_TIMESTAMP() as loaded_at
            FROM @staging.meteoswiss_recent_stage
        )
        PATTERN = '.*_t_recent\\.csv'
        ON_ERROR = CONTINUE
        FORCE = TRUE
        """

        copy_result = session.sql(copy_sql).collect()

        # Get row count
        row_count = session.sql(
            "SELECT COUNT(*) FROM staging.weather_measurements_10min_recent"
        ).collect()[0][0]

        stats["rows_loaded"] = row_count
        stats["end_time"] = str(session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0])
        stats["status"] = "SUCCESS"

    except Exception as e:
        stats["status"] = "FAILED"
        stats["errors"].append(str(e))
        stats["end_time"] = str(session.sql("SELECT CURRENT_TIMESTAMP()").collect()[0][0])

    return stats
$$;

COMMENT ON PROCEDURE staging.sp_fetch_and_load_recent_data() IS
    'Automated procedure to fetch recent weather data from MeteoSwiss STAC API, upload to stage, and load into table. Runs daily.';
