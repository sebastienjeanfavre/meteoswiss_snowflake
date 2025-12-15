#!/usr/bin/env python3
"""
Upload ICON-CH1 forecast CSV files to Snowflake stage
Uses Snowflake Python connector for reliable uploads
"""

import sys
import os
from pathlib import Path
import snowflake.connector

def upload_files_to_stage():
    """Upload CSV files to Snowflake internal stage"""

    # Get credentials from environment variables
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    user = os.getenv('SNOWFLAKE_USER')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    role = os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'METEOSWISS_WH')
    database = os.getenv('SNOWFLAKE_DATABASE', 'METEOSWISS')
    schema = os.getenv('SNOWFLAKE_SCHEMA', 'BRONZE')

    # Validate required parameters
    if not all([account, user, password]):
        print("ERROR: Missing required Snowflake credentials")
        print("Required environment variables: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD")
        sys.exit(1)

    # Files to upload
    grid_file = Path("meteoswiss_data/icon_ch1_grid.csv")
    forecast_file = Path("meteoswiss_data/icon_ch1_forecast_aswdir_s.csv")

    # Verify files exist
    if not grid_file.exists():
        print(f"ERROR: Grid file not found: {grid_file}")
        sys.exit(1)

    if not forecast_file.exists():
        print(f"ERROR: Forecast file not found: {forecast_file}")
        sys.exit(1)

    print("=" * 60)
    print("Uploading ICON-CH1 Forecast Data to Snowflake")
    print("=" * 60)
    print(f"Account: {account}")
    print(f"User: {user}")
    print(f"Database: {database}")
    print(f"Schema: {schema}")
    print(f"Warehouse: {warehouse}")
    print()

    try:
        # Connect to Snowflake
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema
        )

        cursor = conn.cursor()
        print("✓ Connected successfully\n")

        # Upload grid reference file
        print(f"Uploading {grid_file.name}...")
        grid_put_sql = f"""
        PUT file://{grid_file.absolute()} @bronze.stg_icon_ch1
        AUTO_COMPRESS = FALSE
        OVERWRITE = TRUE
        """
        cursor.execute(grid_put_sql)
        result = cursor.fetchone()
        print(f"✓ Grid file uploaded: {result}")

        # Upload forecast data file
        print(f"\nUploading {forecast_file.name}...")
        forecast_put_sql = f"""
        PUT file://{forecast_file.absolute()} @bronze.stg_icon_ch1
        AUTO_COMPRESS = FALSE
        OVERWRITE = TRUE
        """
        cursor.execute(forecast_put_sql)
        result = cursor.fetchone()
        print(f"✓ Forecast file uploaded: {result}")

        # List files in stage to verify
        print("\nVerifying uploads - files in stage:")
        cursor.execute("LIST @bronze.stg_icon_ch1")
        files = cursor.fetchall()
        for file_info in files:
            print(f"  - {file_info[0]}")

        cursor.close()
        conn.close()

        print("\n" + "=" * 60)
        print("✓ Upload completed successfully!")
        print("=" * 60)

        return 0

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(upload_files_to_stage())
