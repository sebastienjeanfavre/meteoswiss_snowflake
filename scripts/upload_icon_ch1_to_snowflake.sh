#!/bin/bash
# ============================================================================
# Upload ICON-CH1 Forecast Data to Snowflake
# ============================================================================
# This script:
# 1. Runs the Python script to fetch ICON-CH1 forecast data
# 2. Uploads the generated CSV files to Snowflake internal stage
#
# Prerequisites:
# - Python environment with meteodatalab, snowflake-connector-python installed
# - Snowflake credentials in environment variables
# - scripts/fetch_icon_ch1_forecast.py exists
#
# Environment variables required:
# - SNOWFLAKE_ACCOUNT
# - SNOWFLAKE_USER
# - SNOWFLAKE_PASSWORD
# - SNOWFLAKE_ROLE (optional, defaults to SYSADMIN)
# - SNOWFLAKE_WAREHOUSE (optional, defaults to METEOSWISS_WH)
# - SNOWFLAKE_DATABASE (optional, defaults to METEOSWISS)
# ============================================================================

set -e  # Exit on error

echo "============================================================"
echo "ICON-CH1 Forecast Data Upload to Snowflake"
echo "============================================================"

# Check required environment variables
if [ -z "$SNOWFLAKE_ACCOUNT" ] || [ -z "$SNOWFLAKE_USER" ] || [ -z "$SNOWFLAKE_PASSWORD" ]; then
    echo "ERROR: Missing required Snowflake credentials"
    echo "Please set the following environment variables:"
    echo "  - SNOWFLAKE_ACCOUNT"
    echo "  - SNOWFLAKE_USER"
    echo "  - SNOWFLAKE_PASSWORD"
    echo ""
    echo "Optional:"
    echo "  - SNOWFLAKE_ROLE (default: SYSADMIN)"
    echo "  - SNOWFLAKE_WAREHOUSE (default: METEOSWISS_WH)"
    echo "  - SNOWFLAKE_DATABASE (default: METEOSWISS)"
    exit 1
fi

# Step 1: Fetch forecast data using Python
echo ""
echo "Step 1: Fetching ICON-CH1 forecast data from MeteoSwiss API..."
python3 scripts/fetch_icon_ch1_forecast.py

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to fetch forecast data"
    exit 1
fi

# Step 2: Upload CSV files to Snowflake stage using Python connector
echo ""
echo "Step 2: Uploading CSV files to Snowflake stage..."
python3 scripts/upload_to_snowflake.py

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to upload files to Snowflake"
    exit 1
fi

echo ""
echo "============================================================"
echo "✓ Upload completed successfully!"
echo "============================================================"
echo ""
echo "Next steps:"
echo "1. Create tables for the forecast data in Snowflake"
echo "2. Run COPY INTO to load the data into tables"
echo "============================================================"
