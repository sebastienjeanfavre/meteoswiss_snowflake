# MeteoSwiss Weather Data Platform

A Snowflake-based data platform for comprehensive weather measurement analytics from MeteoSwiss stations across Switzerland.

## Overview

This project implements a data ingestion and bronze platform to process 10-minute interval weather measurements from MeteoSwiss weather stations. The platform enables analysis of weather patterns including temperature, precipitation, solar radiation, wind, humidity, and atmospheric pressure across Switzerland.

## Architecture

### Data Flow
```
MeteoSwiss STAC API → Python Scripts/Snowpark → Internal Stages → Staging Tables
```

- **Historical**: One-time backfill of all historical data (measurement start to Dec 31 last year)
- **Recent**: Daily automated refresh (Jan 1 current year to yesterday) at 13:00 UTC
- **Now**: Real-time automated refresh (yesterday 12:00 UTC to now) every 10 minutes

### Data Tiers

1. **Historical Data** (`weather_measurements_10min_historical`)
   - Coverage: Measurement start → Dec 31 last year
   - Update: Yearly (manual backfill via Python script)
   - Granularity: 10-minute intervals

2. **Recent Data** (`weather_measurements_10min_recent`)
   - Coverage: Jan 1 current year → Yesterday
   - Update: Daily at 13:00 UTC (automated via Snowpark)
   - Granularity: 10-minute intervals

3. **Now Data** (`weather_measurements_10min_now`)
   - Coverage: Yesterday 12:00 UTC → Now
   - Update: Every 10 minutes (automated via Snowpark)
   - Granularity: 10-minute intervals

### Database Schema

**Database Schemas:**
- `bronze` - Raw data ingestion from MeteoSwiss API endpoints
- `utils` - Utility functions, procedures, and deployment tools

**Staging Tables:**
- `weather_measurements_10min_historical` - Historical weather data (backfill)
- `weather_measurements_10min_recent` - Recent weather data (current year to yesterday)
- `weather_measurements_10min_now` - Real-time weather data (last ~24 hours)

## Project Structure

```
meteoswiss_snowflake/
├── scripts/               # Python data fetching scripts
│   ├── fetch_historical_data.py  # Download all historical data
│   ├── fetch_recent_data.py      # Download recent data
│   └── fetch_now_data.py          # Download now data
├── src/                   # Core database objects
│   └── ddl/              # Data Definition Language
│       ├── bronze/      # Snowpark stored procedures and tasks
│       │   ├── sp_fetch_and_load_recent_data.sql
│       │   ├── sp_fetch_and_load_now_data.sql
│       │   ├── task_refresh_recent_data.sql
│       │   └── task_refresh_now_data.sql
│       └── utils/        # Utility functions
├── setup/                # Setup scripts (run in order 01-09)
│   ├── 01_create_database.sql
│   ├── 02_create_schemas.sql
│   ├── 03_create_warehouse.sql
│   ├── 04_create_nwr.sql
│   ├── 05_create_eai.sql
│   ├── 06_create_git_repository.sql
│   ├── 07_load_historical_data.sql
│   ├── 08_load_recent_data.sql
│   └── 09_load_now_data.sql
└── meteoswiss_data/      # Downloaded CSV files (gitignored)
    ├── historical/
    ├── recent/
    └── now/
```

## Key Measurement Capabilities

### Weather Variables (10-minute intervals)
- **Temperature**: Air temperature (2m, 5cm), surface temperature, wind chill
- **Humidity**: Relative humidity, dew point, water vapor pressure
- **Pressure**: Station level, sea level (QNH, QFF), geopotential heights
- **Wind**: Speed (scalar, vector, gust), direction, cloud cover
- **Precipitation**: Rainfall measurements
- **Solar**: Global radiation, sunshine duration, reflected radiation
- **Snow**: Snow depth measurements
- **Other**: Lysimeter data (infiltration, outflow, storage)

### Data Quality
- **Coverage Analysis**: Data availability by station and time period
- **Audit Trail**: File names and load timestamps tracked
- **Validation**: Duplicate detection and null percentage analysis

## Data Sources

The platform processes MeteoSwiss weather measurements including:
- 30+ weather variables per measurement (10-minute intervals)
- Data from all MeteoSwiss stations across Switzerland
- Three data tiers: historical (backfill), recent (current year), now (real-time)

### API Endpoint
- **MeteoSwiss STAC API**: https://data.geo.admin.ch/api/stac/v1
- **Collection**: ch.meteoschweiz.ogd-smn (Swiss Meteorological Network)

## Key Features

- **Automated Refresh**:
  - Recent data: Daily at 13:00 UTC
  - Now data: Every 10 minutes (real-time)
- **Snowpark Automation**: In-database Python procedures eliminate external orchestration
- **Three-Tier Architecture**: Historical (backfill), Recent (daily), Now (real-time)
- **Complete Coverage**: All MeteoSwiss weather stations
- **High Granularity**: 10-minute measurement intervals
- **Quality Assurance**: Data validation, audit trails, duplicate detection

## Technology Stack

- **Database**: Snowflake Cloud Data Platform
- **Version Control**: Git
- **Data Source**: MeteoSwiss STAC API (Open Government Data)
- **Data Fetching**: Python 3.x (requests library)
- **Automation**: Snowflake Snowpark Python (3.12), Tasks, External Access Integrations
- **Storage**: Snowflake Internal Stages
- **Data Loading**: COPY INTO with semicolon-delimited CSV

## Setup Instructions

1. Run setup scripts in order (01-09) from the `setup/` folder
2. Download historical data: `python scripts/fetch_historical_data.py`
3. Upload historical data using Snowflake CLI (documented in setup/07)
4. Deploy automation: Run stored procedures and tasks from `src/ddl/bronze/`
5. Activate tasks: `ALTER TASK ... RESUME;`

---

For detailed setup instructions, see comments in each setup script. For automation details, see stored procedure files in `src/ddl/bronze/`.