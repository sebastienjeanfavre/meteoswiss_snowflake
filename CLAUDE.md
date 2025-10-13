# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MeteoSwiss Weather Data Platform - A Snowflake-based data ingestion and bronze platform for weather measurements: 10-minute interval historical observations from MeteoSwiss stations using a three-tier architecture (Historical, Recent, Now).

## Three-Tier Measurement Data Architecture

The measurement platform uses three separate data tiers with different update frequencies:

1. **Historical Tier** (`t_weather_measurements_10min_historical`)
   - Coverage: Measurement start → Dec 31 last year
   - Data files: `*_t_historical_*.csv` (decade files: 1980-1989, 1990-1999, etc.)
   - Update frequency: Yearly (manual backfill)
   - Loading methods:
     - **Option A**: Snowpark stored procedure (recommended) - fully automated
     - **Option B**: Python script + Snowflake CLI upload + COPY INTO (legacy)

2. **Recent Tier** (`t_weather_measurements_10min_recent`)
   - Coverage: Jan 1 current year → Yesterday
   - Data files: `*_t_recent.csv`
   - Update frequency: Daily at 13:00 UTC (automated via Snowpark)
   - Loading method: Snowpark stored procedure with Tasks

3. **Now Tier** (`t_weather_measurements_10min_now`)
   - Coverage: Yesterday 12:00 UTC → Now
   - Data files: `*_t_now.csv`
   - Update frequency: Every 10 minutes (automated via Snowpark)
   - Loading method: Snowpark stored procedure with Tasks

**Critical**:
- Recent/Now tiers use temp table + INSERT OVERWRITE strategy (ensures bronze tables never empty)
- Historical data must be loaded manually via legacy Python script + CLI method
- No incremental merges or upserts - all loads are full replacements

## Data Flow Patterns

### Historical Data (Manual Loading)
**Note**: The automated Snowpark stored procedure for historical data has been removed. Use the legacy Python script method below.
```
MeteoSwiss STAC API → Python Script (fetch_historical_data.py)
→ Local Files (meteoswiss_data/historical/)
→ Snowflake CLI Upload → Internal Stage
→ COPY INTO → Table
```

### Recent/Now Data (Automated)
```
MeteoSwiss STAC API → Snowpark Python Procedure (in-database)
→ Internal Stage (via put_stream)
→ COPY INTO → Table
→ Scheduled via Tasks
```

## Silver Layer (Unified Measurement Data)

The silver layer provides unified, deduplicated access to all measurement data by combining the three bronze/bronze tiers.

### Architecture Pattern
```
Bronze/Staging (3 tables) → Silver (1 dynamic table) → Gold (business metrics)
```

### Unified Dynamic Table

**Table**: `silver.dt_weather_measurements_10min`
- ✅ Fast query performance (pre-computed)
- ✅ Automatic incremental refresh (10-minute lag)
- ✅ Deduplicated using priority logic
- ✅ Production-ready
- ✅ Single access point for all queries

### Deduplication Logic
- **Priority**: Now > Recent > Historical (most current data wins)
- **Overlap**: Recent/Now overlap at yesterday 12:00-23:59 UTC (intentional)
- **Method**: QUALIFY with ROW_NUMBER() over (station, timestamp, tier_priority)
- **Result**: Each (station, timestamp) appears exactly once

### Usage Examples
```sql
-- Query unified measurement data
SELECT * FROM silver.dt_weather_measurements_10min
WHERE station_abbr = 'BAS'
  AND reference_timestamp >= '2024-01-01';

-- Daily aggregations
SELECT
    station_abbr,
    DATE_TRUNC('day', reference_timestamp) as day,
    AVG(tre200s0) as avg_temp,
    SUM(rre150z0) as total_precip
FROM silver.dt_weather_measurements_10min
WHERE reference_timestamp >= DATEADD('month', -6, CURRENT_TIMESTAMP())
GROUP BY station_abbr, day;
```

### Data Quality Monitoring
```sql
-- Check for duplicates (should be 0)
SELECT station_abbr, reference_timestamp, COUNT(*)
FROM silver.dt_weather_measurements_10min
GROUP BY station_abbr, reference_timestamp
HAVING COUNT(*) > 1;

-- Monitor overlaps
SELECT * FROM silver.data_quality_overlaps;

-- Check completeness by station
SELECT * FROM silver.data_quality_completeness;
```

### Dynamic Table Management
```sql
-- Monitor refresh status
SHOW DYNAMIC TABLES LIKE 'dt_weather_measurements_10min';

-- Suspend/resume (cost management)
ALTER DYNAMIC TABLE silver.dt_weather_measurements_10min SUSPEND;
ALTER DYNAMIC TABLE silver.dt_weather_measurements_10min RESUME;

-- Manual refresh if needed
ALTER DYNAMIC TABLE silver.dt_weather_measurements_10min REFRESH;
```

**Documentation**: See `docs/SILVER_LAYER_GUIDE.md` for comprehensive usage guide.

## Commands

### Python Data Fetching (Legacy - Optional)
```bash
# These scripts are legacy methods - Snowpark procedures are now recommended

# Download all historical data files (legacy method)
python scripts/fetch_historical_data.py

# Download recent data (only needed if testing Python script)
python scripts/fetch_recent_data.py

# Download now data (only needed if testing Python script)
python scripts/fetch_now_data.py
```

### Snowflake CLI Commands (Legacy - Optional)
```bash
# These commands are only needed if using legacy Python script method

# Upload historical data to stage (legacy method)
snow stage copy ./meteoswiss_data/historical/ @bronze.stg_meteoswiss_historical --recursive

# List files in stage
snow stage list @bronze.stg_meteoswiss_historical
```

### Snowflake SQL Operations

#### Setup (run in order)
```sql
-- Run setup scripts 01-11 in sequence from setup/ directory
-- Scripts 01-02: SYSADMIN role
-- Scripts 03-06: ACCOUNTADMIN role (warehouse, external integrations)
-- Scripts 07-09: Measurement data infrastructure (bronze layer)
-- Script 11: Silver layer (unified measurement data)
```

#### Task Management
```sql
-- Resume automated tasks (measurement data)
ALTER TASK common.task_bronze_load_weather_measurements_10min_recent RESUME;
ALTER TASK common.task_bronze_load_weather_measurements_10min_now RESUME;
ALTER TASK common.task_bronze_load_weather_stations RESUME;
ALTER TASK common.task_silver_refresh_weather_measurements_10min RESUME;

-- Suspend tasks
ALTER TASK common.task_bronze_load_weather_measurements_10min_recent SUSPEND;
ALTER TASK common.task_bronze_load_weather_measurements_10min_now SUSPEND;
ALTER TASK common.task_bronze_load_weather_stations SUSPEND;
ALTER TASK common.task_silver_refresh_weather_measurements_10min SUSPEND;

-- Check task status
SHOW TASKS IN SCHEMA common;

-- View task execution history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
    TASK_NAME => 'TASK_BRONZE_LOAD_WEATHER_MEASUREMENTS_10MIN_RECENT'
))
ORDER BY SCHEDULED_TIME DESC;

-- Manual task execution (testing)
EXECUTE TASK common.task_bronze_load_weather_measurements_10min_recent;
EXECUTE TASK common.task_bronze_load_weather_measurements_10min_now;
```

#### Stored Procedure Calls
```sql
-- Measurement data refresh procedures (Recent and Now tiers only)
CALL bronze.sp_load_weather_measurements_10min_recent();
CALL bronze.sp_load_weather_measurements_10min_now();

-- Note: Historical data SP has been removed - use legacy Python script + CLI method

-- Execute SQL from Git repository
CALL utils.sp_execute_sql_from_git('src/ddl/bronze/table.sql');
```

#### Data Validation
```sql
-- Check measurement row counts across all tiers
SELECT 'historical' as tier, COUNT(*) as rows FROM bronze.t_weather_measurements_10min_historical
UNION ALL
SELECT 'recent' as tier, COUNT(*) as rows FROM bronze.t_weather_measurements_10min_recent
UNION ALL
SELECT 'now' as tier, COUNT(*) as rows FROM bronze.t_weather_measurements_10min_now;

-- Check COPY INTO history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'WEATHER_MEASUREMENTS_10MIN_RECENT',
    START_TIME => DATEADD(HOURS, -24, CURRENT_TIMESTAMP())
));
```

## Key Technical Details

### API Integration
- **Endpoint**: https://data.geo.admin.ch/api/stac/v1
- **Collection**: ch.meteoschweiz.ogd-smn (Swiss Meteorological Network)
- **Pagination**: Uses cursor-based pagination with 100 items per page
- **Authentication**: None required (Open Government Data)

### Snowflake Objects
- **Database**: METEOSWISS
- **Schemas**:
  - `bronze` - Bronze layer (raw data from MeteoSwiss)
  - `silver` - Silver layer (unified, deduplicated data)
  - `utils` - Utility procedures and functions
- **Warehouse**: METEOSWISS_WH
- **External Access Integration**: meteoswiss_integration (for API access)
- **Network Rule**: meteoswiss_network_rule (allows data.geo.admin.ch)
- **Git Repository**: utils.meteoswiss_repo (version control integration)
- **Stages**: stg_meteoswiss_historical, stg_meteoswiss_recent, stg_meteoswiss_now, stg_meteoswiss_stations
- **File Format**: ff_meteoswiss_csv (semicolon-delimited CSV)

### CSV File Format
- **Delimiter**: Semicolon (;)
- **Encoding**: UTF-8
- **Header**: First row skipped
- **Date Format**: DD.MM.YYYY HH24:MI
- **Null Handling**: Empty strings, 'NULL', '-' treated as NULL

### Measurement Variables (31 columns)
- Temperature: tre200s0, tre005s0, tresurs0, xchills0
- Humidity: ure200s0, tde200s0, pva200s0
- Pressure: prestas0, pp0qnhs0, pp0qffs0, ppz850s0, ppz700s0
- Wind: fkl010z1, fve010z0, fkl010z0, dkl010z0, wcc006s0, fu3010z0, fkl010z3, fu3010z1, fu3010z3
- Precipitation: rre150z0
- Solar: htoauts0, gre000z0, sre000z0
- Snow: ods000z0
- Lysimeter: oli000z0, olo000z0, osr000z0

## Common Workflows

### Adding New Stored Procedures
1. Create SQL file in `src/ddl/bronze/` or `src/ddl/utils/`
2. Use Snowpark Python runtime 3.12 for procedures requiring API access
3. Specify EXTERNAL_ACCESS_INTEGRATIONS = (meteoswiss_integration) for HTTP calls
4. Add procedure comments for documentation
5. Commit to Git repository
6. Deploy via Git integration or direct execution

### Modifying Data Loading Logic

**Measurement Data:**
- **Recent data**: Edit `src/ddl/bronze/sp_load_weather_measurements_10min_recent.sql`
- **Now data**: Edit `src/ddl/bronze/sp_load_weather_measurements_10min_now.sql`
- **Historical data**: No automated SP available - use legacy Python script method
- Recent/Now procedures use temp table + INSERT OVERWRITE pattern to ensure bronze tables never empty:
  1. Create temp table with same structure
  2. COPY INTO temp table from stage
  3. INSERT OVERWRITE from temp to bronze (atomic replacement)
  4. Drop temp table
- All changes require CREATE OR REPLACE PROCEDURE execution

### Debugging Data Loading Issues
1. Check task history for failures
2. Call stored procedure manually to see detailed error stats
3. Review returned VARIANT object: status, files_uploaded, files_failed, errors array
4. Verify External Access Integration allows data.geo.admin.ch
5. Check warehouse is running (METEOSWISS_WH)

## File Organization

- `scripts/` - Python scripts for historical measurement data download (local execution, legacy)
- `src/ddl/bronze/` - Snowpark procedures and tasks for automated data refresh
  - `sp_load_weather_measurements_10min_recent.sql` - Recent measurements (daily refresh)
  - `sp_load_weather_measurements_10min_now.sql` - Real-time measurements (10-minute refresh)
  - `sp_load_weather_stations.sql` - Weather station metadata refresh
- `src/ddl/utils/` - Utility procedures (Git integration, SQL execution)
- `setup/` - Infrastructure setup scripts (run once, in order)
  - Scripts 01-06: Core infrastructure (database, schemas, external integrations)
  - Scripts 07-09: Measurement data setup (bronze layer)
  - Script 11: Silver layer setup (unified measurement data)
- `docs/` - Documentation and usage guides
  - `SILVER_LAYER_GUIDE.md` - Comprehensive silver layer usage guide
- `meteoswiss_data/` - Downloaded CSV files (gitignored, local only)

## Important Constraints

**Measurement Data:**
- Historical data must be loaded manually via legacy Python script + Snowflake CLI method
- Historical data uses multiple decade files per station (1980-1989, 1990-1999, etc.)
- Recent/Now automated procedures use temp table + INSERT OVERWRITE strategy (bronze tables never empty)
- Recent/Now data fully automated via Snowpark with scheduled Tasks (no external dependencies)
- Bronze tables are never empty during refresh - INSERT OVERWRITE ensures atomic data replacement

**General:**
- Tasks are created in SUSPENDED state by default - must RESUME manually
- ACCOUNTADMIN role required for External Access Integration and Network Rules
- Windows environment: Use Git Bash or similar for Unix-style commands
- to memorize