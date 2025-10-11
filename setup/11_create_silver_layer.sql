-- ============================================================================
-- Silver Layer Setup - Unified Weather Measurements
-- ============================================================================
-- Creates a unified, deduplicated view of all weather measurements
-- combining historical, recent, and now data tiers.
--
-- Architecture:
-- Bronze/Staging → Silver → Gold
-- - Bronze: Raw data (historical/recent/now tables)
-- - Silver: Cleansed, unified, deduplicated
-- - Gold: Business-specific aggregations
--
-- Prerequisites:
-- - Run scripts 01-09 first (bronze layer setup)
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE METEOSWISS;

-- ============================================================================
-- 1. CREATE SILVER SCHEMA
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS silver
    COMMENT = 'Silver layer - Cleansed, unified, and deduplicated data';

USE SCHEMA silver;

-- ============================================================================
-- 2. CREATE UNIFIED DYNAMIC TABLE (PRODUCTION-READY)
-- ============================================================================
-- Continuously refreshed materialized table for high-performance queries
-- Target lag: 10 minutes (aligned with now data refresh)
-- Combines historical, recent, and now tiers with automatic deduplication
-- Priority: Now > Recent > Historical (most recent data wins)

CREATE OR REPLACE DYNAMIC TABLE silver.weather_measurements_10min
    TARGET_LAG = '10 minutes'
    WAREHOUSE = METEOSWISS_WH
    AS
WITH all_measurements AS (
    -- Historical data
    SELECT
        station_abbr,
        reference_timestamp,
        tre200s0, tre005s0, tresurs0, xchills0,
        ure200s0, tde200s0, pva200s0,
        prestas0, pp0qnhs0, pp0qffs0, ppz850s0, ppz700s0,
        fkl010z1, fve010z0, fkl010z0, dkl010z0, wcc006s0,
        fu3010z0, fkl010z3, fu3010z1, fu3010z3,
        rre150z0, htoauts0, gre000z0, ods000z0,
        oli000z0, olo000z0, osr000z0, sre000z0,
        file_name,
        loaded_at,
        'historical' AS data_tier,
        3 AS tier_priority
    FROM bronze.weather_measurements_10min_historical

    UNION ALL

    -- Recent data
    SELECT
        station_abbr,
        reference_timestamp,
        tre200s0, tre005s0, tresurs0, xchills0,
        ure200s0, tde200s0, pva200s0,
        prestas0, pp0qnhs0, pp0qffs0, ppz850s0, ppz700s0,
        fkl010z1, fve010z0, fkl010z0, dkl010z0, wcc006s0,
        fu3010z0, fkl010z3, fu3010z1, fu3010z3,
        rre150z0, htoauts0, gre000z0, ods000z0,
        oli000z0, olo000z0, osr000z0, sre000z0,
        file_name,
        loaded_at,
        'recent' AS data_tier,
        2 AS tier_priority
    FROM bronze.weather_measurements_10min_recent

    UNION ALL

    -- Now data
    SELECT
        station_abbr,
        reference_timestamp,
        tre200s0, tre005s0, tresurs0, xchills0,
        ure200s0, tde200s0, pva200s0,
        prestas0, pp0qnhs0, pp0qffs0, ppz850s0, ppz700s0,
        fkl010z1, fve010z0, fkl010z0, dkl010z0, wcc006s0,
        fu3010z0, fkl010z3, fu3010z1, fu3010z3,
        rre150z0, htoauts0, gre000z0, ods000z0,
        oli000z0, olo000z0, osr000z0, sre000z0,
        file_name,
        loaded_at,
        'now' AS data_tier,
        1 AS tier_priority
    FROM bronze.weather_measurements_10min_now
)
SELECT
    station_abbr,
    reference_timestamp,
    tre200s0, tre005s0, tresurs0, xchills0,
    ure200s0, tde200s0, pva200s0,
    prestas0, pp0qnhs0, pp0qffs0, ppz850s0, ppz700s0,
    fkl010z1, fve010z0, fkl010z0, dkl010z0, wcc006s0,
    fu3010z0, fkl010z3, fu3010z1, fu3010z3,
    rre150z0, htoauts0, gre000z0, ods000z0,
    oli000z0, olo000z0, osr000z0, sre000z0,
    file_name,
    loaded_at,
    data_tier
FROM all_measurements
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY station_abbr, reference_timestamp
    ORDER BY tier_priority ASC
) = 1;

COMMENT ON TABLE silver.weather_measurements_10min IS
    'Unified, deduplicated weather measurements (10-minute intervals). Dynamic table automatically refreshes every 10 minutes with incremental updates. Combines historical, recent, and now tiers with priority-based deduplication (Now > Recent > Historical).';

-- ============================================================================
-- 3. OPTIONAL: ADD CLUSTERING FOR LARGE DATASETS
-- ============================================================================
-- Uncomment if table grows to millions of rows and you have time-based queries

-- ALTER TABLE silver.weather_measurements_10min
--     CLUSTER BY (reference_timestamp, station_abbr);

-- Monitor clustering:
-- SELECT SYSTEM$CLUSTERING_INFORMATION('silver.weather_measurements_10min');

-- ============================================================================
-- 4. DATA QUALITY VIEWS
-- ============================================================================

-- View to identify overlapping records (for monitoring)
CREATE OR REPLACE VIEW silver.data_quality_overlaps AS
SELECT
    station_abbr,
    reference_timestamp,
    COUNT(DISTINCT data_tier) as tier_count,
    LISTAGG(DISTINCT data_tier, ', ') as tiers_present
FROM (
    SELECT station_abbr, reference_timestamp, 'historical' as data_tier
    FROM bronze.weather_measurements_10min_historical
    UNION ALL
    SELECT station_abbr, reference_timestamp, 'recent' as data_tier
    FROM bronze.weather_measurements_10min_recent
    UNION ALL
    SELECT station_abbr, reference_timestamp, 'now' as data_tier
    FROM bronze.weather_measurements_10min_now
)
GROUP BY station_abbr, reference_timestamp
HAVING COUNT(DISTINCT data_tier) > 1;

COMMENT ON VIEW silver.data_quality_overlaps IS
    'Monitoring view to identify records that exist in multiple tiers (expected for recent/now overlap)';

-- View for data completeness by station
CREATE OR REPLACE VIEW silver.data_quality_completeness AS
SELECT
    station_abbr,
    COUNT(*) as total_records,
    MIN(reference_timestamp) as earliest_measurement,
    MAX(reference_timestamp) as latest_measurement,
    DATEDIFF('day', MIN(reference_timestamp), MAX(reference_timestamp)) as days_covered,

    -- Data completeness percentages
    ROUND(COUNT(tre200s0) * 100.0 / COUNT(*), 2) as temp_2m_pct,
    ROUND(COUNT(gre000z0) * 100.0 / COUNT(*), 2) as solar_rad_pct,
    ROUND(COUNT(rre150z0) * 100.0 / COUNT(*), 2) as precip_pct,
    ROUND(COUNT(prestas0) * 100.0 / COUNT(*), 2) as pressure_pct,

    -- Which tier provides most recent data
    MAX(data_tier) as latest_data_tier
FROM silver.weather_measurements_10min
GROUP BY station_abbr
ORDER BY station_abbr;

COMMENT ON VIEW silver.data_quality_completeness IS
    'Data quality metrics by station: coverage, completeness percentages, data gaps';

-- ============================================================================
-- 5. VALIDATION QUERIES
-- ============================================================================

-- Count total unified records
-- SELECT COUNT(*) as total_records FROM silver.weather_measurements_10min;

-- Count by data tier (should show which tier contributes most)
-- SELECT
--     data_tier,
--     COUNT(*) as record_count,
--     MIN(reference_timestamp) as earliest,
--     MAX(reference_timestamp) as latest
-- FROM silver.weather_measurements_10min
-- GROUP BY data_tier
-- ORDER BY data_tier;

-- Check for duplicates (should be 0)
-- SELECT
--     station_abbr,
--     reference_timestamp,
--     COUNT(*) as duplicate_count
-- FROM silver.weather_measurements_10min
-- GROUP BY station_abbr, reference_timestamp
-- HAVING COUNT(*) > 1;

-- Check overlap count (recent/now overlap region)
-- SELECT COUNT(*) as overlap_count
-- FROM silver.data_quality_overlaps;

-- Check dynamic table refresh status
-- SHOW DYNAMIC TABLES LIKE 'weather_measurements_10min';

-- View refresh history
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
--     'weather_measurements_10min'
-- ))
-- ORDER BY refresh_start_time DESC;

SELECT 'Silver layer created successfully' AS result;
