# MeteoSwiss Setup Scripts

This directory contains infrastructure setup scripts for the MeteoSwiss Snowflake platform. Scripts are organized by required role permissions and must be executed in numerical order.

## Setup Scripts (01-07)

### Phase 1: Database Infrastructure (SYSADMIN)
Basic database infrastructure that can be created by SYSADMIN role:

1. **`01_create_database.sql`** - Create METEOSWISS database
2. **`02_create_schemas.sql`** - Create bronze, silver, gold, and common schemas

### Phase 2: External Integrations (ACCOUNTADMIN)
Advanced features requiring ACCOUNTADMIN privileges:

3. **`03_create_warehouse.sql`** - Create METEOSWISS_WH warehouse
4. **`04_create_nwr.sql`** - Network rule for MeteoSwiss API access (allows data.geo.admin.ch)
5. **`05_create_eai.sql`** - External access integration for API calls from stored procedures

### Phase 3: Data Infrastructure (SYSADMIN)
Setup for weather data loading:

6. **`06_load_historical_data.sql`** - Historical weather measurements setup (legacy Python script method)
7. **`07_grant_pypi_access.sql`** - Grant access to Snowflake PyPI repository (ACCOUNTADMIN)

## Execution Order

**Run scripts in numerical order (01-07)** - dependencies are structured so each script builds on the previous ones.

**Role switching:**
- Scripts 01-02: SYSADMIN role
- Scripts 03-05: ACCOUNTADMIN role (external integrations)
- Script 06: SYSADMIN role
- Script 07: ACCOUNTADMIN role (PyPI access)

## Quick Start

```sql
-- Phase 1: Database Infrastructure (SYSADMIN)
USE ROLE SYSADMIN;
-- 01_create_database.sql
-- 02_create_schemas.sql

-- Phase 2: External Integrations (ACCOUNTADMIN)
USE ROLE ACCOUNTADMIN;
-- 03_create_warehouse.sql
-- 04_create_nwr.sql
-- 05_create_eai.sql

-- Phase 3: Data Infrastructure
USE ROLE SYSADMIN;
-- 06_load_historical_data.sql

USE ROLE ACCOUNTADMIN;
-- 07_grant_pypi_access.sql
```

## Post-Setup Deployment

After running setup scripts, deploy the application objects from `src/` directory:

1. **Bronze layer**: Deploy stages, file formats, tables, and stored procedures from `src/bronze/`
2. **Silver layer**: Deploy dynamic tables and views from `src/silver/`
3. **Gold layer**: Deploy secure views from `src/gold/`
4. **Common layer**: Deploy and resume tasks from `src/common/`

See `CLAUDE.md` for detailed deployment instructions and workflow documentation.
