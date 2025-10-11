# MeteoSwiss Setup Phase

This directory contains all infrastructure setup scripts organized in two phases based on required Snowflake role permissions.

## Setup Phases

### SYSADMIN Phase - Basic Infrastructure
Basic database infrastructure that can be created by SYSADMIN role:

1. `01_create_database.sql` - Create METEOSWISS database
2. `02_create_schemas.sql` - Create bronze and utils schemas
3. `03_create_warehouse.sql` - Create METEOSWISS_WH warehouse (requires ACCOUNTADMIN)

### ACCOUNTADMIN Phase - External Integrations
Advanced features requiring ACCOUNTADMIN privileges:

4. `04_create_nwr.sql` - Network rule for MeteoSwiss API access
5. `05_create_eai.sql` - External access integration for API calls
6. `06_create_git_repository.sql` - Git repository for version control

### Data Infrastructure Phase
Setup for weather measurement data (bronze and silver layers):

7. `07_load_historical_data.sql` - Historical weather measurements setup
8. `08_load_recent_data.sql` - Recent weather measurements setup
9. `09_load_now_data.sql` - Real-time weather measurements setup
11. `11_create_silver_layer.sql` - Unified measurement data (silver layer)

## Execution Order

**Run scripts in numerical order** - dependencies are structured so each script builds on the previous ones.

**Role switching:**
- Scripts 1-2: Can use SYSADMIN
- Script 3: Requires ACCOUNTADMIN (warehouse creation)
- Scripts 4-6: Require ACCOUNTADMIN (external integrations)
- Scripts 7-9, 11: Can use SYSADMIN (data infrastructure - bronze + silver)

## Integration Setup

The `integration/` subdirectory contains additional setup files:
- Git API integration configuration
- Authentication secrets setup

## Quick Start

For a complete setup, run scripts 01-09 and 11 in order:

```sql
-- Core Infrastructure (ACCOUNTADMIN)
-- 01_create_database.sql
-- 02_create_schemas.sql
-- 03_create_warehouse.sql
-- 04_create_nwr.sql
-- 05_create_eai.sql
-- 06_create_git_repository.sql

-- Data Infrastructure - Bronze Layer (SYSADMIN)
-- 07_load_historical_data.sql
-- 08_load_recent_data.sql
-- 09_load_now_data.sql

-- Data Infrastructure - Silver Layer (SYSADMIN)
-- 11_create_silver_layer.sql
```

After setup, deploy stored procedures and tasks from `src/ddl/bronze/`