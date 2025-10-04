# MeteoSwiss Setup Phase

This directory contains all infrastructure setup scripts organized in two phases based on required Snowflake role permissions.

## Setup Phases

### SYSADMIN Phase - Basic Infrastructure
Basic database infrastructure that can be created by SYSADMIN role:

1. `01_create_database.sql` - Create METEOSWISS database
2. `02_create_schemas.sql` - Create staging and utils schemas
3. `03_create_warehouse.sql` - Create METEOSWISS_WH warehouse (requires ACCOUNTADMIN)

### ACCOUNTADMIN Phase - External Integrations
Advanced features requiring ACCOUNTADMIN privileges:

4. `04_create_nwr.sql` - Network rule for MeteoSwiss API access
5. `05_create_eai.sql` - External access integration for API calls
6. `06_create_git_repository.sql` - Git repository for version control

## Execution Order

**Run scripts in numerical order** - dependencies are structured so each script builds on the previous ones.

**Role switching:**
- Scripts 1-2: Can use SYSADMIN
- Script 3: Requires ACCOUNTADMIN (warehouse creation)
- Scripts 4-6: Require ACCOUNTADMIN (external integrations)

## Integration Setup

The `integration/` subdirectory contains additional setup files:
- Git API integration configuration
- Authentication secrets setup

## Quick Start

For a complete setup, run all scripts 01-06 in order as ACCOUNTADMIN:

```sql
-- Run each script in sequence
-- 01_create_database.sql
-- 02_create_schemas.sql
-- 03_create_warehouse.sql
-- 04_create_nwr.sql
-- 05_create_eai.sql
-- 06_create_git_repository.sql
```