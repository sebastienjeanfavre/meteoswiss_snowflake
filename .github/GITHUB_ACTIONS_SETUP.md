# GitHub Actions Setup for ICON Forecast Automation

This guide explains how to set up automated ICON-CH1 forecast data ingestion using GitHub Actions.

## Overview

The workflow automatically:
1. Fetches ICON-CH1 forecast data from MeteoSwiss OGD API every 3 hours
2. Generates two CSV files (grid reference + forecast data)
3. Uploads files to Snowflake stage `@bronze.stg_icon_forecasts` using Python Snowflake connector

**Schedule**: Every 3 hours (matches ICON-CH1 update frequency)
**Cost**: Free (within GitHub Actions limits)
**Infrastructure**: None required - runs in GitHub's cloud
**Upload method**: Python `snowflake-connector-python` (more reliable than CLI)

## Prerequisites

1. ✅ Your code must be in a GitHub repository (public or private)
2. ✅ Snowflake account with credentials
3. ✅ Stage `@bronze.stg_icon_forecasts` must exist in Snowflake

## Setup Instructions

### Step 1: Add GitHub Secrets

Go to your GitHub repository → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Add the following secrets:

| Secret Name | Example Value | Description |
|-------------|---------------|-------------|
| `SNOWFLAKE_ACCOUNT` | `abc12345.us-east-1` | Your Snowflake account identifier |
| `SNOWFLAKE_USER` | `your_username` | Snowflake username |
| `SNOWFLAKE_PASSWORD` | `your_password` | Snowflake password (keep secure!) |
| `SNOWFLAKE_ROLE` | `SYSADMIN` | Role to use |
| `SNOWFLAKE_WAREHOUSE` | `METEOSWISS_WH` | Warehouse for operations |
| `SNOWFLAKE_DATABASE` | `METEOSWISS` | Database name |

**Important**: Never commit credentials to Git! Always use GitHub Secrets.

### Step 2: Enable GitHub Actions

1. Go to your repository → **Actions** tab
2. If prompted, click **"I understand my workflows, go ahead and enable them"**
3. Find the workflow: **"Fetch ICON-CH1 Forecast Data"**

### Step 3: Test the Workflow

**Option A: Manual Test**
1. Go to **Actions** → **Fetch ICON-CH1 Forecast Data**
2. Click **"Run workflow"** → **"Run workflow"**
3. Watch the progress in real-time

**Option B: Wait for Schedule**
- The workflow runs automatically every 3 hours
- First run will be at: 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00 UTC

### Step 4: Verify Success

After the workflow runs:

```sql
-- Check files in Snowflake stage
LIST @bronze.stg_icon_forecasts;

-- Expected files:
-- icon_ch1_grid.csv
-- icon_ch1_forecast_aswdir_s.csv
```

## Troubleshooting

### Workflow fails with "Authentication failed" or "Upload to Snowflake stage" step
- Check that all Snowflake secrets are set correctly in GitHub
- Verify your Snowflake account identifier format (e.g., `abc12345.us-east-1` or `orgname-accountname`)
- Ensure the user has access to the database/warehouse
- Verify user has USAGE privilege on the stage: `GRANT USAGE ON STAGE bronze.stg_icon_forecasts TO ROLE SYSADMIN;`
- Check user has WRITE permission on the stage: `GRANT WRITE ON STAGE bronze.stg_icon_forecasts TO ROLE SYSADMIN;`

### Workflow fails at "Install system dependencies"
- This is rare - GitHub's Ubuntu runners should have the required packages
- Check the workflow logs for specific error messages
- Verify ecCodes installation completed: look for "ecCodes version:" in logs

### Workflow fails at "Fetch ICON-CH1 forecast data"
- MeteoSwiss API might be temporarily unavailable (try again later)
- Check the Python script for errors in the workflow logs
- Verify the variable name is correct (default: ASWDIR_S)
- Network connectivity issues (rare with GitHub runners)

### Files not appearing in Snowflake stage
- Verify the stage exists: `SHOW STAGES LIKE 'stg_icon_forecasts' IN SCHEMA bronze;`
- Check upload step logs for errors
- Run: `LIST @bronze.stg_icon_forecasts;` to see if files are there
- Verify file sizes are reasonable (grid ~50MB, forecast ~150MB)

## Workflow Details

### Schedule
```yaml
schedule:
  - cron: '0 */3 * * *'  # Every 3 hours at minute 0
```

This matches ICON-CH1 model update frequency (every 3 hours).

### Manual Triggers
You can manually trigger the workflow anytime via the GitHub Actions UI.

### Resource Usage
- **Runtime**: ~5-10 minutes per run
- **GitHub Actions minutes used**: ~10 minutes per run
- **Monthly usage**: ~240 minutes (8 runs/day × 30 days)
- **Cost**: Free (well within GitHub's 2000 min/month limit for private repos)

## Monitoring

### View Workflow History
- Go to **Actions** tab in your repository
- See all past runs, success/failure status
- Download logs for debugging

### Email Notifications
GitHub automatically sends email notifications on workflow failures if:
- The workflow has been failing consistently
- You are the repository owner or have notifications enabled

### Set up Slack/Discord Notifications (Optional)
You can add notification steps to the workflow to send alerts to Slack or Discord channels.

## Cost Analysis

### GitHub Actions (Free Tier)
- **Private repos**: 2000 minutes/month free
- **Public repos**: Unlimited
- **This workflow**: ~240 minutes/month
- **Conclusion**: ✅ Stays within free tier

### Snowflake Costs
- Minimal - only charged for:
  - Stage storage (negligible - CSV files are small)
  - Warehouse compute when running COPY INTO (not part of this workflow)

## Next Steps

After files are uploaded to the stage, you'll need to:

1. Create tables for the data
2. Set up COPY INTO commands to load CSVs into tables
3. (Optional) Create a Snowflake Task to automatically load new files

Would you like help with any of these next steps?

## Alternative: Snowpark Container Services

If GitHub Actions doesn't meet your needs, consider **Snowpark Container Services** (native to Snowflake):
- Pros: Everything runs within Snowflake
- Cons: Currently in preview, more complex setup
- Check availability: `SHOW ORGANIZATION ACCOUNTS;`
