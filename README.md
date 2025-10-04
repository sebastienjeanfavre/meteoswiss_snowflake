# MeteoSwiss Solar Data Platform

A Snowflake-based data warehouse for comprehensive solar measurement analytics from MeteoSwiss stations across Switzerland.

## Overview

This project implements a dimensional data model to analyze solar radiation and sunshine duration measurements from MeteoSwiss weather stations. The platform enables analysis of solar energy patterns, station performance comparisons, and temporal trends across Switzerland.

## Architecture

### Data Flow
```
MeteoSwiss API → Staging Layer
```

- **Staging**: Raw data ingestion and processing from MeteoSwiss JSON endpoints

### Database Schema

**Database Schemas:**
- `staging` - Raw data ingestion from MeteoSwiss API endpoints
- `utils` - Utility functions, procedures, and deployment tools

**Staging Tables:**
- `stations` - Weather station metadata and locations
- `solar_measurements_raw` - Raw JSON data from MeteoSwiss API

## Project Structure

```
meteoswiss_snowflake/
├── src/                    # Core database objects
│   ├── ddl/               # Data Definition Language
│   │   ├── staging/       # Raw data tables and procedures
│   │   └── utils/         # Utility functions and deployment tools
│   ├── dml/               # Data Manipulation Language (legacy)
│   │   └── staging/       # Staging data processes
│   └── tests/             # Test scripts and sample data
├── setup/                 # Configuration and deployment
│   ├── integration/       # Snowflake integration setup
│   ├── ci/                # CI/CD pipeline
│   └── reset/             # Database reset scripts
└── logs/                  # Pipeline execution logs
```

## Key Analytics Capabilities

### Solar Measurement Analysis
- **Radiation Patterns**: Global solar radiation analysis by station and time
- **Sunshine Duration**: Daily and hourly sunshine measurements
- **Seasonal Trends**: Solar energy patterns across seasons
- **Geographic Analysis**: Solar radiation variations across Swiss regions

### Station Performance
- **Coverage Analysis**: Data availability by station
- **Quality Metrics**: Measurement consistency and reliability
- **Comparative Analysis**: Station-to-station solar radiation comparison

## Data Sources

The platform processes MeteoSwiss real-time data including:
- Global radiation measurements (10-minute intervals)
- Sunshine duration data (10-minute intervals)
- Station metadata and geographic coordinates

### API Endpoints
- **Global Radiation**: https://data.geo.admin.ch/ch.meteoschweiz.messwerte-globalstrahlung-10min/ch.meteoschweiz.messwerte-globalstrahlung-10min_en.json
- **Sunshine Duration**: https://data.geo.admin.ch/ch.meteoschweiz.messwerte-sonnenscheindauer-10min/ch.meteoschweiz.messwerte-sonnenscheindauer-10min_en.json

## Key Features

- **Automated Refresh**: Staging data refreshes every 6 hours
- **Real-time Data**: Live solar measurement processing from MeteoSwiss API
- **Raw Data Storage**: Complete JSON payload preservation for flexibility
- **Geographic Coverage**: Complete Swiss weather station network
- **Quality Assurance**: Data validation and API response tracking

## Technology Stack

- **Database**: Snowflake Cloud Data Platform
- **Version Control**: Git
- **Data Source**: MeteoSwiss Open Data API
- **Orchestration**: Snowflake Tasks and Stored Procedures
- **Analytics**: SQL-based analysis on staged data

---

For questions about MeteoSwiss solar data or analytics requirements, refer to the test scripts in the `src/tests/` directory for usage examples and patterns.