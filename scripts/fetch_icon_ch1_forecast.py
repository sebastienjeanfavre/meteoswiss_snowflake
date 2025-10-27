from meteodatalab import ogd_api
from pathlib import Path
import xarray as xr
from datetime import timedelta
from earthkit.data import settings
import pandas as pd
import numpy as np

# Enable earthkit-data caching for faster repeated access
# Options: "temporary" (session-only), "user" (persistent), "off" (no cache)
settings.set("cache-policy", "user")
print("✓ Earthkit-data caching enabled (persistent user cache)\n")

# ICON-CH1-EPS: High resolution (1 km), short range (33h), hourly output
# Grid resolution: ~1 km
# Forecast horizon: 33 hours
# Temporal resolution: 1 hour
# Model runs: Every 3 hours

VARIABLE = "ASWDIR_S"  # Global horizontal irradiance component
# GHI ≈ ASWDIR_S + ASWDIFD_S

print("Fetching ICON-CH1-EPS forecasts")
print("  Grid resolution: ~1 km")
print("  Forecast horizon: 33 hours")
print("  Temporal resolution: 1 hour")
print("  Model runs: every 3 hours")
print(f"  Variable: {VARIABLE}\n")

# Fetch all available horizons (0h to 33h)
# horizon parameter accepts: timedelta object or ISO 8601 duration string
print("Fetching all available horizons (0h to 33h)...")
all_data = []
failed_horizons = []
successful_horizons = []

for hour in range(0, 34):
    try:
        print(f"  Fetching horizon {hour}h...", end=" ")
        req = ogd_api.Request(
            collection="ogd-forecasting-icon-ch1",
            variable=VARIABLE,
            ref_time="latest",
            perturbed=False,
            horizon=timedelta(hours=hour),  # Use timedelta object
        )
        data = ogd_api.get_from_ogd(req)
        all_data.append(data)
        successful_horizons.append(hour)
        print(f"✓ (shape: {data.shape})")
    except Exception as e:
        failed_horizons.append(hour)
        print(f"✗ (not available: {str(e)[:50]})")

print(f"\nSuccessfully fetched: {len(successful_horizons)} horizons")
print(f"Failed/unavailable: {len(failed_horizons)} horizons")
if failed_horizons:
    print(f"Failed horizon(s): {failed_horizons}")

if len(all_data) == 0:
    print("\nERROR: No data fetched!")
    exit(1)

# Combine along the lead_time dimension
print("\nCombining data...")
data = xr.concat(all_data, dim='lead_time')

print(f"\n{'='*60}")
print("ICON-CH1-EPS FORECAST DATA")
print(f"{'='*60}")
print(f"Shape: {data.shape}")
print(f"Dimensions: {list(data.dims)}")
print(f"Coordinates: {list(data.coords)}")

# Save to CSV - separate static grid info from time-varying forecast data
print(f"\n{'='*60}")
print("SAVING TO CSV")
print(f"{'='*60}")

# Squeeze singleton dimensions
data_squeezed = data.squeeze(dim=['eps', 'ref_time'])

# Create output directory
Path("meteoswiss_data").mkdir(exist_ok=True)

# 1. Save static grid information (cell, lon, lat)
print("\n1. Creating grid reference file...")
grid_df = pd.DataFrame({
    'cell': data_squeezed.coords['cell'].values,
    'lon': data_squeezed.coords['lon'].values,
    'lat': data_squeezed.coords['lat'].values,
})
grid_file = "meteoswiss_data/icon_ch1_grid.csv"
print(f"   Saving to {grid_file}...")
grid_df.to_csv(grid_file, index=False)
print(f"   ✓ Grid file saved: {len(grid_df):,} cells")
print(f"     Columns: {list(grid_df.columns)}")
print(f"     File size: {Path(grid_file).stat().st_size / 1024 / 1024:.1f} MB")

# 2. Save forecast data (cell + lead times)
print("\n2. Creating forecast data file (wide format)...")
# Extract just the ASWDIR_S values in wide format
# Shape: (cell, lead_time) -> pivot to (cell) x (lead_time columns)

# Convert to DataFrame with proper structure
data_values = data_squeezed.values  # Shape: (34, 1147980)
cell_ids = data_squeezed.coords['cell'].values
lead_times = data_squeezed.coords['lead_time'].values

# Create DataFrame with lead times as columns
# Convert numpy.timedelta64 to hours
lead_time_hours = [int(lt / np.timedelta64(1, 'h')) for lt in lead_times]

forecast_df = pd.DataFrame(
    data_values.T,  # Transpose to (1147980, 34)
    columns=[f'lead_time_{h}h' for h in lead_time_hours]
)
forecast_df.insert(0, 'cell', cell_ids)

forecast_file = "meteoswiss_data/icon_ch1_forecast_aswdir_s.csv"
print(f"   Saving to {forecast_file}...")
forecast_df.to_csv(forecast_file, index=False)
print(f"   ✓ Forecast file saved: {len(forecast_df):,} cells × {len(forecast_df.columns)-1} lead times")
print(f"     Columns: cell, {list(forecast_df.columns[1:5])}... (showing first 4)")
print(f"     File size: {Path(forecast_file).stat().st_size / 1024 / 1024:.1f} MB")

print(f"\n{'='*60}")
print("✓ CSV FILES CREATED SUCCESSFULLY!")
print(f"{'='*60}")
print(f"1. Grid reference:  {grid_file}")
print(f"   - {len(grid_df):,} rows × {len(grid_df.columns)} columns")
print(f"2. Forecast data:   {forecast_file}")
print(f"   - {len(forecast_df):,} rows × {len(forecast_df.columns)} columns")
print(f"{'='*60}")
