#!/usr/bin/env python3
"""
Fetch historical weather measurement data from MeteoSwiss STAC API
Downloads ALL 10-minute historical data files for all stations

This script downloads complete historical data (from measurement start to Dec 31 of last year).
Downloads all decade files (e.g., 1980-1989, 1990-1999, 2000-2009, etc.).
Updated once per year by MeteoSwiss.
"""

import requests
from pathlib import Path

# MeteoSwiss STAC API endpoints
STAC_API_BASE = "https://data.geo.admin.ch/api/stac/v1"
COLLECTION_ID = "ch.meteoschweiz.ogd-smn"

# Output directory for downloaded files
OUTPUT_DIR = Path("meteoswiss_data") / "historical"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def fetch_all_stations():
    """
    Fetch all stations from the STAC API using pagination
    Returns a list of all station features
    """
    all_features = []
    url = f"{STAC_API_BASE}/search"

    # Initial request
    payload = {
        "collections": [COLLECTION_ID],
        "limit": 100  # Max items per page
    }

    print(f"Fetching stations from {COLLECTION_ID}...")

    while True:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        data = response.json()

        features = data.get('features', [])
        all_features.extend(features)

        print(f"  Fetched {len(features)} stations (total: {len(all_features)})")

        # Check for next page
        links = data.get('links', [])
        next_link = next((link for link in links if link.get('rel') == 'next'), None)

        if not next_link:
            break

        # Update payload with cursor for next page
        cursor = next_link.get('body', {}).get('cursor')
        if cursor:
            payload['cursor'] = cursor
        else:
            break

    return all_features

def download_csv(url, output_path):
    """Download a CSV file from URL to output_path"""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        with open(output_path, 'wb') as f:
            f.write(response.content)

        return True
    except Exception as e:
        print(f"      Error: {e}")
        return False

def download_station_data(station_id, assets):
    """
    Download ALL 10-minute historical data (_t_historical_*.csv) files for a station
    """
    # Filter for _t_historical_*.csv files only
    historical_assets = {
        name: asset for name, asset in assets.items()
        if '_t_historical_' in name and name.endswith('.csv')
    }

    if not historical_assets:
        print(f"  No _t_historical files available for {station_id}")
        return 0

    # Create station directory
    station_dir = OUTPUT_DIR / station_id
    station_dir.mkdir(exist_ok=True)

    downloaded = 0
    print(f"  Station {station_id.upper()}: {len(historical_assets)} historical files")

    # Download all historical files
    for asset_name, asset_info in historical_assets.items():
        url = asset_info.get('href')
        if not url:
            continue

        output_path = station_dir / asset_name

        # Skip if already downloaded
        if output_path.exists():
            print(f"    ✓ {asset_name} (already exists)")
            downloaded += 1
            continue

        print(f"    ↓ {asset_name}...", end='')

        if download_csv(url, output_path):
            print(" ✓")
            downloaded += 1
        else:
            print(" ✗")

    return downloaded

def main():
    print("=" * 70)
    print("MeteoSwiss Historical Data Downloader")
    print("Downloading ALL 10-minute historical files per station")
    print("Data range: Measurement start to December 31st of last year")
    print("Files: All decade files (e.g., 1980-1989, 1990-1999, etc.)")
    print("=" * 70)
    print()

    # Fetch all stations
    stations = fetch_all_stations()
    print(f"\nTotal stations found: {len(stations)}")
    print("=" * 70)
    print()

    # Download data for each station
    total_files = 0
    stations_with_data = 0

    for idx, station in enumerate(stations, 1):
        station_id = station.get('id')
        station_title = station.get('properties', {}).get('title', 'N/A')
        assets = station.get('assets', {})

        print(f"[{idx}/{len(stations)}] {station_title}")

        files_downloaded = download_station_data(station_id, assets)

        if files_downloaded > 0:
            stations_with_data += 1
            total_files += files_downloaded

        print()

    # Summary
    print("=" * 70)
    print("Download Summary")
    print("=" * 70)
    print(f"Stations processed: {len(stations)}")
    print(f"Stations with historical data: {stations_with_data}")
    print(f"Total files downloaded: {total_files}")
    print(f"Output directory: {OUTPUT_DIR.absolute()}")
    print("=" * 70)

if __name__ == "__main__":
    main()
