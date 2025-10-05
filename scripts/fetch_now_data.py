#!/usr/bin/env python3
"""
Fetch realtime weather measurement data from MeteoSwiss STAC API
Downloads 10-minute realtime data (_t_now.csv) for all stations

This script downloads realtime data from yesterday 12:00 UTC to now.
Updated every 10 minutes by MeteoSwiss.
Run this script frequently (e.g., every 10-30 minutes) to capture realtime data.
"""

import requests
from pathlib import Path
from datetime import datetime

# MeteoSwiss STAC API endpoints
STAC_API_BASE = "https://data.geo.admin.ch/api/stac/v1"
COLLECTION_ID = "ch.meteoschweiz.ogd-smn"

# Output directory for downloaded files
OUTPUT_DIR = Path("meteoswiss_data") / "now"
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
    Download 10-minute realtime data (_t_now.csv) for a station
    """
    # Filter for _t_now.csv file only
    now_asset = None
    for name, asset in assets.items():
        if name.endswith('_t_now.csv'):
            now_asset = (name, asset)
            break

    if not now_asset:
        print(f"  No _t_now.csv available for {station_id}")
        return 0

    # Create station directory
    station_dir = OUTPUT_DIR / station_id
    station_dir.mkdir(exist_ok=True)

    asset_name, asset_info = now_asset
    url = asset_info.get('href')

    if not url:
        print(f"  No URL for {asset_name}")
        return 0

    output_path = station_dir / asset_name

    # Always download (overwrite) to get latest realtime data
    print(f"  ↓ {station_id.upper()}: {asset_name}...", end='')

    if download_csv(url, output_path):
        print(" ✓")
        return 1
    else:
        print(" ✗")
        return 0

def main():
    print("=" * 70)
    print("MeteoSwiss Realtime Data Downloader")
    print("Downloading 10-minute realtime data (_t_now.csv) for all stations")
    print("Data range: Yesterday 12:00 UTC to now")
    print(f"Fetch time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
    print(f"Stations with realtime data: {stations_with_data}")
    print(f"Total files downloaded: {total_files}")
    print(f"Output directory: {OUTPUT_DIR.absolute()}")
    print("=" * 70)
    print()
    print("NOTE: Realtime data is updated every 10 minutes by MeteoSwiss")
    print("Run this script frequently to capture latest measurements")

if __name__ == "__main__":
    main()
