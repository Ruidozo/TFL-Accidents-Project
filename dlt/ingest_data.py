import dlt
import requests
import yaml
import os
import json
import pandas as pd
import gzip
from google.cloud import storage
from datetime import datetime

# Load configuration
try:
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
        if config is None:
            raise ValueError("Configuration file is empty")
except FileNotFoundError:
    print("❌ Configuration file 'config.yaml' not found.")
    exit(1)
except ValueError as e:
    print(f"❌ {e}")
    exit(1)

# Extract config values
TFL_API_URL = config["tfl_api_url"]
START_YEAR = config["start_year"]
END_YEAR = config["end_year"]
GCS_BUCKET = config["gcs_bucket"]
LOCAL_STORAGE = config["local_storage"]
RAW_JSONL_STORAGE = os.path.join(LOCAL_STORAGE, "raw/jsonl")
RAW_CSV_STORAGE = os.path.join(LOCAL_STORAGE, "raw/csv")

# Ensure directories exist
os.makedirs(LOCAL_STORAGE, exist_ok=True)
os.makedirs(RAW_JSONL_STORAGE, exist_ok=True)
os.makedirs(RAW_CSV_STORAGE, exist_ok=True)

def fetch_tfl_data(year):
    """Fetch accident data for a specific year from the TFL API."""
    url = f"{TFL_API_URL}/{year}"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Failed to fetch data for {year}. Status: {response.status_code}")
        return []

def save_jsonl(data, file_path):
    """Saves data in JSONL format."""
    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        for record in data:
            f.write(json.dumps(record) + "\n")
    print(f"✅ Stored RAW JSONL: {file_path}")

def save_csv(data, file_path):
    """Saves data in CSV format."""
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    print(f"✅ Stored RAW CSV: {file_path}")

def load_tfl_data():
    """DLT pipeline to fetch, normalize, and store raw accident data."""
    
    dlt.config["destination.filesystem.bucket_url"] = LOCAL_STORAGE
    pipeline = dlt.pipeline(
        pipeline_name="tfl_ingestion",
        destination="filesystem",
        dataset_name="tfl_accidents"
    )

    for year in range(START_YEAR, END_YEAR + 1):
        print(f"📡 Fetching data for {year}...")
        data = fetch_tfl_data(year)

        if not data:
            print(f"⚠️ No data found for {year}. Skipping.")
            continue

        for item in data:
            item["year"] = year  # Add year for partitioning

        # Store raw JSONL & CSV files
        jsonl_file_path = os.path.join(RAW_JSONL_STORAGE, f"tfl_accidents_{year}.jsonl.gz")
        csv_file_path = os.path.join(RAW_CSV_STORAGE, f"tfl_accidents_{year}.csv")

        save_jsonl(data, jsonl_file_path)
        save_csv(data, csv_file_path)

        # Upload files to GCS
        upload_to_gcs(data_type="jsonl", file_path=jsonl_file_path, year=year)
        upload_to_gcs(data_type="csv", file_path=csv_file_path, year=year)

    print("🎯 Data ingestion completed successfully!")

def upload_to_gcs(data_type="jsonl", file_path=None, year=None):
    """Uploads JSONL and CSV data to Google Cloud Storage, organized per year."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    if data_type == "jsonl":
        folder = f"raw/jsonl/tfl_accidents_{year}.jsonl.gz"
    elif data_type == "csv":
        folder = f"raw/csv/tfl_accidents_{year}.csv"
    else:
        print("❌ Invalid data type specified for upload.")
        return

    blob = bucket.blob(folder)
    blob.chunk_size = 10 * 1024 * 1024  # ✅ Set chunk size correctly
    blob.upload_from_filename(file_path, timeout=300)

    print(f"✅ Uploaded {data_type.upper()} file: {file_path} to GCS ({folder}).")

if __name__ == "__main__":
    load_tfl_data()
