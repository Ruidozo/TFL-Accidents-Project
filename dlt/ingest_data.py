import dlt
import requests
import yaml
import os
import json
import pandas as pd
import gzip
from google.cloud import storage
from datetime import datetime
from google.api_core.exceptions import RetryError
from google.api_core.retry import Retry

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

TFL_API_URL = config["tfl_api_url"]
START_YEAR = config["start_year"]
END_YEAR = config["end_year"]
GCS_BUCKET = config["gcs_bucket"]
LOCAL_STORAGE = config["local_storage"]
CSV_STORAGE = os.path.join(LOCAL_STORAGE, "csv_output")

# Ensure directories exist
os.makedirs(LOCAL_STORAGE, exist_ok=True)
os.makedirs(CSV_STORAGE, exist_ok=True)

def fetch_tfl_data(year):
    """Fetch accident data for a specific year from the TFL API."""
    url = f"{TFL_API_URL}/{year}"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Failed to fetch data for {year}. Status: {response.status_code}")
        return []

def load_tfl_data():
    """DLT pipeline to fetch, normalize, transform, and load TFL data."""

    # Configure DLT output directory
    dlt.config["destination.filesystem.bucket_url"] = LOCAL_STORAGE

    # Create DLT pipeline with filesystem destination
    pipeline = dlt.pipeline(
        pipeline_name="tfl_ingestion",
        destination="filesystem",
        dataset_name="tfl_accidents"
    )

    # Fetch and collect data for all years
    all_data = []
    for year in range(START_YEAR, END_YEAR + 1):
        print(f"📡 Fetching data for {year}...")
        data = fetch_tfl_data(year)
        for item in data:
            item["year"] = year  # Add year for partitioning
        all_data.extend(data)

    # Run the pipeline and store locally
    print("🚀 Storing data locally in JSON format...")
    pipeline.run(all_data, table_name="tfl_accidents_raw", write_disposition="replace")


    # Convert to CSV
    move_and_transform_data()

    # Upload CSVs to GCS
    upload_to_gcs()

def move_and_transform_data():
    """Move JSONL files, convert to CSV, and delete JSONL after processing."""
    # Automatically find the DLT output directory
    possible_dirs = [d for d in os.listdir(LOCAL_STORAGE) if d.startswith("tfl_accidents")]
    if possible_dirs:
        DLT_OUTPUT_DIR = os.path.join(LOCAL_STORAGE, possible_dirs[0])
    else:
        print(f"❌ ERROR: No valid DLT output directory found in {LOCAL_STORAGE}.")
        exit(1)

    print(f"✅ Found DLT output directory: {DLT_OUTPUT_DIR}")


    if not os.path.exists(DLT_OUTPUT_DIR):
        print(f"❌ DLT output directory '{DLT_OUTPUT_DIR}' not found. Check if the pipeline ran successfully.")
        exit(1)

    jsonl_files = []
    for root, _, files in os.walk(DLT_OUTPUT_DIR):
        for file in files:
            if file.endswith(".jsonl"):
                src_path = os.path.join(root, file)
                dest_path = os.path.join(LOCAL_STORAGE, file)

                # Move the JSONL file
                os.rename(src_path, dest_path)
                jsonl_files.append(dest_path)

    print(f"✅ Found {len(jsonl_files)} JSONL files. Converting to CSV...")

    for jsonl_file in jsonl_files:
        try:
            # Convert JSONL to CSV
            csv_path = os.path.join(CSV_STORAGE, os.path.basename(jsonl_file).replace(".jsonl", ".csv"))
            jsonl_to_csv(jsonl_file, csv_path)

            # Delete JSONL file after transformation
            os.remove(jsonl_file)
            print(f"🗑️ Deleted {jsonl_file} after conversion to CSV.")
        except Exception as e:
            print(f"❌ ERROR: Failed to convert {jsonl_file} to CSV. Exception: {e}")

    print("✅ Data transformed to CSV and JSONL deleted!")

def jsonl_to_csv(jsonl_file, csv_file):
    """Convert JSONL to CSV."""
    try:
        with gzip.open(jsonl_file, "rt", encoding="utf-8") as f:
            data = [json.loads(line) for line in f]
    except OSError:
        with open(jsonl_file, "r", encoding="utf-8") as f:
            data = [json.loads(line) for line in f]

    df = pd.DataFrame(data)
    df.to_csv(csv_file, index=False)
    print(f"✅ Converted {jsonl_file} → {csv_file}")

def upload_to_gcs():
    """Uploads both JSONL (raw) and CSV (processed) data to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    # Upload JSONL to Raw Data Lake
    for file in os.listdir(LOCAL_STORAGE):
        file_path = os.path.join(LOCAL_STORAGE, file)
        if file.endswith(".jsonl") and os.path.isfile(file_path):
            blob = bucket.blob(f"raw/jsonl/{file}")
            blob.chunk_size = 10 * 1024 * 1024  # ✅ Set chunk size correctly
            blob.upload_from_filename(file_path, timeout=300)
            print(f"✅ Uploaded RAW JSONL: {file} to GCS.")

    # Upload CSV to Processed Data Lake
    for file in os.listdir(CSV_STORAGE):
        file_path = os.path.join(CSV_STORAGE, file)
        if os.path.isfile(file_path):
            blob = bucket.blob(f"processed/csv/{file}")
            blob.chunk_size = 10 * 1024 * 1024  # ✅ Set chunk size correctly
            blob.upload_from_filename(file_path, timeout=300)
            print(f"✅ Uploaded PROCESSED CSV: {file} to GCS.")



if __name__ == "__main__":
    load_tfl_data()