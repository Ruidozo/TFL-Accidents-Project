import os
import logging
import pandas as pd
import psycopg2
import psycopg2.extras
import gzip
import shutil
import json
import ast
from io import StringIO
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path=env_path)

# Database Credentials
DB_PARAMS = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# Google Cloud Storage (GCS) configuration
GCS_BUCKET = os.getenv("GCS_BUCKET")
GCS_RAW_PATH = os.getenv("GCS_RAW_PATH")
LOCAL_STORAGE = os.getenv("LOCAL_STORAGE")

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Ensure local directory exists
os.makedirs(LOCAL_STORAGE, exist_ok=True)

def connect_db():
    """Establish a connection to PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logging.info("✅ Successfully connected to PostgreSQL.")
        return conn
    except Exception as e:
        logging.error(f"❌ Error connecting to PostgreSQL: {e}")
        return None

def recreate_table(table_name="public.stg_tfl_accidents"):
    """Drop and recreate the PostgreSQL table to ensure the correct schema."""
    conn = connect_db()
    if not conn:
        return

    try:
        drop_table_sql = f"DROP TABLE IF EXISTS {table_name};"
        create_table_sql = f"""
            CREATE TABLE {table_name} (
                accident_id INTEGER PRIMARY KEY,
                lat FLOAT,
                lon FLOAT,
                location TEXT,
                accident_date TIMESTAMP,
                severity TEXT,
                borough TEXT,
                casualties JSONB, -- Stored as structured JSON
                vehicles JSONB -- Stored as structured JSON
            );
        """
        cur = conn.cursor()
        cur.execute(drop_table_sql)  # Ensure old table is removed
        cur.execute(create_table_sql)  # Recreate table with correct schema
        conn.commit()
        cur.close()
        logging.info(f"✅ Table `{table_name}` recreated successfully.")
    except Exception as e:
        logging.error(f"❌ Error creating table `{table_name}`: {e}")
    finally:
        conn.close()

def sanitize_json_field(field):
    """Sanitize and clean JSON-like fields, removing unnecessary keys."""
    if pd.isna(field) or field.strip() == "":
        return None
    try:
        parsed = ast.literal_eval(field)  # Convert to Python object
        if isinstance(parsed, list):  # Ensure it's a list
            cleaned_data = [{k: v for k, v in item.items() if k != "$type"} for item in parsed]
            return json.dumps(cleaned_data)  # Convert back to JSON string
        return json.dumps(parsed)  # Default fallback
    except (ValueError, SyntaxError):
        logging.warning(f"⚠️ Could not parse JSON field: {field}")
        return None   


def get_gcs_files():
    """List all GZipped CSV files in GCS."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blobs = list(bucket.list_blobs(prefix=GCS_RAW_PATH))

    gzipped_files = [blob.name for blob in blobs if blob.name.endswith(".csv.gz")]
    logging.info(f"📦 Found {len(gzipped_files)} compressed CSV files in GCS.")
    return gzipped_files

def download_gcs_file(file_path):
    """Download and extract a GZipped CSV file from GCS."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(file_path)

    local_gz_path = os.path.join(LOCAL_STORAGE, os.path.basename(file_path))
    local_csv_path = local_gz_path.replace(".gz", "")

    try:
        logging.info(f"📥 Downloading `{file_path}` from GCS...")
        blob.download_to_filename(local_gz_path)

        # Extract GZ file
        with gzip.open(local_gz_path, "rb") as f_in, open(local_csv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(local_gz_path)  # Remove compressed file after extraction

        logging.info(f"✅ Extracted `{local_csv_path}`.")
        return local_csv_path
    except Exception as e:
        logging.error(f"❌ Failed to download `{file_path}` from GCS: {e}")
        return None

def clean_and_transform_data(df):
    """Transform data to match PostgreSQL schema."""
    # Drop the unnecessary `$type` column
    if "$type" in df.columns:
        df.drop(columns=["$type"], inplace=True)

    # Rename columns for consistency
    rename_mapping = {
        "id": "accident_id",
        "date": "accident_date"
    }
    df.rename(columns=rename_mapping, inplace=True)

    # Ensure expected columns are present
    expected_columns = [
        "accident_id", "lat", "lon", "location", "accident_date",
        "severity", "borough", "casualties", "vehicles"
    ]
    df = df[[col for col in expected_columns if col in df.columns]]

    # Convert `accident_id` to integer safely
    df["accident_id"] = pd.to_numeric(df["accident_id"], errors="coerce").dropna().astype(int)

    # Convert `accident_date` to timestamp format
    df["accident_date"] = pd.to_datetime(df["accident_date"], errors="coerce")

    # Convert JSON-like columns to valid JSON format and clean `$type`
    for json_col in ["casualties", "vehicles"]:
        if json_col in df.columns:
            df[json_col] = df[json_col].apply(sanitize_json_field)

    return df

def create_table(table_name="public.stg_tfl_accidents"):
    """Create PostgreSQL table if it does not exist."""
    conn = connect_db()
    if not conn:
        return

    try:
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                accident_id INTEGER PRIMARY KEY,
                lat FLOAT,
                lon FLOAT,
                location TEXT,
                accident_date TIMESTAMP,
                severity TEXT,
                borough TEXT,
                casualties TEXT, -- Stored as JSON string
                vehicles TEXT -- Stored as JSON string
            );
        """
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        cur.close()
        logging.info(f"✅ Table `{table_name}` is ready.")
    except Exception as e:
        logging.error(f"❌ Error creating table `{table_name}`: {e}")
    finally:
        conn.close()

def load_csv_in_batches(file_path, table_name="public.stg_tfl_accidents", batch_size=10000):
    """Load CSV file into PostgreSQL in batches."""
    conn = connect_db()
    if not conn:
        return

    try:
        chunk_iterator = pd.read_csv(file_path, chunksize=batch_size)

        total_rows = 0
        for chunk in chunk_iterator:
            logging.debug(f"Columns in DataFrame: {chunk.columns.tolist()}")

            # Transform data
            chunk = clean_and_transform_data(chunk)

            # Convert to CSV buffer
            csv_buffer = StringIO()
            chunk.to_csv(csv_buffer, index=False, header=False, sep='\t')  # Use tab delimiter for PostgreSQL COPY
            csv_buffer.seek(0)

            # COPY to PostgreSQL with explicit column names
            copy_sql = f"""
                COPY {table_name} (accident_id, lat, lon, location, accident_date, severity, borough, casualties, vehicles)
                FROM STDIN WITH CSV DELIMITER E'\t' NULL 'NULL' QUOTE '"';
            """
            cur = conn.cursor()
            cur.copy_expert(copy_sql, csv_buffer)
            conn.commit()

            total_rows += len(chunk)
            logging.info(f"✅ Uploaded {len(chunk)} rows, Total: {total_rows}")

        cur.close()
        logging.info(f"🎯 Finished loading `{file_path}`: {total_rows} rows uploaded.")
    except Exception as e:
        logging.error(f"❌ Error loading `{file_path}`: {e}")
        conn.rollback()
    finally:
        conn.close()

def process_pipeline():
    """End-to-end pipeline: recreate table, and load CSV files from local storage into PostgreSQL."""
    recreate_table()  # Drop and recreate the table with the correct schema

    # List all GZipped CSV files in the local storage directory
    local_files = [f for f in os.listdir(LOCAL_STORAGE) if f.endswith(".csv.gz")]

    if not local_files:
        logging.warning("⚠️ No GZipped CSV files found in local storage.")
        return

    for local_file in local_files:
        local_gz_path = os.path.join(LOCAL_STORAGE, local_file)
        local_csv_path = local_gz_path.replace(".gz", "")

        # Extract GZ file
        try:
            logging.info(f"📥 Extracting `{local_gz_path}`...")
            with gzip.open(local_gz_path, "rb") as f_in, open(local_csv_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            logging.info(f"✅ Extracted `{local_csv_path}`.")
        except Exception as e:
            logging.error(f"❌ Failed to extract `{local_gz_path}`: {e}")
            continue

        # Load CSV in batches
        logging.info(f"📄 Processing `{local_csv_path}`...")
        load_csv_in_batches(local_csv_path)

if __name__ == "__main__":
    logging.info("🚀 Starting CSV ingestion pipeline...")
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
    process_pipeline()
    logging.info("🎯 Pipeline finished.")
