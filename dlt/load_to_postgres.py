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
from dotenv import load_dotenv

# Load environment variables
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(dotenv_path=env_path)

# Determine whether to use cloud or local PostgreSQL
USE_CLOUD_DB = os.getenv("USE_CLOUD_DB", "False").strip().lower() == "true"

DB_PARAMS = {
    "host": os.getenv("CLOUD_DB_HOST") if USE_CLOUD_DB else os.getenv("DB_HOST"),
    "port": os.getenv("CLOUD_DB_PORT") if USE_CLOUD_DB else os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("CLOUD_DB_NAME") if USE_CLOUD_DB else os.getenv("DB_NAME"),
    "user": os.getenv("CLOUD_DB_USER") if USE_CLOUD_DB else os.getenv("DB_USER"),
    "password": os.getenv("CLOUD_DB_PASSWORD") if USE_CLOUD_DB else os.getenv("DB_PASSWORD")
}

# Local storage configuration
LOCAL_STORAGE = os.getenv("LOCAL_STORAGE", "downloaded_data")

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Ensure local directory exists
os.makedirs(LOCAL_STORAGE, exist_ok=True)

def connect_db():
    """Establish a connection to PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logging.info(f"✅ Connected to PostgreSQL {'(Cloud)' if USE_CLOUD_DB else '(Local)'}")
        return conn
    except Exception as e:
        logging.error(f"❌ Database connection failed: {e}")
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
        cur.execute(drop_table_sql)
        cur.execute(create_table_sql)
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
        if isinstance(parsed, list):
            cleaned_data = [{k: v for k, v in item.items() if k != "$type"} for item in parsed]
            return json.dumps(cleaned_data)
        return json.dumps(parsed)
    except (ValueError, SyntaxError):
        logging.warning(f"⚠️ Could not parse JSON field: {field}")
        return None   

def get_local_files():
    """List all GZipped CSV files in the LOCAL_STORAGE directory."""
    local_files = [f for f in os.listdir(LOCAL_STORAGE) if f.endswith(".csv.gz")]
    logging.info(f"📂 Found {len(local_files)} compressed CSV files in `{LOCAL_STORAGE}`.")
    return local_files

def extract_gz_file(gz_file_path):
    """Extract a GZipped CSV file."""
    local_csv_path = gz_file_path.replace(".gz", "")

    try:
        with gzip.open(gz_file_path, "rb") as f_in, open(local_csv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove(gz_file_path)  # Remove compressed file after extraction
        logging.info(f"✅ Extracted `{local_csv_path}`.")
        return local_csv_path
    except Exception as e:
        logging.error(f"❌ Failed to extract `{gz_file_path}`: {e}")
        return None

def clean_and_transform_data(df):
    """Transform data to match PostgreSQL schema."""
    if "$type" in df.columns:
        df.drop(columns=["$type"], inplace=True)

    rename_mapping = {
        "id": "accident_id",
        "date": "accident_date"
    }
    df.rename(columns=rename_mapping, inplace=True)

    expected_columns = [
        "accident_id", "lat", "lon", "location", "accident_date",
        "severity", "borough", "casualties", "vehicles"
    ]
    df = df[[col for col in expected_columns if col in df.columns]]

    df["accident_id"] = pd.to_numeric(df["accident_id"], errors="coerce").dropna().astype(int)
    df["accident_date"] = pd.to_datetime(df["accident_date"], errors="coerce")

    for json_col in ["casualties", "vehicles"]:
        if json_col in df.columns:
            df[json_col] = df[json_col].apply(sanitize_json_field)

    return df

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
            chunk = clean_and_transform_data(chunk)

            csv_buffer = StringIO()
            chunk.to_csv(csv_buffer, index=False, header=False, sep='\t')
            csv_buffer.seek(0)

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
    """End-to-end pipeline: recreate table, process local CSV files, and load them into PostgreSQL."""
    recreate_table()

    local_files = get_local_files()
    if not local_files:
        logging.warning("⚠️ No GZipped CSV files found in LOCAL_STORAGE.")
        return

    for local_file in local_files:
        local_gz_path = os.path.join(LOCAL_STORAGE, local_file)
        local_csv_path = extract_gz_file(local_gz_path)
        if not local_csv_path:
            continue

        logging.info(f"📄 Processing `{local_csv_path}`...")
        load_csv_in_batches(local_csv_path)
        os.remove(local_csv_path) # Remove CSV file after loading

if __name__ == "__main__":
    logging.info("🚀 Starting CSV ingestion pipeline...")
    process_pipeline()
    logging.info("🎯 Pipeline finished.")
