# **ETL Pipeline Documentation**

## **Overview**
This documentation covers the ingestion, minimal transformation, and loading process of **TFL accident data** into **PostgreSQL**. The scripts provide a straightforward workflow for handling raw accident data in compressed CSV format and efficiently storing it in a structured PostgreSQL table.

### **Features:**
✅ Supports **local** and **cloud-based** PostgreSQL deployment  
✅ Handles **compressed CSV (GZipped) and JSONL files**  
✅ **Cleans and structures** data before loading into PostgreSQL  
✅ Uses **batch processing** for efficient data ingestion  
✅ Stores **nested JSON fields** in PostgreSQL for structured querying  
✅ Stores raw data in a **GCS bucket** as `csv.gz` and `jsonl` files  

---

## **1. Process Overview**
The ingestion workflow consists of the following steps:

1️⃣ **Data Ingestion**  
   - Reads raw **GZipped CSV** files from local storage or **GCS bucket**  
   - Extracts and preprocesses the files  

2️⃣ **Minimal Data Transformation**  
   - Renames relevant fields for consistency  
   - Converts date fields into proper **timestamp format**  
   - Converts **nested JSON fields** into structured PostgreSQL `JSONB` type  

3️⃣ **Data Loading**  
   - Creates or recreates the **staging table** in PostgreSQL  
   - Loads data into PostgreSQL in **batches (chunked processing)**  
   - Uses `COPY` command for **fast bulk ingestion**  

---

## **2. Configuration - Local vs. Cloud Deployment**
The scripts allow users to **toggle between a local PostgreSQL instance and a Cloud SQL instance** using the `.env` file.

### **.env Configuration**
```ini
# Toggle between local & cloud DB
USE_CLOUD_DB=False  # Set to True to use Cloud SQL

# Local PostgreSQL
DB_HOST=localhost
DB_PORT=5433
DB_NAME=tfl_accidents
DB_USER=your_username
DB_PASSWORD=your_password

# Cloud SQL Instance Variables
CLOUD_DB_HOST=XXXX
CLOUD_DB_PORT=5432
CLOUD_DB_NAME=tfl_accidents
CLOUD_DB_USER=your_username
CLOUD_DB_PASSWORD=your_password

# GCS Storage Configuration
GCS_BUCKET=tfl-accidents-project-data-lake
GCS_FILE_PATH=raw/tfl_accidents.jsonl
GCS_CSV_PATH=/processed_data/raw/csv/
GCS_JSONL_PATH=raw/jsonl/
```
**How it works:**  
- If `USE_CLOUD_DB=True`, the scripts connect to the **Cloud SQL instance**  
- If `USE_CLOUD_DB=False`, the scripts connect to the **local PostgreSQL instance**  
- The raw files are stored in a **GCS bucket** as both **CSV.GZ and JSONL** files for flexibility  

---

## **3. Code Breakdown**

### **3.1 Ingestion**
📂 **File:** `ingest_data.py`  
🔹 **Functionality:**  
- Connects to PostgreSQL  
- Reads compressed CSV files (`.csv.gz`)  
- Extracts the raw CSV  
- Calls `load_to_postgres.py` for processing and storage  

🔹 **Key Function:**
```python
def extract_gz_file(gz_file_path):
    """Extracts a compressed CSV file (GZipped)."""
    local_csv_path = gz_file_path.replace(".gz", "")
    with gzip.open(gz_file_path, "rb") as f_in, open(local_csv_path, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(gz_file_path)  # Remove compressed file
    return local_csv_path
```
---

### **3.2 Minimal Transformation & Loading**
📂 **File:** `load_to_postgres.py`  
🔹 **Functionality:**  
- Cleans and structures the dataset  
- Ensures correct data types (integer, timestamp, JSON)  
- Uses **batch processing** for efficient insertion  
- Handles JSON columns (`casualties`, `vehicles`)  

🔹 **PostgreSQL Table Schema:**
```sql
CREATE TABLE public.stg_tfl_accidents (
    accident_id INTEGER PRIMARY KEY,
    lat FLOAT,
    lon FLOAT,
    location TEXT,
    accident_date TIMESTAMP,
    severity TEXT,
    borough TEXT,
    casualties JSONB,
    vehicles JSONB
);
```

🔹 **Efficient Batch Loading using `COPY`:**
```python
copy_sql = """
    COPY public.stg_tfl_accidents (accident_id, lat, lon, location, accident_date, severity, borough, casualties, vehicles)
    FROM STDIN WITH CSV DELIMITER E'\t' NULL 'NULL' QUOTE '"';
"""
cur.copy_expert(copy_sql, csv_buffer)
```
This method is **10x faster** than inserting rows one by one.

---

## **4. Running the Scripts**
### **Step 1: Setup Environment**
Ensure you have the required dependencies:
```bash
pip install -r requirements.txt
```

### **Step 2: Update `.env` file**
Decide whether to use **local** or **cloud** PostgreSQL:
```ini
USE_CLOUD_DB=False  # Set to True for Cloud SQL
```

### **Step 3: Run the Ingestion & Loading**
```bash
python ingest_data.py
```
This will:
✅ Extract the compressed CSV files  
✅ Clean and structure the data  
✅ Load it into PostgreSQL  
✅ Store raw files in **Google Cloud Storage (GCS)** as `csv.gz` and `jsonl`  

---

## **5. Summary**
| Feature | Status |
|---------|--------|
| Local & Cloud Support | ✅ |
| Compressed CSV & JSONL Handling | ✅ |
| JSON Fields in PostgreSQL | ✅ |
| Batch Processing for Efficiency | ✅ |
| Simple and Fast Data Ingestion | ✅ |
| Storage in GCS | ✅ |

---

This documentation ensures that the ingestion, minimal transformation, and loading scripts are **efficient and flexible**, allowing easy deployment both **locally and in the cloud**, while keeping raw files stored securely in **Google Cloud Storage**. 🚀

