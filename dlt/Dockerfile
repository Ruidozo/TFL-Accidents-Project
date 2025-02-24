FROM python:3.10

# Set the working directory
WORKDIR /app

# Install required dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ingestion script and config
COPY ingest_data.py .
COPY config.yaml .

# Set environment variables for GCP authentication (update as needed)
ENV GOOGLE_APPLICATION_CREDENTIALS "/app/gcp_key.json"

# Copy Google Cloud credentials (ensure this is properly configured)
COPY gcp_key.json /app/gcp_key.json

# Run the ingestion script
CMD ["python", "ingest_data.py"]
