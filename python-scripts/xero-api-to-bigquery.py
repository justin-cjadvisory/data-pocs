import json
import requests
from google.cloud import storage, bigquery
from config import endpoint_config, base_url, bucket_name, project_name, dataset_id

# Initialize Google Cloud clients
storage_client = storage.Client(project=project_name)
bigquery_client = bigquery.Client(project=project_name)

# Function to fetch data from Xero API
def fetch_data(endpoint_type):
    url = f"{base_url}{endpoint_type}"

    if endpoint_type not in endpoint_config:
        print(f"Unknown endpoint type: {endpoint_type}")
        return None

    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching {endpoint_type}: {response.status_code}, {response.text}")
        return None

# Function to remove keys that start with '@' and convert to NDJSON format
def transform_to_ndjson(data):
    def remove_at_keys(obj):
        if isinstance(obj, dict):
            return {k: remove_at_keys(v) for k, v in obj.items() if not k.startswith('@')}
        elif isinstance(obj, list):
            return [remove_at_keys(item) for item in obj]
        else:
            return obj

    cleaned_data = remove_at_keys(data)
    ndjson_lines = []
    if isinstance(cleaned_data, list):
        for entry in cleaned_data:
            ndjson_lines.append(json.dumps(entry))
    else:
        ndjson_lines.append(json.dumps(cleaned_data))
    return ndjson_lines

# Function to upload NDJSON data directly to Google Cloud Storage
def upload_ndjson_to_gcs(ndjson_lines, bucket, destination_blob_name):
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string("\n".join(ndjson_lines))
    print(f"Uploaded NDJSON data to gs://{bucket_name}/{destination_blob_name}")

# Main execution
if __name__ == "__main__":
    bucket = storage_client.bucket(bucket_name)

    # Step 1: Fetch and transform data, then upload directly to GCS
    for endpoint in endpoint_config.keys():
        print(f"Processing {endpoint}...")
        data = fetch_data(endpoint)
        if data:
            ndjson_lines = transform_to_ndjson(data)
            destination_blob_name = f'ndjson/{endpoint}.ndjson'
            upload_ndjson_to_gcs(ndjson_lines, bucket, destination_blob_name)

    # Step 2: Load NDJSON files into BigQuery
    dataset_id_full = f"{project_name}.{dataset_id}"
    try:
        bigquery_client.get_dataset(dataset_id_full)
        print(f"Dataset {dataset_id_full} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_id_full)
        dataset.location = "US"
        bigquery_client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset {dataset_id_full}.")

    blobs = storage_client.list_blobs(bucket_name, prefix="ndjson/")
    for blob in blobs:
        if blob.name.endswith('.ndjson'):
            filename = blob.name.split('/')[-1]
            table_id = filename.rsplit('.ndjson', 1)[0]
            table_ref = f"{dataset_id_full}.{table_id}"

            source_uri = f"gs://{bucket_name}/{blob.name}"
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )

            load_job = bigquery_client.load_table_from_uri(
                source_uri,
                table_ref,
                job_config=job_config
            )

            load_job.result()
            print(f"Loaded {load_job.output_rows} rows into {table_ref}.")
