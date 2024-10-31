import os
from google.cloud import bigquery
from google.cloud import storage
from config import bucket_name, project_name, dataset_id

# Confirm the BigQuery module is imported successfully
print("BigQuery module imported successfully!")

# Initialize clients for BigQuery and Storage
bq_client = bigquery.Client(project=project_name)
gcs_client = storage.Client(project=project_name)

# Define the GCS URI for NDJSON files
source_folder = f"gs://{bucket_name}/ndjson/"

def main():
    # Step 1: Check if Dataset exists, if not, create it
    dataset_id_full = f"{project_name}.{dataset_id}"  # Full dataset ID
    try:
        bq_client.get_dataset(dataset_id_full)  # Attempt to retrieve the dataset
        print(f"Dataset {dataset_id_full} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_id_full)
        dataset.location = "US"  # Set the location as needed
        bq_client.create_dataset(dataset, exists_ok=True)
        print(f"Created dataset {dataset_id_full}.")

    # Step 2: List NDJSON files in the GCS bucket
    blobs = gcs_client.list_blobs(bucket_name, prefix="ndjson/")

    # Step 3: Load each NDJSON file into a separate table
    for blob in blobs:
        if blob.name.endswith('.ndjson'):
            # Use the filename without '.ndjson' as table ID
            table_id = blob.name[len("ndjson/"):-8]  # Remove the prefix and '.ndjson'
            table_id = table_id.replace('/', '_')  # Replace any '/' with '_'
            table_ref = f"{dataset_id_full}.{table_id}"  # Full table ID

            # Load data from GCS to BigQuery
            source_uri = blob.public_url  # GCS URI for the NDJSON file
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,  # Automatically infers schema from data
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite the old table
            )

            # Load the NDJSON file into the BigQuery table
            load_job = bq_client.load_table_from_uri(
                source_uri,
                table_ref,
                job_config=job_config
            )

            # Wait for the job to complete
            load_job.result()
            print(f"Loaded {load_job.output_rows} rows into {table_ref}.")

if __name__ == "__main__":
    main()
