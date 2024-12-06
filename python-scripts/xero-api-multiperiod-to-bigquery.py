import json
import requests
from google.cloud import storage, bigquery
from datetime import datetime, timedelta
import calendar
from config import endpoint_config, base_url, bucket_name, project_name, dataset_id

# Initialize Google Cloud clients
storage_client = storage.Client(project=project_name)
bigquery_client = bigquery.Client(project=project_name)

# Function to get the last day of a given month
def get_end_of_month(date):
    # Returns the last day of the month for a given date
    next_month = date.replace(day=28) + timedelta(days=4)  # Move to the 1st of next month
    return next_month - timedelta(days=next_month.day)

# Function to fetch data from the Xero API with dynamic date range
def fetch_data(endpoint_type, from_date, to_date):
    url = f"{base_url}{endpoint_type}(FromDate={from_date},ToDate={to_date})"
    
    if endpoint_type not in endpoint_config:
        print(f"Unknown endpoint type: {endpoint_type}")
        return None

    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching {endpoint_type} for date range ({from_date} - {to_date}): {response.status_code}, {response.text}")
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

    # Initialize the starting date
    initial_from_date = "2021-01-01"

    # Get the end of the current month (for to_date)
    current_date = datetime.today()  # This gives today's date and time
    to_date = get_end_of_month(current_date).strftime("%Y-%m-%d")  # Get end of current month

    # Loop through the endpoints
    for endpoint in ["BalanceSheetMultiPeriodTable", "ProfitAndLossMultiPeriodTable", "TrialBalanceMultiPeriodTable"]:
        print(f"Processing {endpoint}...")

        # Reset from_date for each endpoint to the initial starting date
        from_date = initial_from_date

        # Start the date range loop for the current endpoint
        while True:
            # Get the last day of the current month for 'to_date'
            next_to_date = get_end_of_month(datetime.strptime(from_date, "%Y-%m-%d")).strftime("%Y-%m-%d")
            
            # Print the data range being processed
            print(f"Fetching data for {from_date} to {next_to_date}...")

            # Fetch the data from the API
            data = fetch_data(endpoint, from_date, next_to_date)
            if data:
                # Transform data to NDJSON format
                ndjson_lines = transform_to_ndjson(data)
                
                # Upload the data to GCS
                destination_blob_name = f'ndjson/{endpoint}_{from_date}_{next_to_date}.ndjson'
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

                source_uri = f"gs://{bucket_name}/{destination_blob_name}"
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    autodetect=True,
                    write_disposition=bigquery.WriteDisposition.WRITE_APPEND
                )

                load_job = bigquery_client.load_table_from_uri(
                    source_uri,
                    f"{dataset_id_full}.{endpoint}",
                    job_config=job_config
                )

                load_job.result()
                print(f"Loaded {load_job.output_rows} rows into {dataset_id_full}.{endpoint}.")
            
            # Update the from_date and next_to_date for the next iteration
            # Set next_from_date to the 1st of the next month
            next_from_date = (datetime.strptime(next_to_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            
            # If next_from_date exceeds current date, stop
            if datetime.strptime(next_from_date, "%Y-%m-%d") > datetime.strptime(to_date, "%Y-%m-%d"):
                print(f"End of data range reached for {endpoint}.")
                break
            
            # Set from_date to next_from_date for the next iteration
            from_date = next_from_date
