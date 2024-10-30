import os
from google.cloud import storage
from config import bucket_name, output_folder, project_name

# Initialize the Cloud Storage client
client = storage.Client(project=project_name)
bucket = client.bucket(bucket_name)

def upload_file_to_gcs(local_path, bucket, destination_blob_name):
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} to gs://{bucket_name}/{destination_blob_name}")

# Main execution block
if __name__ == "__main__":
    # Upload each .ndjson file from the output folder to the GCS bucket
    for filename in os.listdir(output_folder):
        if filename.endswith('.ndjson'):
            local_path = os.path.join(output_folder, filename)
            destination_blob_name = f'ndjson/{filename}'  # Folder structure within the bucket
            upload_file_to_gcs(local_path, bucket, destination_blob_name)
