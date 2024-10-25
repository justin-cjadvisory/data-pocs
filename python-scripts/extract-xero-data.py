# main.py
import requests
import json
import os
from config import endpoint_config, base_url  # Import the endpoint configuration

def save_json_to_file(data, filename):
    """Saves the given data to a JSON file."""
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)  # Save with pretty print (indentation)

def fetch_data(endpoint_type):
    """Fetches data from a specified endpoint and saves it to a JSON file."""
    url = f"{base_url}{endpoint_type}"

    if endpoint_type not in endpoint_config:
        print(f"Unknown endpoint type: {endpoint_type}")
        return

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        # Create the output directory if it doesn't exist
        output_directory = 'jsonoutput'
        os.makedirs(output_directory, exist_ok=True)

        # Define the filename for the JSON file
        filename = os.path.join(output_directory, f"{endpoint_type}.json")

        # Save the data to a JSON file
        save_json_to_file(data, filename)
        print(f"Data for {endpoint_type} saved to {filename}.")  # Optional: confirmation message
    else:
        print(f"Error: {response.status_code}, {response.text}")

if __name__ == "__main__":
    # Example usage
    endpoints_to_fetch = list(endpoint_config.keys())  # Get the endpoint types dynamically
    for endpoint in endpoints_to_fetch:
        fetch_data(endpoint)  # Fetch and save data for each endpoint type
