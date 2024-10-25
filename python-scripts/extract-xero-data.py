# main.py
import requests
import json
import os
from config import endpoint_config, base_url  # Import the endpoint configuration

def save_json_to_file(data, filename):
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)  # Save with pretty print (indentation)

def fetch_data(endpoint_type):
    url = f"{base_url}{endpoint_type}"

    if endpoint_type not in endpoint_config:
        print(f"Unknown endpoint type: {endpoint_type}")
        return

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        output_directory = 'jsonoutput'
        os.makedirs(output_directory, exist_ok=True)

        filename = os.path.join(output_directory, f"{endpoint_type}.json")

        save_json_to_file(data, filename)
        print(f"Data for {endpoint_type} saved to {filename}.")  
    else:
        print(f"Error: {response.status_code}, {response.text}")

if __name__ == "__main__":
    endpoints_to_fetch = list(endpoint_config.keys()) 
    for endpoint in endpoints_to_fetch:
        fetch_data(endpoint)  
