import os
import json

# Paths to input and output folders
input_folder = 'jsonoutput'
output_folder = 'jsonoutput/newline-delimited'

# Ensure the output folder exists
os.makedirs(output_folder, exist_ok=True)

# Function to read JSON, transform, and save as newline-delimited JSON
def convert_json_to_ndjson(input_path, output_path):
    with open(input_path, 'r') as f:
        data = json.load(f)
    
    with open(output_path, 'w') as f:
        # If data is a list of objects
        if isinstance(data, list):
            for entry in data:
                f.write(json.dumps(entry) + '\n')
        # If data is a single JSON object, wrap it in a list to handle it
        else:
            f.write(json.dumps(data) + '\n')

# Process each JSON file in the input folder
for filename in os.listdir(input_folder):
    if filename.endswith('.json'):
        input_path = os.path.join(input_folder, filename)
        output_path = os.path.join(output_folder, filename.replace('.json', '.ndjson'))
        
        # Convert and save the data
        convert_json_to_ndjson(input_path, output_path)
        print(f"Transformed {filename} and saved as {output_path}")
