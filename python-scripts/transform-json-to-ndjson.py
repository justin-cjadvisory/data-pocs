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
        # Function to remove keys that start with '@'
        def remove_at_keys(obj):
            if isinstance(obj, dict):
                # Create a new dictionary excluding keys that start with '@'
                return {k: remove_at_keys(v) for k, v in obj.items() if not k.startswith('@')}
            elif isinstance(obj, list):
                # Apply the function recursively to each item in the list
                return [remove_at_keys(item) for item in obj]
            else:
                return obj

        # Clean the data to remove any keys that start with '@'
        cleaned_data = remove_at_keys(data)

        # Write cleaned data to NDJSON format
        if isinstance(cleaned_data, list):
            for entry in cleaned_data:
                f.write(json.dumps(entry) + '\n')
        else:
            f.write(json.dumps(cleaned_data) + '\n')

if __name__ == "__main__":
    # Process each JSON file in the input folder
    for filename in os.listdir(input_folder):
        if filename.endswith('.json'):
            input_path = os.path.join(input_folder, filename)
            output_path = os.path.join(output_folder, filename.replace('.json', '.ndjson'))

            # Convert and save the data
            convert_json_to_ndjson(input_path, output_path)
            print(f"Transformed {filename} and saved as {output_path}")
