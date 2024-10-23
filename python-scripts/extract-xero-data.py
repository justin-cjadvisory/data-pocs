import requests

url_test = 'https://data.odatalink.com/CJ-Adv-672986/ryan_developmen_4670/Rya-Dev/'

try:
    r = requests.get(url_test)
    r.raise_for_status()  # Ensure request was successful
    r_json = r.json()     # Parse JSON content
    
    # Loop through 'value' and print the whole object for debugging
    for value in r_json.get('value', []):
        print("Full value:", value)  # Debug: Print entire object
        url = value.get('url')  # Extract 'AccountID' safely
        if url is not None:
            print(f"URL: {url}")
        else:
            print("URL not found")
        
except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")
except ValueError:
    print("Error decoding JSON response.")
