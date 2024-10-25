import requests
import json

# Define a dictionary to map endpoint types to their corresponding fields
endpoint_config = {
    "Accounts": [
        "AccountID", "Code", "Name", "Status", "Type", "TaxType", "Description", 
        "Class", "SystemAccount", "EnablePaymentsToAccount", "ShowInExpenseClaims", 
        "BankAccountNumber", "BankAccountType", "CurrencyCode", "ReportingCode", 
        "ReportingCodeName", "HasAttachments", "UpdatedDateUTC", "AddToWatchlist", 
        "DataFileID", "DataFileName", "DataFileCode"
    ],
    "AccountsClass": [
        "Class", "IsProfitLoss", "IsBalanceSheet", "IsDebit", "IsCredit", 
        "DrCrSign", "ProfitLossSign", "VarianceImpactSign"
    ],
    # Add more endpoint types and their respective fields here
    # "AnotherEndpointType": ["Field1", "Field2", ...]
}

# Define a function to get data based on the endpoint
def fetch_data(endpoint_type):
    # Base URL
    base_url = "https://data.odatalink.com/CJ-Adv-672986/ryan_developmen_4670/Rya-Dev/"
    
    # Construct the full URL based on the endpoint type
    url = f"{base_url}{endpoint_type}"

    # Check if the endpoint type is in the dictionary
    if endpoint_type not in endpoint_config:
        print(f"Unknown endpoint type: {endpoint_type}")
        return

    # Get the fields to display from the dictionary
    fields_to_display = endpoint_config[endpoint_type]

    # Make the GET request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()

        # Access the "value" field containing the account information
        accounts = data.get("value", [])

        # Print the data for each account
        for account in accounts:
            print(f"{endpoint_type} Details:")
            # Print DataFile details
            data_file = account.get("DataFile", {})
            # Print each field, including DataFile fields
            for field in fields_to_display:
                # Check if the field belongs to the account or the DataFile
                if field in data_file:
                    value = data_file.get(field)  # Get the value for DataFile fields
                else:
                    value = account.get(field)  # Get the value for account fields
                print(f"  {field}: {value}")
    else:
        print(f"Error: {response.status_code}, {response.text}")

# Example usage - loop through multiple endpoints dynamically from endpoint_config
endpoints_to_fetch = list(endpoint_config.keys())  # Dynamically get the endpoints from the dictionary keys

for endpoint in endpoints_to_fetch:
    fetch_data(endpoint)  # Fetch and display data for each endpoint type
