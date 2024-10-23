import requests
import json

# Replace with your actual OData endpoint URL
url = "https://data.odatalink.com/CJ-Adv-672986/ryan_developmen_4670/Rya-Dev/Accounts"

# Make the GET request without headers
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()
    
    # Access the "value" field containing the account information
    accounts = data.get("value", [])
    
    # Define a list of the fields you want to print
    fields_to_display = [
        "AccountID",
        "Code",
        "Name",
        "Status",
        "Type",
        "TaxType",
        "Description",
        "Class",
        "SystemAccount",
        "EnablePaymentsToAccount",
        "ShowInExpenseClaims",
        "BankAccountNumber",
        "BankAccountType",
        "CurrencyCode",
        "ReportingCode",
        "ReportingCodeName",
        "HasAttachments",
        "UpdatedDateUTC",
        "AddToWatchlist",
        "DataFileID",         # Added DataFile fields
        "DataFileName",
        "DataFileCode"
    ]

    # Process each account
    for account in accounts:
        print("Account Details:")
        
        # Print DataFile details
        data_file = account.get("DataFile", {})
        
        # Print each field, including DataFile fields
        for field in fields_to_display:
            # Check if the field belongs to the account or the DataFile
            if field in data_file:
                value = data_file.get(field)  # Get the value for DataFile fields
            else:
                value = account.get(field)  # Get the value for account fields
            
            print(f"  {field}: {value}")  # Print the field and its value

else:
    print(f"Error: {response.status_code}, {response.text}")
