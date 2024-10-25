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
    "AccountsTable": [
        "DataFileID", "DataFileName", "DataFileCode", "AccountID", "Code",
        "Name", "Status", "Type", "TaxType", "Description", "Class", "SystemAccount",
        "EnablePaymentsToAccount", "ShowInExpenseClaims", "BankAccountNumber",
        "BankAccountType", "CurrencyCode", "ReportingCode", "ReportingCodeName",
        "HasAttachments", "UpdatedDateUTC", "AddToWatchlist"
    ],
    "AccountsTotals": [
        "AccountTotal", "AccountTotalName", "IsProfitLoss", 
        "IsBalanceSheet", "IsAccountClassification", "Order", 
        "VarianceImpactSign", "AccountsTypes"  
    ],
    "AccountsTypes": [
        "AccountType", "AccountTypeName", "Class", "IsProfitLoss",
        "IsBalanceSheet", "IsDebit", "IsCredit", "DrCrSign",
        "VarianceImpactSign"
    ],
    "BalanceSheetAdvanced": [
        "DataFileID", "DataFileName", "DataFileCode", "Date", "PaymentsOnly", "StandardLayout", 
        "TimeFrame", "Periods", "TrackingOptionID", "TrackingOptionID2", "LineType", "AccountID", 
        "AccountName", "AccountLineType", "LineDescription", "Amount"
    ],
    "BalanceSheetByMonth": [
        "DataFileID", "DataFileName", "DataFileCode", "FinancialYear", "PaymentsOnly", 
        "StandardLayout", "UpdatedDateUTC", "Lines"
    ],
    "BalanceSheetByTrackingOption": [
        "DataFileID", "DataFileName", "DataFileCode", "Date", 
        "PaymentsOnly", "StandardLayout", "TrackingOptionID", 
        "TrackingOptionID2", "LineType", "AccountID", 
        "AccountName", "AccountLineType", "Amount"
    ],
    "BalanceSheetMultiPeriodTable": [
        "DataFileID", "DataFileName", "DataFileCode", "Date", 
        "Period", "PaymentsOnly", "StandardLayout", "LineType", 
        "AccountID", "AccountName", "AccountLineType", "Amount"
    ],
    "BalanceSheetTable": [
        "DataFileID", "DataFileName", "DataFileCode", "Date", "PaymentsOnly", 
        "StandardLayout", "LineType", "AccountID", "AccountName", 
        "AccountLineType", "Amount"
    ],
    "BankSummary": [
        "DataFileID", "DataFileName", "DataFileCode", 
        "FromDate", "ToDate", "UpdatedDateUTC", "Lines", 
        "LineType", "AccountID", "AccountName", "OpeningBalanceAmount", 
        "CashReceivedAmount", "CashSpentAmount", 
        "FXGainLossAmount", "ClosingBalanceAmount"
    ],
    "BankTransactions": ["DataFileID", "DataFileName", "DataFileCode", "BankTransactionID", "AccountID", "Code", "Name", "BatchPayment", "Type", "Reference", "Url", "IsReconciled", "PrepaymentID", "OverpaymentID", "HasAttachments", "ContactID", "ContactName", "Date", "Status", "SubTotal", "TotalTax", "Total", "UpdatedDateUTC", "CurrencyCode", "CurrencyRate"]







}

def print_data(data, level=0):
    """Recursively prints data, supporting nested dictionaries and lists."""
    indent = "  " * level  # Indentation based on recursion level

    if isinstance(data, dict):
        for key, value in data.items():
            print(f"{indent}{key}:")
            print_data(value, level + 1)  # Recurse into the value
            if level == 0:
                print()  # Add a line break after top-level items for better separation
    elif isinstance(data, list):
        for item in data:
            print(f"{indent}- ")  # Start a bullet point for list items
            if isinstance(item, dict):  # Check if the item is a dictionary
                print_data(item, level + 1)  # Print the dictionary with increased indentation
            else:
                print(f"{indent}  {item}")  # Print non-dictionary items with additional indentation
    else:
        print(f"{indent}{data}")  # Print the value directly

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
            for field in fields_to_display:
                # Access the field data
                value = account.get(field)
                if value is not None:  # Ensure value is not None
                    print(f"  {field}:")
                    print_data(value)  # Call the recursive function for any nested structure
            print()  # Add an extra line break for separation between accounts
    else:
        print(f"Error: {response.status_code}, {response.text}")

# Example usage - loop through multiple endpoints dynamically from endpoint_config
endpoints_to_fetch = list(endpoint_config.keys())  # Dynamically get the endpoints from the dictionary keys

# for endpoint in endpoints_to_fetch:
#     fetch_data(endpoint)  # Fetch and display data for each endpoint type

fetch_data("BalanceSheetTable")  # Fetch and display data for each endpoint type
