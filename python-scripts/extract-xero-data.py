from config import CLIENT_ID, CLIENT_SECRET

from xero_python.accounting import AccountingApi, ContactPerson, Contact, Contacts
from xero_python.api_client import ApiClient, serialize
from xero_python.api_client.configuration import Configuration
from xero_python.api_client.oauth2 import OAuth2Token
from xero_python.exceptions import AccountingBadRequestException
from xero_python.identity import IdentityApi
from xero_python.utils import getvalue

api_client = ApiClient(
    Configuration(
        debug=False,
        oauth2_token=OAuth2Token(
            client_id= CLIENT_ID, client_secret= CLIENT_SECRET
        ),
    ),
    pool_threads=1,
)

api_client.set_oauth2_token("YOUR_ACCESS_TOKEN")

def accounting_get_bank_transactions_history():
    api_instance = AccountingApi(api_client)
    xero_tenant_id = 'YOUR_XERO_TENANT_ID'
    bank_transaction_id = '00000000-0000-0000-0000-000000000000'
    
    try:
        api_response = api_instance.get_bank_transactions_history(xero_tenant_id, bank_transaction_id)
        print(api_response)
    except AccountingBadRequestException as e:
        print("Exception when calling AccountingApi->getBankTransactionsHistory: %s\n" % e)

def main():
    # Call the function that retrieves the bank transactions history
    accounting_get_bank_transactions_history()

if __name__ == "__main__":
    main()