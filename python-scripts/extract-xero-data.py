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
