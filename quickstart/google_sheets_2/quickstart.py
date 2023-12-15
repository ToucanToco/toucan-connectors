"""
Use this file to initiate the OAuth2 dance and test the connectors that requires it.
"""

from toucan_connectors.google_sheets_2.google_sheets_2_connector import (
    GoogleSheets2Connector,
    GoogleSheets2DataSource,
)

from .helpers import JsonFileSecretsKeeper, get_authorization_response

# Get these info from the provider
CLIENT_ID = ''
CLIENT_SECRET = ''
# ...and give this one to the provider
REDIRECT_URI = 'http://localhost:34097/'

google_sheets_conn = GoogleSheets2Connector(
    name='test',
    auth_flow_id='test',
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    secrets_keeper=JsonFileSecretsKeeper(filename='secrets.json'),
)
sample_data_source_ss = GoogleSheets2DataSource(
    name='test',
    domain='test-connector',
    spreadsheet_id='1L5YraXEToFv7p0HMke7gXI4IhJotdT0q5bk_PInI1hA',
)

# The OAuth2 authorization process
authorization_response = get_authorization_response(google_sheets_conn.build_authorization_url(), 'localhost', 34097)
google_sheets_conn.retrieve_tokens(authorization_response)

# The actual data request
df = google_sheets_conn.get_df(data_source=sample_data_source_ss)
print(df)
