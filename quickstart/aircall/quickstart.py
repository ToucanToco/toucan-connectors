"""
Use this file to initiate the OAuth2 dance and test the connectors that requires it.
"""

from helpers import JsonFileSecretsKeeper, get_authorization_response

from toucan_connectors.aircall.aircall_connector import AircallConnector, AircallDataSource

# Get these info from the provider
CLIENT_ID = ''
CLIENT_SECRET = ''
# ...and give this one to the provider
REDIRECT_URI = ''

aircall_conn = AircallConnector(
    name='test',
    auth_flow_id='test',
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    secrets_keeper=JsonFileSecretsKeeper(filename='secrets_aircall.json'),
)
sample_data_source_ss = AircallDataSource(
    name='test', domain='test-connector', limit=10, dataset='calls'
)

# The OAuth2 authorization process
# Manually rewrite to localhost:80/....
authorization_response = get_authorization_response(
    aircall_conn.build_authorization_url(), 'localhost', 35000
)
# Here we edited our /etc/hosts file to redirect the uri given by aircall to our localhost
aircall_conn.retrieve_tokens(authorization_response.replace('http', 'https').replace(':35000', ''))

# The actual data request
# Print the retrieved Token, thus ending the oAuth Dance
print('retrieved aircall token %s' % aircall_conn.get_access_token())
