"""
Use this file to initiate the OAuth2 dance and test the connectors that requires it.
"""

from toucan_connectors.github.github_connector import (
    GithubConnector,
    GithubDataSource,
)

from helpers import JsonFileSecretsKeeper, get_authorization_response

# Get these info from the provider
CLIENT_ID = ''
CLIENT_SECRET = ''
# ...and give this one to the provider
REDIRECT_URI = 'http://localhost:34097/'

github_conn = GithubConnector(
    name='test',
    auth_flow_id='test',
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
    secrets_keeper=JsonFileSecretsKeeper(filename='secrets.json'),
)

# The OAuth2 authorization process
authorization_response = get_authorization_response(
    github_conn.build_authorization_url(), 'localhost', 34097
)

github_conn.retrieve_tokens(authorization_response)
print('retrieved github token %s' % github_conn.get_access_token())