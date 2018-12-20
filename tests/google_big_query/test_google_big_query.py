import pandas as pd

from toucan_connectors.google_big_query.google_big_query_connector import (
    GoogleBigQueryConnector, GoogleBigQueryDataSource
)

from toucan_connectors.common import GoogleCredentials
from google.oauth2.service_account import Credentials


def test_gbq(mocker):
    my_read_gbq = mocker.patch('pandas_gbq.read_gbq')
    my_read_gbq.return_value = mydf = pd.DataFrame({'a': [1, 1], 'b': [2, 2]})
    mocker.patch("cryptography.hazmat.primitives.serialization.load_pem_private_key")
    my_credentials = GoogleCredentials(
        type='my_type',
        project_id='my_project_id',
        private_key_id='my_private_key_id',
        private_key='my_private_key',
        client_email='my_client_email',
        client_id='my_client_id',
        auth_uri='my_auth_uri',
        token_uri='my_token_uri',
        auth_provider_x509_cert_url='my_provider',
        client_x509_cert_url='my_cert'
    )
    connector = GoogleBigQueryConnector(
        name="MyGBQ",
        credentials=my_credentials,
        dialect='standard',
        scopes=["https://www.googleapis.com/auth/bigquery",
                'https://www.googleapis.com/auth/drive']
    )
    datasource = GoogleBigQueryDataSource(
        name='MyGBQ',
        domain='wiki',
        query='SELECT * FROM [bigquery-public-data:samples.wikipedia] LIMIT 1000'
    )
    assert connector.get_df(datasource).equals(mydf)
    args, kwargs = my_read_gbq.call_args
    assert kwargs['query'] == 'SELECT * FROM [bigquery-public-data:samples.wikipedia] LIMIT 1000'
    assert kwargs['project_id'] == 'my_project_id'
    assert kwargs['dialect'] == 'standard'
    assert isinstance(kwargs['credentials'], Credentials)
    assert kwargs['credentials'].scopes == ["https://www.googleapis.com/auth/bigquery",
                                            'https://www.googleapis.com/auth/drive']
