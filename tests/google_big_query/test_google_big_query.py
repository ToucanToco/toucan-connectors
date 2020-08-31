import pandas as pd
from google.oauth2.service_account import Credentials

from toucan_connectors.google_big_query.google_big_query_connector import (
    GoogleBigQueryConnector,
    GoogleBigQueryDataSource,
)
from toucan_connectors.google_credentials import GoogleCredentials


def test_gbq(mocker):
    my_read_gbq = mocker.patch('pandas_gbq.read_gbq')
    my_read_gbq.return_value = mydf = pd.DataFrame({'a': [1, 1], 'b': [2, 2]})
    mocker.patch('cryptography.hazmat.primitives.serialization.load_pem_private_key')
    my_credentials = GoogleCredentials(
        type='my_type',
        project_id='my_project_id',
        private_key_id='my_private_key_id',
        private_key='my_private_key',
        client_email='my_client_email',
        client_id='my_client_id',
        auth_uri='https://accounts.google.com/o/oauth2/auth',
        token_uri='https://oauth2.googleapis.com/token',
        auth_provider_x509_cert_url='https://www.googleapis.com/oauth2/v1/certs',
        client_x509_cert_url='https://www.googleapis.com/robot/v1/metadata/x509/pika.com',
    )
    connector = GoogleBigQueryConnector(
        name='MyGBQ',
        credentials=my_credentials,
        scopes=[
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/drive',
        ],
    )
    datasource = GoogleBigQueryDataSource(
        name='MyGBQ',
        domain='wiki',
        query='SELECT * FROM bigquery-public-data:samples.wikipedia LIMIT 1000',
    )
    assert connector.get_df(datasource).equals(mydf)

    args, kwargs = my_read_gbq.call_args
    credentials = kwargs.pop('credentials')
    assert isinstance(credentials, Credentials)
    assert credentials.scopes == [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/drive',
    ]
    assert kwargs == {
        'query': 'SELECT * FROM bigquery-public-data:samples.wikipedia LIMIT 1000',
        'project_id': 'my_project_id',
        'dialect': 'standard',
    }
