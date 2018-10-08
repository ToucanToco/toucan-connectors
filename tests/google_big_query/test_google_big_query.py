import json

import pandas as pd

from toucan_connectors.google_big_query.google_big_query_connector import (
    GoogleBigQueryConnector, GoogleBigQueryDataSource
)


def test_gbq(mocker):
    my_read_gbq = mocker.patch('pandas_gbq.read_gbq')
    my_read_gbq.return_value = mydf = pd.DataFrame({'a': [1, 1], 'b': [2, 2]})
    my_authentication = {
        'type': 'my_type',
        'project_id': 'my_project',
        'private_key_id': 'my_private_id',
        'private_key': 'my_private_key',
        'client_email': 'my_email',
        'client_id': 'my_id',
        'auth_uri': 'my_auth',
        'token_uri': 'my_token',
        'auth_provider_x509_cert_url': 'my_provider',
        'client_x509_cert_url': 'my_cert'
    }
    connector = GoogleBigQueryConnector(
        name="MyGBQ",
        authentication=my_authentication
    )
    datasource = GoogleBigQueryDataSource(
        name='MyGBQ',
        domain='wiki',
        query='SELECT * FROM [bigquery-public-data:samples.wikipedia] LIMIT 1000'
    )
    assert connector.get_df(datasource).equals(mydf)
    args, kwargs = my_read_gbq.call_args
    assert kwargs == {
        'query': 'SELECT * FROM [bigquery-public-data:samples.wikipedia] LIMIT 1000',
        'project_id': 'my_project',
        'private_key': json.dumps(my_authentication),
        'reauth': True,
        'dialect': 'legacy'
    }
