import pytest

from toucan_connectors.snowflake import SnowflakeDataSource
from toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector import SnowflakeoAuth2Connector


@pytest.fixture
def snowflake_oauth2_connector(secrets_keeper):
    return SnowflakeoAuth2Connector(
        name='test',
        account='acc',
        client_id='clientid',
        client_secret='clientsecret',
        authorization_url='https://foo.bar/laputa/oauth/redirect',
        scope='all:your:data',
        token_url='https://acc.token',
        secrets_keeper=secrets_keeper,
        role='testrole',
    )


@pytest.fixture
def snowflake_data_source():
    return SnowflakeDataSource(
        name='test_name',
        domain='test_domain',
        database='test_database',
        warehouse='test_warehouse',
        query='test_query with %(foo)s and %(pokemon)s',
    )
