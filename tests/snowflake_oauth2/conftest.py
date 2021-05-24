import pytest

from toucan_connectors.snowflake_oauth2.snowflake_oauth2_connector import (
    SnowflakeoAuth2Connector,
    SnowflakeoAuth2DataSource,
)


@pytest.fixture
def snowflake_oauth2_connector(secrets_keeper):
    return SnowflakeoAuth2Connector(
        name='test',
        account='acc',
        client_id='clientid',
        client_secret='clientsecret',
        scope='all:your:data',
        token_url='https://acc.token',
        secrets_keeper=secrets_keeper,
        role='testrole',
        default_warehouse='aqualy',
    )


@pytest.fixture
def snowflake_data_source():
    return SnowflakeoAuth2DataSource(
        name='test_name',
        domain='test_domain',
        database='test_database',
        warehouse='test_warehouse',
        query='test_query with %(foo)s and %(pokemon)s',
    )
