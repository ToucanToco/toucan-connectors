import jwt
import pytest
from mock import patch, Mock
from pandas import DataFrame

from pydantic import SecretStr
from toucan_connectors.common import ConnectorStatus
from toucan_connectors.snowflake import (
    AuthenticationMethod,
    AuthenticationMethodValue,
    SnowflakeConnector,
    SnowflakeDataSource,
)
import pandas as pd
from toucan_connectors.snowflake_common import SnowflakeCommon

OAUTH_TOKEN_ENDPOINT = 'http://example.com/endpoint'
OAUTH_TOKEN_ENDPOINT_CONTENT_TYPE = 'application/x-www-form-urlencoded'
OAUTH_ACCESS_TOKEN = jwt.encode({'exp': 42, 'sub': 'snowflake_user'}, key='clef')
OAUTH_REFRESH_TOKEN = 'baba au rhum'
OAUTH_CLIENT_ID = 'client_id'
OAUTH_CLIENT_SECRET = 'client_s3cr3t'


sc = SnowflakeConnector(
    identifier='snowflake_test',
    name='test_name',
    authentication_method=AuthenticationMethod.PLAIN,
    user='test_user',
    password='test_password',
    account='test_account',
    default_warehouse='warehouse_1'
)

sc_oauth = SnowflakeConnector(
    identifier='snowflake_test',
    name='test_name',
    authentication_method=AuthenticationMethod.OAUTH,
    user='test_user',
    password='test_password',
    account='test_account',
    token_endpoint=OAUTH_TOKEN_ENDPOINT,
    token_endpoint_content_type=OAUTH_TOKEN_ENDPOINT_CONTENT_TYPE,
    default_warehouse='warehouse_1'
)

sd = SnowflakeDataSource(
    name='test_name',
    domain='test_domain',
    database='database_1',
    warehouse='warehouse_1',
    query='test_query with %(foo)s and %(pokemon)s',
    parameters={'foo': 'bar', 'pokemon': 'pikachu'},
)


@pytest.fixture
def snowflake_connector_oauth(mocker):
    user_tokens_keeper = mocker.Mock(
        access_token=SecretStr(OAUTH_ACCESS_TOKEN),
        refresh_token=SecretStr(OAUTH_REFRESH_TOKEN),
        update_tokens=mocker.Mock()
    )
    sso_credentials_keeper = mocker.Mock(
        client_id=OAUTH_CLIENT_ID,
        client_secret=SecretStr(OAUTH_CLIENT_SECRET)
    )
    return SnowflakeConnector(
        name='test_name',
        authentication_method=AuthenticationMethod.OAUTH,
        user='test_user',
        password='test_password',
        account='test_account',
        token_endpoint=OAUTH_TOKEN_ENDPOINT,
        token_endpoint_content_type=OAUTH_TOKEN_ENDPOINT_CONTENT_TYPE,
        user_tokens_keeper=user_tokens_keeper,
        sso_credentials_keeper=sso_credentials_keeper,
        default_warehouse='default_wh'
    )


@pytest.fixture
def snowflake_connector():
    return SnowflakeConnector(
        identifier='snowflake_test',
        name='test_name',
        authentication_method=AuthenticationMethod.PLAIN,
        user='test_user',
        password='test_password',
        account='test_account',
        default_warehouse='default_wh'
    )


@pytest.fixture
def snowflake_datasource():
    return SnowflakeDataSource(
        name='test_name',
        domain='test_domain',
        database='database_1',
        warehouse='warehouse_1',
        query='SELECT * FROM table WHERE %(key) = %(value)',
        parameters={'key': 'column', 'value': 'tortank'}
    )


@pytest.fixture
def data_frame():
    data = {
        'First Column Name': ['First value', 'Second value', ...],
        'Second Column Name': ['First value', 'Second value', ...]
    }
    df = pd.DataFrame(data, columns=['First Column Name', 'Second Column Name', ...])
    return df


# @patch('toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._get_connection', return_value={'success': True})
# @patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_databases', return_value=['database_1', 'database_2'])
# @patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_databases', return_value=[])
# @patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses', return_value=['warehouse_1', 'warehouse_2'])
# @patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses', return_value=[])
# @patch('toucan_connectors.snowflake_common.SnowflakeCommon.__execute_query', return_value=df)


def test_set_warehouse(mocker, snowflake_connector, snowflake_datasource):
    sd.warehouse = None
    new_data_source = sc._set_warehouse(sd)
    assert new_data_source.warehouse == 'warehouse_1'


# TODO - Que fait on lorsqu'il n'y a pas de default warehouse ? Les requêtes sont quand même exécutées ?
def test_set_warehouse_without_default_warehouse(mocker, snowflake_connector, snowflake_datasource):
    sc_without_default_warehouse = SnowflakeConnector(
        identifier='snowflake_test',
        name='test_name',
        authentication_method=AuthenticationMethod.PLAIN,
        user='test_user',
        password='test_password',
        account='test_account'
    )
    sd.warehouse = None
    new_data_source = sc_without_default_warehouse._set_warehouse(sd)
    assert new_data_source.warehouse is None


@patch('toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._get_connection', return_value={'success': True})
@patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses', return_value=['warehouse_1'])
def test_get_status(mocker, snowflake_connector):
    connector_status = sc.get_status()
    assert connector_status.status


@patch('toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._get_connection', return_value={'success': True})
@patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses', return_value=[])
def test_get_status_without_warehouses(mocker, snowflake_connector):
    connector_status = sc.get_status()
    assert not connector_status.status


@patch('requests.post')
def test_refresh_oauth_token(mocked_post, snowflake_connector):
    mocked_post.return_value = Mock(status_code=201, json=lambda: {"access_token": "token", "refresh_token": "token"})
    sc_oauth._refresh_oauth_token()
    assert sc_oauth.user_tokens_keeper == 'test'


@patch('toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._get_connection', return_value={'success': True})
@patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_databases', return_value=['database_1', 'database_2'])
def test_get_database_without_filter(mocker, snowflake_connector):
    result = sc._get_databases()
    assert result[0] == 'database_1'
    assert result[1] == 'database_2'
    assert result.__len__() == 2


@patch('toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._get_connection', return_value={'success': True})
@patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_databases', return_value=['database_1'])
def test_get_database_with_filter_found(mocker, snowflake_connector):
    result = sc._get_databases('database_1')
    assert result[0] == 'database_1'
    assert result.__len__() == 1


@patch('toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._get_connection', return_value={'success': True})
@patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_databases', return_value=[])
def test_get_database_with_filter_not_found(mocker, snowflake_connector):
    result = sc._get_databases('database_3')
    assert result.__len__() == 0


@patch('toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._get_connection', return_value={'success': True})
@patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses', return_value=['warehouse_1', 'warehouse_2'])
def test_get_warehouse_without_filter(mocker, snowflake_connector):
    result = sc._get_warehouses()
    assert result[0] == 'warehouse_1'
    assert result[1] == 'warehouse_2'


@patch('toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._get_connection', return_value={'success': True})
@patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses', return_value=['warehouse_1'])
def test_get_warehouse_with_filter_found(mocker, snowflake_connector):
    result = sc._get_warehouses('warehouse_1')
    assert result[0] == 'warehouse_1'
    assert result.__len__() == 1


@patch('toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._get_connection', return_value={'success': True})
@patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses', return_value=[])
def test_get_warehouse_with_filter_not_found(mocker, snowflake_connector):
    result = sc._get_warehouses('warehouse_3')
    assert result.__len__() == 0


@patch('toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._get_connection', return_value={'success': True})
@patch('toucan_connectors.snowflake_common.SnowflakeCommon._execute_query', return_value=data_frame)
def test_retrieve_data(mocker, snowflake_connector, snowflake_datasource):
    df_result: DataFrame = sc._retrieve_data(sd)
    # FIXME - result false
    # assert 3 == len(df_result)


@patch('toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._get_connection', return_value={'success': True})
@patch('toucan_connectors.snowflake_common.SnowflakeCommon._execute_query', return_value=data_frame)
def test_get_slice(mocker, snowflake_connector, snowflake_datasource):
    df_result: DataFrame = sc._retrieve_data(sd)
    # FIXME - result false
    # assert 3 == len(df_result)


@patch('toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._get_connection', return_value={'success': True})
@patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_warehouses', return_value=['warehouse_1', 'warehouse_2'])
@patch('toucan_connectors.snowflake_common.SnowflakeCommon.get_databases', return_value=['database_1', 'database_2'])
def test_get_form(mocker, snowflake_connector, snowflake_datasource):
    result = sd.get_form(sc, {})
    assert 'warehouse_1' == result['properties']['warehouse']['default']


@patch('snowflake.connector.connect', return_value={})
@patch('toucan_connectors.snowflake.snowflake_connector.SnowflakeConnector._refresh_oauth_token')
def test_get_connection_connect(mocker, snowflake_connector):
    conn = sc._get_connection('database_1', 'warehouse_1')
    conn = sc_oauth._get_connection('database_1', 'warehouse_1')
    mocker._refresh_oauth_token.assert_called_once()


