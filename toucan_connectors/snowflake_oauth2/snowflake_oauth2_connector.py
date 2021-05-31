from enum import Enum
from typing import Any, Dict, List, Type

import pandas as pd
import snowflake as pysnowflake
from pydantic import Field, SecretStr
from snowflake.connector import DictCursor

from toucan_connectors.oauth2_connector.oauth2connector import (
    OAuth2Connector,
    OAuth2ConnectorConfig,
)
from toucan_connectors.snowflake.snowflake_connector import (
    AuthenticationMethod,
    SnowflakeDataSource,
)
from toucan_connectors.toucan_connector import Category, ToucanConnector


class SnowflakeoAuth2DataSource(SnowflakeDataSource):
    """ """


class SnowflakeRoleAvailable(str, Enum):
    ACCOUNTADMIN: str = 'ACCOUNTADMIN'
    SECURITYADMIN: str = 'SECURITYADMIN'
    USERADMIN: str = 'USERADMIN'
    SYSADMIN: str = 'SYSADMIN'
    PUBLIC: str = 'PUBLIC'


class SnowflakeScopeAvailable(str, Enum):
    refresh_token: str = 'refresh_token'
    ACCOUNTADMIN: str = 'session:role:ACCOUNTADMIN'
    SECURITYADMIN: str = 'session:role:SECURITYADMIN'
    USERADMIN: str = 'session:role:USERADMIN'
    SYSADMIN: str = 'session:role:SYSADMIN'
    PUBLIC: str = 'session:role:PUBLIC'


class SnowflakeoAuth2Connector(ToucanConnector):
    _auth_flow = 'oauth2'
    _oauth_trigger = 'connector'
    data_source_model: SnowflakeoAuth2DataSource

    token_url: str = Field(None, **{'hidden': True})
    auth_flow_id: str = Field(None, **{'hidden': True})
    oauth2_version = Field('1', **{'hidden': True})
    authorization_url: str = Field(None, **{'hidden': True})
    redirect_uri: str = Field(None, **{'hidden': True})
    category: Category = Field(Category.SNOWFLAKE, **{'hidden': True})

    info_step1: str = Field(
        '''<div style="width: 100%; padding: 10px; background-color: #2a66a1;">Step 1<br />Please fill connector name</div>''',
        title='step_1',
        widget='info',
    )

    info_step2: str = Field(
        '''<div style="width: 100%; padding: 10px; background-color: #2a66a1;">Step 2<br />Play this request in Snowflake<br />
        'create security integration toucan_oauth2_{{name}}<br />
        '<span style="color: red;">
        'type = oauth<br />
        'enabled = true<br />
        'oauth_client = custom<br />
        'oauth_client_type = 'CONFIDENTIAL'<br />
        'oauth_redirect_uri = 'https://localhost:5000/tttt/oauth/redirect?connector_name={{name}}'<br />
        'oauth_issue_refresh_tokens = true<br />
        'oauth_allow_non_tls_redirect_uri = true<br />
        'oauth_refresh_token_validity = 86400<br />
        'pre_authorized_roles_list = ('PUBLIC');<br />
        '</span>
        '<br />
        'If you update your connector name, play this request<br />
        '<span style="color: red;">
        'alter security integration toucan_oauth2_{{name}} set oauth_redirect_uri = 'http://localhost:5000/fbb-snowflake/oauth/redirect?connector_name={{name}}';
        '</span>
        '</div>''',
        title='step_2',
        widget='info',
        **{'watch_field': ['name']},
    )

    info_step3: str = Field(
        '''<div style="width: 100%; padding: 10px; background-color: #2a66a1;">Step 2<br />
        Get your client_id and client_secret with request<br />
        <span style="color: red;">
        select system$show_oauth_client_secrets('toucan_oauth2_{{name}}');
        </span>
        </div>''',
        title='step_3',
        widget='info',
        **{'watch_field': ['name']},
    )

    client_id: str = Field(
        ...,
        title='Client ID',
        description='The client id of you Snowflake integration',
        **{'ui.required': True},
        required_label=True,
    )
    client_secret: SecretStr = Field(
        ...,
        title='Client Secret',
        description='The client secret of your Snowflake integration',
        **{'ui.required': True},
        required_label=True,
    )
    scope: SnowflakeScopeAvailable = Field(
        None, title='Scope', description='The scope the integration', placeholder='refresh_token'
    )
    role: SnowflakeRoleAvailable = Field(
        SnowflakeRoleAvailable.PUBLIC,
        title='Snowflake Role',
        description='Role to use for queries',
        **{
            'ui': {
                'checkbox': False,
                'required': True,
            }
        },
        required_label=True,
    )
    account: str = Field(
        ...,
        title='Account',
        description='The full name of your Snowflake account. '
        'It might require the region and cloud platform where your account is located, '
        'in the form of: "your_account_name.region_id.cloud_platform". See more details '
        '<a href="https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#label-account-format-info" target="_blank">here</a>.',
        **{'placeholder': 'your_account_name.region_id.cloud_platform', 'ui': {'required': True}},
        required_label=True,
    )
    default_warehouse: str = Field(
        ...,
        title="Default warehouse",
        description='The default warehouse that shall be used for any data source',
        **{'ui.required': True},
        required_label=True,
    )

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type['SnowflakeoAuth2Connector']) -> None:
            ordered_keys = [
                'type',
                'info_step1',
                'name',
                'info_step2',
                'account',
                'info_step3',
                'client_id',
                'client_secret',
                'role',
                'scope',
                'default_warehouse',
                'retry_policy',
                'category',
                'token_url',
                'auth_flow_id',
                'oauth2_version',
                'authorization_url',
                'redirect_uri',
            ]
            schema['properties'] = {k: schema['properties'][k] for k in ordered_keys}

    def __init__(self, **kwargs):
        super().__init__(**{k: v for k, v in kwargs.items() if k != 'secrets_keeper'})
        self.token_url = f'https://{self.account}.snowflakecomputing.com/oauth/token-request'
        self.authorization_url = f'https://{self.account}.snowflakecomputing.com/oauth/authorize'
        self.__dict__['_oauth2_connector'] = OAuth2Connector(
            auth_flow_id=self.auth_flow_id,
            authorization_url=self.authorization_url,
            scope=self.scope,
            token_url=self.token_url,
            redirect_uri=self.redirect_uri,
            config=OAuth2ConnectorConfig(
                client_id=self.client_id,
                client_secret=self.client_secret,
            ),
            secrets_keeper=kwargs['secrets_keeper'],
        )

    def build_authorization_url(self, **kwargs):
        return self.__dict__['_oauth2_connector'].build_authorization_url(**kwargs)

    def retrieve_tokens(self, authorization_response: str):
        return self.__dict__['_oauth2_connector'].retrieve_tokens(authorization_response)

    def _get_warehouses(self) -> List[str]:
        with self.connect() as connection:
            return [
                warehouse['name']
                for warehouse in connection.cursor(DictCursor).execute('SHOW WAREHOUSES').fetchall()
                if 'name' in warehouse
            ]

    def get_access_token(self):
        return self.__dict__['_oauth2_connector'].get_access_token()

    def connect(self, database=None, warehouse=None) -> pysnowflake.connector.SnowflakeConnection:
        connection_params = {
            'account': self.account,
            'authenticator': AuthenticationMethod.OAUTH,
            'application': 'ToucanToco',
            'token': self.get_access_token(),
            'role': self.role if self.role else '',
        }
        return pysnowflake.connector.connect(
            **connection_params, database=database, warehouse=warehouse
        )

    def _retrieve_data(self, data_source: SnowflakeoAuth2DataSource) -> pd.DataFrame:
        connection = self.connect(
            database=data_source.database,
            warehouse=data_source.warehouse,
        )
        cursor = connection.cursor(DictCursor)
        query_res = cursor.execute(data_source.query)
        df = pd.DataFrame(query_res.fetchall())
        connection.close()

        return df
