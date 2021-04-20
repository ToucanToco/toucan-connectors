from contextlib import suppress
from datetime import datetime
from enum import Enum
from os import path
from typing import Dict, List

import jwt
import pandas as pd
import requests
import snowflake.connector
from jinja2 import Template
from pydantic import Field, SecretStr, constr, create_model
from snowflake.connector import DictCursor

from toucan_connectors.common import ConnectorStatus, convert_to_qmark_paramstyle
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource, strlist_to_enum


class Path(str):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not path.exists(v):
            raise ValueError(f'path does not exists: {v}')
        return v


class SnowflakeConnectorException(Exception):
    """Raised when something wrong happened in a snowflake context"""


class SnowflakeDataSource(ToucanDataSource):
    database: str = Field(..., description='The name of the database you want to query')
    warehouse: str = Field(None, description='The name of the warehouse you want to query')
    query: constr(min_length=1) = Field(
        ..., description='You can write your SQL query here', widget='sql'
    )

    @classmethod
    def _get_databases(cls, connector: 'SnowflakeConnector'):
        # FIXME: Maybe use a generator instead of a list here?
        with connector.connect() as connection:
            return [
                db['name']
                # Fetch rows as dicts with column names as keys
                for db in connection.cursor(DictCursor).execute('SHOW DATABASES').fetchall()
                if 'name' in db
            ]

    @classmethod
    def get_form(cls, connector: 'SnowflakeConnector', current_config):
        constraints = {}

        with suppress(Exception):
            databases = cls._get_databases(connector)
            warehouses = connector._get_warehouses()
            # Restrict some fields to lists of existing counterparts
            constraints['database'] = strlist_to_enum('database', databases)
            constraints['warehouse'] = strlist_to_enum('warehouse', warehouses)

        res = create_model('FormSchema', **constraints, __base__=cls).schema()
        res['properties']['warehouse']['default'] = connector.default_warehouse
        return res


class AuthenticationMethod(str, Enum):
    PLAIN: str = 'snowflake'
    OAUTH: str = 'oauth'


class SnowflakeConnector(ToucanConnector):
    """
    Import data from Snowflake data warehouse.
    """

    data_source_model: SnowflakeDataSource

    authentication_method: AuthenticationMethod = Field(
        None,
        title='Authentication Method',
        description='The authentication mechanism that will be used to connect to your snowflake data source',
    )

    user: str = Field(..., description='Your login username')
    password: SecretStr = Field(None, description='Your login password')
    oauth_token: str = Field(None, description='Your oauth token')
    oauth_args: dict = Field(None, description='Named arguments for an OIDC auth')
    account: str = Field(
        ...,
        description='The full name of your Snowflake account. '
        'It might require the region and cloud platform where your account is located, '
        'in the form of: "your_account_name.region_id.cloud_platform". See more details '
        '<a href="https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#label-account-format-info" target="_blank">here</a>.',
    )
    role: str = Field(
        None,
        description='The user role that you want to connect with. '
        'See more details <a href="https://docs.snowflake.com/en/user-guide/admin-user-management.html#user-roles" target="_blank">here</a>.',
    )

    default_warehouse: str = Field(
        ..., description='The default warehouse that shall be used for any data source'
    )
    ocsp_response_cache_filename: Path = Field(
        None,
        title='OCSP response cache filename',
        description='The path of the '
        '<a href="https://docs.snowflake.net/manuals/user-guide/python-connector-example.html#caching-ocsp-responses" target="_blank">OCSP cache file</a>',
    )

    def get_status(self) -> ConnectorStatus:
        error = None
        status = True

        try:
            with self.connect(login_timeout=5) as connection:
                cursor = connection.cursor()
                self._execute_query(cursor, 'SHOW WAREHOUSES', {})
        except snowflake.connector.errors.ProgrammingError as e:
            status = False
            error = str(e)
        except snowflake.connector.errors.DatabaseError:
            status = False
            error = f"Connection failed for the user '{self.user}', please check your credentials"

        connector_status = ConnectorStatus(
            status=status,
        )
        if error is not None:
            connector_status.error = error
        return connector_status

    def get_connection_params(self):
        res = {
            'user': Template(self.user).render(),
            'account': self.account,
            'authenticator': self.authentication_method,
            'application': 'ToucanToco',
        }

        if not self.authentication_method:
            # Default to User/Password authentication method if the parameter
            # was not set when the connector was created
            res['authenticator'] = AuthenticationMethod.PLAIN

        if res['authenticator'] == AuthenticationMethod.PLAIN and self.password:
            res['password'] = self.password.get_secret_value()

        if self.authentication_method == AuthenticationMethod.OAUTH:
            if self.oauth_token is not None:
                res['token'] = Template(self.oauth_token).render()

        if self.role != '':
            res['role'] = self.role

        return res

    def _refresh_oauth_token(self):
        """Regenerates an oauth token if configuration was provided and if the given token has expired."""
        if 'token_endpoint' in self.oauth_args and 'refresh_token' in self.oauth_args:
            access_token = jwt.decode(
                Template(self.oauth_token).render(),
                verify=False,
                options={'verify_signature': False},
            )
            if datetime.fromtimestamp(access_token['exp']) < datetime.now():
                content_type = 'application/json'
                # Content-Type may need to be overridden
                if 'content_type' in self.oauth_args:
                    content_type = self.oauth_args['content_type']

                res = requests.post(
                    Template(self.oauth_args['token_endpoint']).render(),
                    data={
                        'grant_type': 'refresh_token',
                        'client_id': Template(self.oauth_args['client_id']).render(),
                        'client_secret': Template(self.oauth_args['client_secret']).render(),
                        'refresh_token': Template(self.oauth_args['refresh_token']).render(),
                    },
                    headers={'Content-Type': content_type},
                )

                # Check if the request has a status_code equals to 200 and raise if not
                # https://github.com/psf/requests/blob/master/requests/models.py#L936-L943
                if not res.ok:  # pragma: no cover
                    res.raise_for_status()

                self.oauth_token = res.json().get('access_token')

    def connect(self, **kwargs) -> snowflake.connector.SnowflakeConnection:
        # This needs to be set before we connect
        snowflake.connector.paramstyle = 'qmark'
        if self.oauth_args and self.authentication_method == AuthenticationMethod.OAUTH:
            self._refresh_oauth_token()
        connection_params = self.get_connection_params()

        return snowflake.connector.connect(**connection_params, **kwargs)

    def _get_warehouses(self) -> List[str]:
        with self.connect() as connection:
            return [
                warehouse['name']
                for warehouse in connection.cursor(DictCursor).execute('SHOW WAREHOUSES').fetchall()
                if 'name' in warehouse
            ]

    def _execute_query(self, cursor, query: str, query_parameters: Dict):
        """Executes `query` against Snowflake's client and retrieves the
        results.

        Args:
            cursor (SnowflakeCursor): A snowflake database cursor
            query (str): The query that will be executed against a database
            query_parameters (Dict): The eventual parameters that will be applied to `query`

        Returns: A pandas DataFrame
        """
        # Prevent error with dict and array values in the parameter object
        converted_query, ordered_values = convert_to_qmark_paramstyle(query, query_parameters)
        query_res = cursor.execute(converted_query, ordered_values)

        # https://docs.snowflake.com/en/user-guide/python-connector-api.html#fetch_pandas_all
        # `fetch_pandas_all` will only work with `SELECT` queries, if the
        # query does not contains 'SELECT' then we're defaulting to the usual
        # `fetchall`.
        if 'SELECT' in query.upper():
            return query_res.fetch_pandas_all()
        else:
            return pd.DataFrame.from_dict(query_res.fetchall())

    def _retrieve_data(self, data_source: SnowflakeDataSource) -> pd.DataFrame:
        warehouse = data_source.warehouse
        # Default to default warehouse if not specified in `data_source`
        if self.default_warehouse and not warehouse:
            warehouse = self.default_warehouse

        connection = self.connect(
            database=Template(data_source.database).render(),
            warehouse=Template(warehouse).render(),
            ocsp_response_cache_filename=self.ocsp_response_cache_filename,
        )
        cursor = connection.cursor(DictCursor)

        df = self._execute_query(cursor, data_source.query, data_source.parameters)

        connection.close()

        return df
