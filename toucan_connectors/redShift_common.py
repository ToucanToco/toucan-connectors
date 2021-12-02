from typing import Dict

from pydantic import Field, SecretStr

from toucan_connectors import ToucanConnector, ToucanDataSource
from enum import Enum


class RsDataSource(ToucanDataSource):
    database: str = Field(..., description='The name of the database you want to query')

    query: Dict = Field(None, description='An object describing a simple select query')


class RedshiftConnectorDbAuth(ToucanConnector):
    data_source_model: RsDataSource

    dbname: str = Field(..., description='The database name.')
    user: str = Field(..., description='Your login username.')
    password: SecretStr = Field(None, description='Your login password')
    host: str = Field(None, description='IP address or hostname.')
    port: int = Field(..., description='port value of 5439 is specified by default')
    connect_timeout: int = Field(
        None,
        title='Connection timeout',
        description='You can set a connection timeout in seconds here, i.e. the maximum length of '
        'time you want to wait for the server to respond. None by default',
    )
    cluster_identifier: str = Field(..., description='Name of the cluster')


class AuthenticationMethod(str, Enum):
    DB: str = 'Database credentials'
    IAM: str = 'IAM Credentials'
    PROFILE: str = 'Authentication Profile'
    IDP: str = 'Identity Provider'
    AWS: str = 'access secret key'
