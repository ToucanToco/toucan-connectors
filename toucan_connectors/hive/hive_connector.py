from enum import Enum
from typing import Optional

import pandas as pd
from pydantic import Field, SecretStr, constr
from pyhive import hive

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class HiveDataSource(ToucanDataSource):
    database: str = 'default'
    query: constr(min_length=1) = Field(..., widget='sql')


class AuthType(str, Enum):
    NONE = 'NONE'
    BASIC = 'BASIC'
    NOSASL = 'NOSASL'
    KERBEROS = 'KERBEROS'
    LDAP = 'LDAP'
    CUSTOM = 'CUSTOM'


class HiveConnector(ToucanConnector):
    data_source_model: HiveDataSource

    host: str
    port: int = 10000
    auth: AuthType = AuthType.NONE
    configuration: Optional[dict] = Field(None, description='A dictionary of Hive settings')
    kerberos_service_name: Optional[str] = Field(None, description="Use with auth='KERBEROS' only")
    username: Optional[str] = None
    password: Optional[SecretStr] = Field(
        None, description="Use with auth='LDAP' or auth='CUSTOM' only"
    )

    def _retrieve_data(self, data_source: HiveDataSource) -> pd.DataFrame:
        cursor = hive.connect(
            host=self.host,
            port=self.port,
            username=self.username,
            database=data_source.database,
            auth=self.auth,
            configuration=self.configuration,
            kerberos_service_name=self.kerberos_service_name,
            password=self.password.get_secret_value() if self.password else None,
        ).cursor()
        cursor.execute(data_source.query, parameters=data_source.parameters)
        columns = [metadata[0] for metadata in cursor.description]
        return pd.DataFrame.from_records(cursor.fetchall(), columns=columns)
