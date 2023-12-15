import pyhdb
from pydantic import Field, StringConstraints
from typing_extensions import Annotated

from toucan_connectors.common import pandas_read_sql
from toucan_connectors.toucan_connector import PlainJsonSecretStr, ToucanConnector, ToucanDataSource


class SapHanaDataSource(ToucanDataSource):
    query: Annotated[str, StringConstraints(min_length=1)] = Field(
        ..., description="You can write your SQL query here", widget="sql"
    )


class SapHanaConnector(ToucanConnector, data_source_model=SapHanaDataSource):
    """
    Import data from Sap Hana.
    """

    host: str = Field(
        ...,
        description="The domain name (preferred option as more dynamic) or "
        "the hardcoded IP address of your database server",
    )

    port: int = Field(..., description="The listening port of your database server")
    user: str = Field(..., description="Your login username")
    password: PlainJsonSecretStr = Field("", description="Your login password")

    def _retrieve_data(self, data_source):
        connection = pyhdb.connect(
            self.host,
            self.port,
            self.user,
            self.password.get_secret_value() if self.password else PlainJsonSecretStr("").get_secret_value(),
        )

        df = pandas_read_sql(data_source.query, con=connection)

        connection.close()

        return df
