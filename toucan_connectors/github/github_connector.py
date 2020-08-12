import pandas as pd
from pydantic import Field, constr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class GithubDataSource(ToucanDataSource):
    query: constr(min_length=1) = Field(
        ..., description='You can write your SQL query here', widget='graphql'
    )


class GithubConnector(ToucanConnector):
    data_source_model: GithubDataSource

    graphql_endpoint: str = Field(
        None,
        default='https://api.github.com/graphql',
        description='The graphql endpoint for github',
    )
    token: str = Field(
        None,
        description='A token that has read access',
    )

    def _retrieve_data(self, data_source: GithubDataSource) -> pd.DataFrame:
        pass

