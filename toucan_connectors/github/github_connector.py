import json

import pandas as pd
import requests
from pydantic import Field, constr
from pandas import json_normalize
from typing import Dict, List, Any

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class GithubDataSource(ToucanDataSource):
    query: constr(min_length=1) = Field(
        ..., description='You can write your graphQL query here', widget='graphql'
    )
    mapping: List[List[str]] = Field(
        [],
        description="the mapping in the response json. Used to flatten the response. "
                    "Should point to an array in the json. You can have more than one"
    )


class GithubConnector(ToucanConnector):
    data_source_model: GithubDataSource

    graphql_endpoint: str = Field(
        default='https://api.github.com/graphql',
        description='The graphql endpoint for github',
    )
    token: str = Field(
        None,
        description='A token that has read access',
    )

    def _retrieve_data(self, data_source: GithubDataSource) -> pd.DataFrame:
        result = requests.post("https://api.github.com/graphql",
                               json={"query": data_source.query}, headers={"Authorization": "bearer " + self.token})
        df = pd.json_normalize(data=result.json(), record_path=data_source.mapping)
        return df

