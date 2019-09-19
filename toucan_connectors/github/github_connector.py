import json
import requests
import pandas as pd

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from toucan_connectors.auth import Auth


class GithubDataSource(ToucanDataSource):
    query: dict
    github_graphql_api_url: str = 'https://api.github.com/graphql'


class GithubConnector(ToucanConnector):
    data_source_model: GithubDataSource

    auth: Auth

    def _retrieve_data(self, data_source: GithubDataSource) -> pd.DataFrame:
        result = requests.post(
            url=data_source.github_graphql_api_url,
            auth=self.auth,
            data=json.dumps(data_source.query)
        )
        try:
            data = result.json()
        except ValueError:
            GithubConnector.logger.error(f'Could not decode "{result.content}"')
            raise
        return pd.DataFrame(data)


