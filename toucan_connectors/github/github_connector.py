import pandas as pd
import requests
import json
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
import os
from pydantic import Field

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

class GithubDataSource(ToucanDataSource):
    repo_name: str = Field(..., description='The name of the repo you want to query')
    owner: str = Field(..., description='The name of the repo you want to query')
    query: dict = Field(..., description='The query to send to Github API')


class GithubConnector(ToucanConnector):
    data_source_model: GithubDataSource
    bearer_auth_token: str = Field(..., description='Personal Token')

    def _retrieve_data(self, data_source: GithubDataSource) -> pd.DataFrame:
        headers = {'Authorization' : 'token %s' % self.bearer_auth_token}
        endpoint = "https://api.github.com/graphql"
        response = requests.post(endpoint, headers=headers, 
            json=data_source.query)

        if response.status_code == 200:
            res = response.json()
            if 'data' in res.keys():
                """ Here I define what's done with the result
                It only tries to get title and update_date of PR
                based on predifined keys
                In a real connector additional logic will be needed
                to parse the result"""
                df = pd.DataFrame.from_dict([{'title':node['node']['title'],
                'updatedAt':node['node']['updatedAt']} \
                for node in res['data']['repository']['pullRequests']['edges']])
                return df
            else:
                raise Exception("Invalid Query")
        elif response.status_code == 401:
            raise Exception("Query Failed, Token is wrong")
        
