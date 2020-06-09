import pandas as pd
import requests
import json
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
import os
from pydantic import Field

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

class GithubDataSource(ToucanDataSource):
    """ This class is our ToucanDataSource implementation for the github data source
    it instanciates with a repo name from where to pull the info, an owner name to whom
    the repo belongs to and a dict containing the query to send to the Github GraphQL API"""
    repo_name: str = Field(..., description='The name of the repo you want to query')
    owner: str = Field(..., description='The name of the owner of the repo you want to query')
    query: dict = Field(..., description='The query to send to Github API')


class GithubConnector(ToucanConnector):
    """ This class is our ToucanConnector implementation to handle the connection with Github's API
    and retrieve the data. It instanciates with a GithubDataSource object and mandatory token to
    be able to call the API"""
    data_source_model: GithubDataSource
    bearer_auth_token: str = Field(..., description='Personal Token')

    def _retrieve_data(self, data_source: GithubDataSource) -> pd.DataFrame:
        """This function is responsible of calling the API with the given Token
        and retrieving the data based on the result of the given query. 
        It implements the abstract method of the ToucanConnector class, takes 
        a GithubDataSource in argument and return and pandas Dataframe if everything
        went well"""
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
        
