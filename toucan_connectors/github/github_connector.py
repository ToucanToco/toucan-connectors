from enum import Enum

import pandas as pd
import requests
from pydantic import Field

from toucan_connectors.toucan_connector import BaseModel, ToucanConnector


class State(Enum):
    OPEN = 'open'
    CLOSED = 'closed'
    ALL = 'all'


class GithubDataSource(BaseModel):
    owner: str = Field(
        ...,
        title='Owner of the GitHub repository (company name ...)',
        description='Can be found in the URL: https://github.com/<owner>/<repo>',
    )
    repo: str = Field(
        ...,
        title='Github Repository where we want to get PRs from',
        description='Can be found in the URL: https://github.com/<owner>/<repo>',
    )
    state: State = Field(..., title='State of the PRs we want to filter on')


class GithubConnector(ToucanConnector):
    data_source_model: GithubDataSource

    username: str = Field(..., title='GitHub username')
    personal_token: str = Field(
        ...,
        title='GitHub personal access token use to access to private repos',
        description='Can be set and found on this page https://github.com/settings/tokens. Scope must be `repo`',
    )

    def _retrieve_data(self, data_source: GithubDataSource) -> pd.DataFrame:
        url = 'https://api.github.com/repos/{}/{}/pulls'.format(data_source.owner, data_source.repo)
        if data_source.state:
            url += '?state={}'.format(data_source.state)

        pull_requests = []
        r = requests.get(url, auth=(self.username, self.personal_token))

        if not r.ok:
            return pd.DataFrame(pull_requests)

        for pr in r.json():
            pull_requests.append(
                {'url': pr['url'], 'id': pr['id'], 'title': pr['title'], 'state': pr['state']}
            )

        return pd.DataFrame(pull_requests)
