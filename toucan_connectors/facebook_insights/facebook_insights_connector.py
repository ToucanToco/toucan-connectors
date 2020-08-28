"""
Provide a facbook insights connector.

Check
https://developers.facebook.com/docs/graph-api/reference/v2.8/insights_result
for the official API documentation.

The connector assumes that you already have a list of *page tokens*. You can
fetch page tokens programmatically but you need a user shortlived token
beforehands anyway. For more information about tokens, check
https://developers.facebook.com/docs/facebook-login/access-tokens/

Here's a sample python script to fetch all page tokens available to a given user

```python
from toucan_connectors.facebook_insights_connector import get_longlived_token, get_page_tokens

shortlived_token = 'EAAFXzXZAivE4BAP8II1WucQgmNOYlZB...'
ll_token = get_longlived_token(shortlived_token, 'my-app-id', 'my-app-secret')
page_tokens = get_page_tokens(ll_token)
```
"""
from typing import Dict, List

import facebook
import pandas as pd
import requests

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


def get_longlived_token(shortlived_token, appid, appsecret):
    """exchange a user shortlived token with a longlived one.

    cf. https://developers.facebook.com/docs/facebook-login/access-tokens/#termtokens
    for more information about facebook tokens.

    Args:
        `shortlived_token`: user shortlived token, obtained manually
        `appid`: application id
        `appsecret`: application secret

    Returns:
        The longlived token
    """
    resp = requests.get(
        'https://graph.facebook.com/oauth/access_token',
        params={
            'grant_type': 'fb_exchange_token',
            'client_id': appid,
            'client_secret': appsecret,
            'fb_exchange_token': shortlived_token,
        },
    ).json()
    return resp['access_token']


def get_page_tokens(longlived_token) -> Dict[str, str]:
    """exchange a longlived token with a page token.

    Args:
        `longlived_token`: user longlived token
    Returns:
        a mapping page_id → page_token
    """
    graph = facebook.GraphAPI(access_token=longlived_token, version='2.8')
    pages_data = graph.get_object('me/accounts')
    page_tokens = {item['id']: item['access_token'] for item in pages_data['data']}
    return page_tokens


class FacebookInsightsDataSource(ToucanDataSource):
    """cf. https://developers.facebook.com/docs/graph-api/reference/v2.8/insights"""

    pages: Dict[str, str]  # mapping page_id → page_token
    metrics: List[str]
    period: str = 'week'
    date_preset: str = 'last_30d'


class FacebookInsightsConnector(ToucanConnector):
    data_source_model: FacebookInsightsDataSource

    def _retrieve_data(self, data_source: FacebookInsightsDataSource) -> pd.DataFrame:
        """Return the concatenated insights for all pages.

        Insight values will be flattened in the output dataframe. Here are the
        expected columns:
        - `id`: the insight id (e.g. `page-id-1/insights/page_total_actions/week`)
        - `name`: the metric name (e.g. `page_total_actions`)
        - `period`: the insight collection period (e.g. `week`)
        - `title`: a title for the metric returned
        - `description`: a description for the metric returned
        - `end_time`: timestamp, as string, at which insight was collected
          (e.g. `2019-02-01T08:00:00+0000`)
        - `value`: insight value (e.g. `42`)
        """
        graph = facebook.GraphAPI()
        insights = []
        for pageid, pagetoken in data_source.pages.items():
            insight = graph.get_object(
                id=f'{pageid}/insights',
                metric=data_source.metrics,
                period=data_source.period,
                date_preset=data_source.date_preset,
                access_token=pagetoken,
            )
            for data_obj in insight['data']:
                for insight_value in data_obj.pop('values'):
                    insights.append({**data_obj, **insight_value})
        df = pd.DataFrame(insights)
        return df
