from typing import Dict, List

import facebook
import pandas as pd
import requests

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


def get_longlived_token(shortlived_token, appid, appsecret):
    resp = requests.get(
        'https://graph.facebook.com/oauth/access_token',
        params={
            'grant_type': 'fb_exchange_token',
            'client_id': appid,
            'client_secret': appsecret,
            'fb_exchange_token': shortlived_token
        }
    ).json()
    return resp['access_token']


def get_page_tokens(longlived_token):
    graph = facebook.GraphAPI(access_token=longlived_token, version='2.8')
    pages_data = graph.get_object("me/accounts")
    page_tokens = {item['id']: item['access_token'] for item in pages_data['data']}
    return page_tokens


class FacebookInsightsDataSource(ToucanDataSource):
    pages: Dict[str, str]  # mapping page_ids → page_tokens
    metrics: List[str]
    period: str = 'week'
    date_preset: str = 'last_30d'


class FacebookInsightsConnector(ToucanConnector):
    type = "facebook_insights"
    data_source_model: FacebookInsightsDataSource

    def get_df(self, data_source: FacebookInsightsDataSource) -> pd.DataFrame:
        graph = facebook.GraphAPI()
        insights = []
        for pageid, pagetoken in data_source.pages.items():
            insight = graph.get_object(
                id=f'{pageid}/insights',
                metric=data_source.metrics,
                period=data_source.period,
                date_preset=data_source.date_preset,
                access_token=pagetoken)
            for data_obj in insight['data']:
                for insight_value in data_obj.pop('values'):
                    insights.append({**data_obj, **insight_value})
        df = pd.DataFrame(insights)
        return df
