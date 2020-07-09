from typing import List

import pandas as pd
import pyjq
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from pandas.io.json import json_normalize
from pydantic import BaseModel

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource

API_SERVICE_NAME = 'mybusiness'
API_VERSION = 'v4'
DISCOVERY_URI = f'https://developers.google.com/my-business/samples/{API_SERVICE_NAME}_google_rest_{API_VERSION}.json'  # noqa: E501


class Metric(BaseModel):
    metric: str
    options: List[str] = None


class TimeRange(BaseModel):
    start_time: str
    end_time: str


class GoogleMyBusinessDataSource(ToucanDataSource):
    location_ids: List[str] = None
    metric_requests: List[Metric]
    time_range: TimeRange


class GoogleCredentials(BaseModel):
    token: str
    refresh_token: str
    token_uri: str
    client_id: str
    client_secret: str


class GoogleMyBusinessConnector(ToucanConnector):
    data_source_model: GoogleMyBusinessDataSource

    credentials: GoogleCredentials
    scopes: List[str] = ['https://www.googleapis.com/auth/business.manage']

    def build_service(self):
        credentials = Credentials.from_authorized_user_info(
            self.credentials.dict(), scopes=self.scopes
        )
        service = build(
            API_SERVICE_NAME,
            API_VERSION,
            credentials=credentials,
            discoveryServiceUrl=DISCOVERY_URI,
        )
        return service

    def _retrieve_data(self, data_source: GoogleMyBusinessDataSource) -> pd.DataFrame:
        service = self.build_service()
        accounts = service.accounts().list().execute()
        name = accounts['accounts'][0]['name']

        if data_source.location_ids:
            # concatenate name with these ids:
            location_names = [f'{name}/locations/{id}' for id in data_source.location_ids]
        else:
            # retrieve all locations:
            locations = service.accounts().locations().list(parent=name).execute()
            location_names = [loc['name'] for loc in locations['locations']]

        query = {
            'locationNames': location_names,
            'basicRequest': {
                'metricRequests': data_source.dict()['metric_requests'],
                'timeRange': data_source.dict()['time_range'],
            },
        }

        report_insights = (
            service.accounts().locations().reportInsights(name=name, body=query).execute()
        )

        location_metrics = report_insights['locationMetrics']

        f = """
            .[] as $in |
            $in.metricValues[] as $mv |
            $in | del(.metricValues) as $in2 |
            $in2 *  if $mv.dimensionalValues != null then
                      {"metric": $mv.metric} * $mv.dimensionalValues[]
                    else
                      {"metric": $mv.metric} * $mv.totalValue
                    end
        """
        res = pyjq.all(f, location_metrics)
        df = json_normalize(res)

        return df
