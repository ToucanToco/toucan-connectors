from typing import Dict, List

import pandas as pd

from .enums import HubspotDataset


def flatten_subdict(d: Dict, subdict_key: str) -> Dict:
    for k, v in d[subdict_key].items():
        d[k] = v
    d.pop(subdict_key)


def format_hubspot_api_v3(data: List[dict]) -> pd.DataFrame:
    for result in data:
        flatten_subdict(result, 'properties')
    return pd.DataFrame(data)


def format_webanalytics(data: List[dict]) -> pd.DataFrame:
    """Handles data that cames from the webanalytics API route.

    For more information, look at the dedicated documentation: https://developers.hubspot.com/docs/api/events/web-analytics
    """
    # if data is none, return just an empty dataframe
    if not data:
        return pd.DataFrame([])

    # Extract the surrounding data to insert it into the properties dict
    for result in data:
        if 'eventType' in result:
            result['properties']['eventType'] = result['eventType']
        if 'occuredAt' in result:
            result['properties']['occuredAt'] = result['occuredAt']
        flatten_subdict(result, 'properties')
    return pd.DataFrame(data)


def format_email_events(data: List[dict]) -> pd.DataFrame:
    """Handles data that cames from the email-events API route.

    For more information, look at the dedicated documentation: https://legacydocs.hubspot.com/docs/methods/email/get_events?_ga=2.71868499.1363348269.1614853210-1638453014.16134
    """
    if not data:
        return pd.DataFrame([])
    events = []

    # Extract the useful data instead of returning everything contained in the event
    for event in data:
        e = {
            'appName': event.get('appName'),
            'created': event.get('created'),
            'recipient': event.get('recipient'),
            'type': event.get('type'),
        }

        if event.get('location'):
            e['city'] = event['location']['city']
            e['country'] = event['location']['country']
            e['state'] = event['location']['state']

        if event.get('browser'):
            e['browser'] = event['browser']['name']

        events.append(e)

    return pd.DataFrame(events)


def format_hubspot_response(dataset: str, data: List[dict]) -> pd.DataFrame:
    """This function maps `data` to a formatting function tied to `dataset`.
    Some endpoints will return data in a different kind or with data that are not
    stored under the properties, these formatting functions will extract these data
    to ensure that they are retrieved by the end user.
    """
    RESPONSE_V3_FORMAT_MAPPING = [
        HubspotDataset.contacts,
        HubspotDataset.companies,
        HubspotDataset.deals,
        HubspotDataset.products,
    ]

    RESPONSE_CUSTOM_FORMAT_MAPPING = {
        HubspotDataset.webanalytics: format_webanalytics,
        HubspotDataset.emails_events: format_email_events,
    }

    if dataset in RESPONSE_V3_FORMAT_MAPPING:
        return format_hubspot_api_v3(data)

    if dataset in RESPONSE_CUSTOM_FORMAT_MAPPING:
        return RESPONSE_CUSTOM_FORMAT_MAPPING[dataset](data)


def has_next_page(data):
    return 'paging' in data and 'next' in data['paging']


def has_next_page_legacy(data):
    return 'hasMore' in data and data['hasMore'] is True
