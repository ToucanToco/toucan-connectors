import json

import pytest

from toucan_connectors.google_analytics.google_analytics_connector import (
    GoogleAnalyticsConnector, GoogleAnalyticsDataSource)


def test_google_analytics(mocker):
    gac = GoogleAnalyticsConnector(
        type="GoogleAnalytics",
        name="Test",
        credentials={
            "type": "test",
            "project_id": "test",
            "private_key_id": "test",
            "private_key": "test",
            "client_email": "test",
            "client_id": "test",
            "auth_uri": "test",
            "token_uri": "test",
            "auth_provider_x509_cert_url": "test",
            "client_x509_cert_url": "test"
        }
    )

    gads = GoogleAnalyticsDataSource(
        name="Test", domain="test",
        report_request={
            "viewId": "0123456789",
            "dateRanges": [
                {"startDate": "2018-06-01", "endDate": "2018-07-01"}
            ]
        })

    fixture = json.load(open('tests/google_analytics/fixtures/reports.json'))
    module = 'toucan_connectors.google_analytics.google_analytics_connector'
    mocker.patch(f'{module}.ServiceAccountCredentials.from_json_keyfile_dict')
    mocker.patch(f'{module}.build')
    mocker.patch(f'{module}.get_query_results').return_value = fixture['reports'][0]

    df = gac.get_df(gads)
    assert df.shape == (3, 11)


@pytest.mark.skip(reason="This uses a live instance")
def test_live_instance():
    gac = GoogleAnalyticsConnector(
        type="GoogleAnalytics",
        name="Test",
        credentials={
            "type": "",
            "project_id": "",
            "private_key_id": "",
            "private_key": "",
            "client_email": "",
            "client_id": "",
            "auth_uri": "",
            "token_uri": "",
            "auth_provider_x509_cert_url": "",
            "client_x509_cert_url": ""
        }
    )

    gads = GoogleAnalyticsDataSource(
        name="Test", domain="test",
        report_request={
            "viewId": "119151898",
            "pageSize": 100,
            "orderBys": [
                {
                    "fieldName": "ga:date",
                    "orderType": "VALUE",
                    "sortOrder": "%(sortOrder)s"
                }
            ],
            "dimensions": [
                {"name": "ga:hostname"},
                {"name": "ga:date"},
                {"name": "ga:dimension1"},
                {"name": "ga:deviceCategory"},
                {"name": "ga:eventLabel"}
            ],
            "dateRanges": [
                {"startDate": "2018-06-01", "endDate": "2018-07-01"}
            ],
            "metrics": [
                {"expression": "ga:sessions"},
                {"expression": "ga:sessionDuration"}
            ]
        },
        parameters={'sortOrder': 'DESCENDING'}
    )

    df = gac.get_df(gads)
    assert df.shape == (230, 11)
