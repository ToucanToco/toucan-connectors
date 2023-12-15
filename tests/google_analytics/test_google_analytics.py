import pytest

from toucan_connectors.google_analytics.google_analytics_connector import (
    GoogleAnalyticsConnector,
    GoogleAnalyticsDataSource,
)
from toucan_connectors.json_wrapper import JsonWrapper


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
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/pika.com",
        },
    )

    gads = GoogleAnalyticsDataSource(
        name="Test",
        domain="test",
        report_request={
            "viewId": "0123456789",
            "dateRanges": [{"startDate": "2018-06-01", "endDate": "2018-07-01"}],
        },
    )

    fixture = JsonWrapper.load(open("tests/google_analytics/fixtures/reports.json"))
    module = "toucan_connectors.google_analytics.google_analytics_connector"
    mocker.patch(f"{module}.ServiceAccountCredentials.from_json_keyfile_dict")
    mocker.patch(f"{module}.build")
    mocker.patch(f"{module}.get_query_results").return_value = fixture["reports"][0]

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
            "client_x509_cert_url": "",
        },
    )

    gads = GoogleAnalyticsDataSource(
        name="Test",
        domain="test",
        report_request={
            "viewId": "119151898",
            "pageSize": 100,
            "orderBys": [{"fieldName": "ga:date", "orderType": "VALUE", "sortOrder": "%(sortOrder)s"}],
            "dimensions": [
                {"name": "ga:hostname"},
                {"name": "ga:date"},
                {"name": "ga:dimension1"},
                {"name": "ga:deviceCategory"},
                {"name": "ga:eventLabel"},
            ],
            "dateRanges": [{"startDate": "2018-06-01", "endDate": "2018-07-01"}],
            "metrics": [{"expression": "ga:sessions"}, {"expression": "ga:sessionDuration"}],
        },
        parameters={"sortOrder": "DESCENDING"},
    )

    df = gac.get_df(gads)
    assert df.shape == (230, 11)
