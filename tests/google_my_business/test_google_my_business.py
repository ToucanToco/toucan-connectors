from toucan_connectors.google_my_business.google_my_business_connector import (
    GoogleMyBusinessDataSource, GoogleMyBusinessConnector
)

c = GoogleMyBusinessConnector(
    name='test_name',
    credentials={
        'token': 'test',
        'refresh_token': 'test',
        'token_uri': 'test',
        'client_id': 'test',
        'client_secret': 'test',
    }
)

s = GoogleMyBusinessDataSource(
    name='test_name',
    domain='test_domain',
    metric_requests=[],
    time_range={"start_time": "", "end_time": ""},
)


def test_get_df():
    df = c.get_df(s)
    print(df)
