from toucan_connectors.google_my_business.google_my_business_connector import (
    GoogleMyBusinessConnector,
    GoogleMyBusinessDataSource,
)

c = GoogleMyBusinessConnector(
    name='test_name',
    credentials={
        'token': 'test',
        'refresh_token': 'test',
        'token_uri': 'test',
        'client_id': 'test',
        'client_secret': 'test',
    },
)

s = GoogleMyBusinessDataSource(
    name='test_name',
    domain='test_domain',
    location_ids=['foo'],
    metric_requests=[],
    time_range={'start_time': '', 'end_time': ''},
)


def test_get_df(mocker):
    REPORT_INSIGHTS = {  # noqa: N806
        'locationMetrics': [
            {
                'locationName': 'locations/hey',
                'timeZone': 'Europe/Paris',
                'metricValues': [
                    {
                        'metric': 'QUERIES_DIRECT',
                        'dimensionalValues': [
                            {'metricOption': 'AGGREGATED_DAILY', 'value': '1007'},
                            {'metricOption': 'AGGREGATED_DAILY', 'value': '949'},
                        ],
                    },
                    {
                        'metric': 'QUERIES_DIRECT',
                        'totalValue': {'metricOption': 'AGGREGATED_TOTAL', 'value': '29423'},
                    },
                    {
                        'metric': 'QUERIES_INDIRECT',
                        'totalValue': {'metricOption': 'AGGREGATED_TOTAL', 'value': '32520'},
                    },
                ],
            }
        ]
    }

    mock_service = mocker.patch.object(GoogleMyBusinessConnector, 'build_service').return_value
    mock_service.accounts.return_value.list.return_value.execute.return_value = {'accounts': [{'name': 'plop'}]}
    mock_service.accounts.return_value.locations.return_value.reportInsights.return_value.execute.return_value = (
        REPORT_INSIGHTS
    )

    df = c.get_df(s)
    assert df.shape == (4, 5)
    assert set(df.columns) == {'locationName', 'metric', 'metricOption', 'timeZone', 'value'}
    assert (df.locationName == 'locations/hey').all()
