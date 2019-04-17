import responses

from toucan_connectors.facebook_insights.facebook_insights_connector import (
    FacebookInsightsConnector, FacebookInsightsDataSource, get_page_tokens, get_longlived_token
)


def mock_facebook_insights_response():
    responses.add(
        responses.GET,
        'https://graph.facebook.com/v2.8/page-id-1/insights',
        content_type='application/json',
        json={
            'data': [{
                'description': 'Weekly: number of clicks',
                'id': 'page-id-1/insights/page_total_actions/week',
                'name': 'page_total_actions',
                'period': 'week',
                'title': 'Weekly Total: total action count per Page',
                'values':
                    [
                        {
                            'end_time': '2019-01-02T08:00:00+0000',
                            'value': 24
                        }, {
                            'end_time': '2019-01-03T08:00:00+0000',
                            'value': 21
                        }
                    ],
            }]
        }
    )
    responses.add(
        responses.GET,
        'https://graph.facebook.com/v2.8/page-id-2/insights',
        content_type='application/json',
        json={
            'data': [{
                'description': 'Weekly: number of clicks',
                'id': 'page-id-2/insights/page_total_actions/week',
                'name': 'page_total_actions',
                'period': 'week',
                'title': 'Weekly Total: total action count per Page',
                'values':
                    [
                        {
                            'end_time': '2019-02-01T08:00:00+0000',
                            'value': 10
                        }
                    ],
            }]
        }
    )


@responses.activate
def test_get_page_tokens():
    responses.add(
        responses.GET,
        'https://graph.facebook.com/v2.8/me/accounts?access_token=the-longlived-token',
        content_type='application/json',
        json={
            'data':
                [
                    {
                        'access_token': 'access-token-1',
                        'category': 'Shopping Mall',
                        'category_list': [],
                        'id': 'page-id-1',
                        'name': 'page 1',
                        'store_number': 40,
                        'tasks': ['ANALYZE', 'ADVERTISE', 'MODERATE', 'CREATE_CONTENT', 'MANAGE'],
                    },
                    {
                        'access_token': 'access-token-2',
                        'category': 'Just For Fun',
                        'category_list': [],
                        'id': 'page-id-2',
                        'name': 'page 2',
                        'tasks': ['ANALYZE', 'ADVERTISE', 'MODERATE', 'CREATE_CONTENT', 'MANAGE'],
                    }
                ],
            'paging': {
                'cursors': {
                    'after': 'NzgwODkyODA1NjMwODU0',
                    'before': 'MjkyMDc0MTQ3MzAw',
                    },
                },
            }
        )
    tokens = get_page_tokens('the-longlived-token')
    assert tokens == {
        'page-id-1': 'access-token-1',
        'page-id-2': 'access-token-2',
    }


@responses.activate
def test_get_longlived_tokens():
    responses.add(
        responses.GET,
        'https://graph.facebook.com/oauth/access_token',
        content_type='application/json',
        json={
            'access_token': 'the-longlived-token',
            'token_type': 'bearer',
        }
    )
    token = get_longlived_token('the-shortlived-token', 'appid', 'appsecret')
    assert token == 'the-longlived-token'


@responses.activate
def test_get_facebook_insights_df():
    mock_facebook_insights_response()
    datasource = FacebookInsightsDataSource(
        name='test', domain='test',
        pages={
            'page-id-1': 'access-token-1',
            'page-id-2': 'access-token-2',
        },
        metrics=['page_total_actions'],
    )
    connector = FacebookInsightsConnector(
        name='facebook',
        type='facebook_insights')
    df = connector.get_df(datasource)
    assert df.shape == (3, 7)
    assert set(df.columns) == {'description', 'end_time', 'name', 'title', 'value', 'id', 'period'}
    assert df[['id', 'end_time', 'value']].values.tolist() == [
        ['page-id-1/insights/page_total_actions/week', '2019-01-02T08:00:00+0000', 24],
        ['page-id-1/insights/page_total_actions/week', '2019-01-03T08:00:00+0000', 21],
        ['page-id-2/insights/page_total_actions/week', '2019-02-01T08:00:00+0000', 10],
    ]
