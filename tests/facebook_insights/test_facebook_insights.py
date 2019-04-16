import requests
import responses

from toucan_connectors.facebook_insights.facebook_insights_connector import FacebookInsightsConnector, FacebookInsightsDataSource, get_page_tokens, get_longlived_token


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
                    'before': 'MjkyMDc0MTQ3MzAw'
                }
            }
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
