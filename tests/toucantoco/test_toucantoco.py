import pytest
import responses

from toucan_connectors.toucantoco.toucantoco_connector import ToucanTocoConnector, \
    ToucanTocoDataSource


tcc = ToucanTocoConnector(
    name='test',
    host='https://example.com',
    username='username',
    password='password'
)

tcd = ToucanTocoDataSource(
    name='test',
    domain='test',
    endpoint='small-apps'
)


@responses.activate
def test_toucantoco():

    responses.add('GET', 'https://example.com/small-apps',
                  json=[{'duplicateOf': '', 'id': '', 'last_update': '', 'name': '', 'stage': ''}],
                  status=200)
    df = tcc.get_df(tcd)
    assert list(df.columns) == ['duplicateOf', 'id', 'last_update', 'name', 'stage']


@pytest.mark.skip(reason="This uses a live demo")
def test_live():
    tcc_live = ToucanTocoConnector(
        name='test',
        host='https://api-demo.toucantoco.com',
        username='**********',
        password='**********'
    )
    df = tcc_live.get_df(tcd)
    assert list(df.columns) == ['duplicateOf', 'id', 'last_update', 'name', 'stage']