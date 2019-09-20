import pytest
import responses

from toucan_connectors.toucan_toco.toucan_toco_connector import (
    ToucanTocoConnector,
    ToucanTocoDataSource,
)

tcc = ToucanTocoConnector(
    name='test', host='https://example.com', username='username', password='password'
)

tcd = ToucanTocoDataSource(name='test', domain='test', endpoint='small-apps')

tcda = ToucanTocoDataSource(name='test', domain='test', endpoint='config', all_small_apps=True)


fixtures = {
    'small_apps': [{'duplicateOf': '', 'id': 'test', 'last_update': '', 'name': '', 'stage': ''}],
    'config': {'arbitrary': 'object'},
}


@responses.activate
def test_toucantoco_instance():
    responses.add('GET', 'https://example.com/small-apps', json=fixtures['small_apps'], status=200)

    df = tcc.get_df(tcd)
    assert list(df.columns) == ['duplicateOf', 'id', 'last_update', 'name', 'stage']


@responses.activate
def test_toucan_toco_all_small_apps():
    responses.add('GET', 'https://example.com/small-apps', json=fixtures['small_apps'], status=200)
    responses.add('GET', 'https://example.com/test/config', json=fixtures['config'], status=200)

    df = tcc.get_df(tcda)
    assert set(df.columns) == {'small_app', 'response'}
    assert df.iloc[0]['response'] == fixtures['config']


@pytest.mark.skip(reason="This uses a live demo")
def test_live():
    tcc_live = ToucanTocoConnector(
        name='test',
        host='https://api-demo.toucantoco.com',
        username='**********',
        password='**********',
    )
    df = tcc_live.get_df(tcd)
    assert set(df.columns) == {'duplicateOf', 'id', 'last_update', 'name', 'stage'}
