import os

from toucan_connectors.aircall.aircall_connector import AircallConnector, AircallDataSource

con = AircallConnector(name='test_name', bearer_auth_id='bearer-auth-id')

ds = AircallDataSource(name='test_name', domain='test_domain', endpoint='/users/{{ myUser }}')


def test_aircall(mocker):
    mocker.patch.dict(os.environ, {'BEARER_API_KEY': 'my_bearer_api_key'})
    calls = {
        'Eric': {'calls': [1, 2]},
        'Pierre': {'calls': [3, 4]},
    }
    mocker.patch.object(
        AircallConnector, 'bearer_oauth_get_endpoint', new=lambda self, x: calls[x.split('/')[-1]]
    )

    ds.parameters = {'myUser': 'Eric'}
    df = con.get_df(ds)
    assert df.loc[0, 'calls'] == [1, 2]

    ds.parameters = {'myUser': 'Pierre'}
    df = con.get_df(ds)
    assert df.loc[0, 'calls'] == [3, 4]
