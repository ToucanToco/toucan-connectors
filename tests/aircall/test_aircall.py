import pytest
from toucan_connectors.aircall.aircall_connector import AircallConnector, AircallDataSource


# team_data = jq(teams_jq_filter).transform(teams_data)
# users_data = jq(users_jq_filter).transform(empty_users)

# test_empty = jq(teams_jq_filter).transform(ugly_json)

# test_empty_users = jq(teams_jq_filter).transform(empty_team_users)
# test_empty_teams = jq(teams_jq_filter).transform(empty_teams)

# print('test ', test)
# print('test empty users ', test_empty_users)
# print('test empty teams', test_empty_teams)

# print('users_data ', users_data)

# test_users = jq(users_jq_filter).transform(user_json)
# test_empty_users = jq(users_jq_filter).transform(empty_users)

# print('test users ', test_users)
# print('empty ', test_empty_users)


@pytest.fixture
def con(bearer_auth_id):
    return AircallConnector(name='test_name', bearer_auth_id=bearer_auth_id)


def test_get_page_data_async(mocker):
    """This tests async data call to /teams route"""
    con = AircallConnector(name='mah_test', bearer_auth_id='abc123efg')
    ds = AircallDataSource(
        name='mah_ds',
        domain='test_domain',
        dataset='users',
        limit=10,
        # filter='.calls | map({id, user, teams})'

        # filter='.calls | map({id, user, direction, duration, answered_at, ended_at, raw_digits, tags, teams})'
    )

    con._retrieve_data(ds)


@pytest.mark.flaky(reruns=5, reruns_delay=2)
def test_aircall_params_default_limit(con, mocker):
    """It should retrieve 100 entries by default"""
    get_page_data_spy = mocker.spy(AircallConnector, '_get_page_data')
    ds = AircallDataSource(
        name='test_name', domain='test_domain', endpoint='/calls', filter='.calls | map({id})',
    )

    df = con.get_df(ds)
    assert len(df) == 100
    assert get_page_data_spy.call_count == 2


def test_aircall_params_with_no_limit(con, mocker):
    """It should retrieve all entries if limit is -1"""
    get_page_data_mock = mocker.patch.object(
        AircallConnector,
        '_get_page_data',
        side_effect=[
            ([{'a': 1}] * 50, False),
            ([{'a': 1}] * 50, False),
            ([{'a': 1}] * 50, False),
            ([{'a': 1}] * 17, True),
        ],
    )

    ds = AircallDataSource(
        name='test_name',
        domain='test_domain',
        endpoint='/calls',
        limit=-1,
        filter='.calls | map({id})',
    )
    df = con.get_df(ds)
    assert len(df) == 167
    assert get_page_data_mock.call_count == 4


def test_aircall_params_negative_limit():
    """It should be forbidden to set a negative limit (except -1)"""
    with pytest.raises(ValueError):
        AircallDataSource(
            name='test_name', domain='test_domain', endpoint='/calls', limit=-2,
        )


@pytest.mark.flaky(reruns=5, reruns_delay=2)
def test_aircall_params_limit_filter(con):
    """It should filter properly the retrieved data"""
    ds = AircallDataSource(
        name='test_name',
        domain='test_domain',
        endpoint='/calls',
        query={'order': 'asc', 'order_by': 'ended_at'},
        limit=10,
        filter='.calls | map({id, duration, ended_at})',
    )

    df = con.get_df(ds)
    assert df.shape == (10, 3)
    assert list(df.columns) == ['id', 'duration', 'ended_at']
    assert df.ended_at.sort_values(ascending=True).equals(df.ended_at)


def test_aircall_params_no_meta(con, mocker):
    """It should work if no meta is sent"""
    ds = AircallDataSource(name='test_name', domain='test_domain', endpoint='/calls/1',)
    mocker.patch(
        'toucan_connectors.toucan_connector.ToucanConnector.bearer_oauth_get_endpoint',
        return_value={'id': 1},
    )

    df = con.get_df(ds)
    assert len(df) == 1
