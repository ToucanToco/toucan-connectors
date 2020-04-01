import asyncio
import pytest
from tests.aircall.helpers import build_con_and_ds, run_loop
from tests.aircall.mock_results import (
    fake_tags,
    fake_teams,
    fake_users,
    filtered_calls,
    filtered_tags,
    filtered_teams,
    filtered_users
)
from toucan_connectors.aircall.aircall_connector import AircallConnector, AircallDataSource

# STUFF = '156faf0053c34ea6535126f9274181f4:1434a05fe17fe0cd0121d840966d8d71@'
# endpoint = 'api.aircall.io/v1/'


def test__get_data_calls(mocker):
    """Tests _get_data() with calls call"""
    con, ds = build_con_and_ds('calls')
    res = run_loop(con, ds)
    columns_for_calls = [
        'id',
        'direction',
        'duration',
        'answered_at',
        'ended_at',
        'raw_digits',
        'user_id',
        'tags',
        'user_name'
    ]
    columns_for_teams = ['team', 'user_id', 'user_name', 'user_created_at']

    first_part, second_part = res

    if len(first_part) > 0:
        first_ele = first_part[0]
        keys = list(first_ele)
        assert keys == columns_for_teams  # insures order of columns

    if len(second_part):
        second_ele = second_part[0]
        keys = list(second_ele)
        assert keys == columns_for_calls  # insures order of columns


def test__get_data_users(mocker):
    """Tests _get_data() with users call"""
    con, ds = build_con_and_ds('users')
    res = run_loop(con, ds)
    columns_for_users = ['user_id', 'user_name', 'user_created_at']
    columns_for_teams = ['team', 'user_id', 'user_name', 'user_created_at']

    first_part, second_part = res

    if len(first_part) > 0:
        first_ele = first_part[0]
        keys = list(first_ele)
        assert keys == columns_for_teams  # insures order of columns

    if len(second_part):
        second_ele = second_part[0]
        keys = list(second_ele)
        assert keys == columns_for_users  # insures order of columns


async def test__get_data_tags_case(mocker):
    """Tests with tags happy case"""
    dataset = 'tags'
    f = asyncio.Future()
    f.set_result(fake_tags)
    fake_fetch_page = mocker.patch(
        'toucan_connectors.aircall.aircall_connector.fetch_page',
        return_value=f
    )
    con, ds = build_con_and_ds(dataset)
    res = await con._get_tags(ds.dataset, {}, 10)

    assert fake_fetch_page.call_count == 1
    assert len(res) == 3


async def test__get_data_users_case(mocker):
    """Tests with users happy case"""
    dataset = 'users'
    f = asyncio.Future()
    f.set_result([fake_teams, fake_users])
    fake_fetch_page = mocker.patch(
        'toucan_connectors.aircall.aircall_connector.fetch_page',
        return_value=f
    )
    con, ds = build_con_and_ds(dataset)
    res = await con._get_data(ds.dataset, {}, 10)
    print(res)

    # assert fake_fetch_page.call_count == 1
    # assert len(res) == 3


@pytest.fixture
def con(bearer_aircall_auth_id):
    return AircallConnector(name='test_name', bearer_auth_id=bearer_aircall_auth_id)


def test__retrieve_data_users_happy_case(mocker):
    """Tests case when users call has data"""
    # NOTE: this test is only cursory because 'users' call is tested more
    # thoroughly in helpers test file
    order_of_cols = [
        'team',
        'user_id',
        'user_name',
        'user_created_at'
    ]
    run_fetches_mock = mocker.patch.object(
        AircallConnector,
        'run_fetches',
        return_value=[filtered_teams, filtered_users]
    )
    con, ds = build_con_and_ds('users')

    df = con._retrieve_data(ds)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (6, 4)

    assert list(df.columns) == order_of_cols


def test__retrieve_data_calls_happy_case(mocker):
    """Tests case when calls call has data"""
    order_of_cols = [
        'id',
        'direction',
        'duration',
        'answered_at',
        'ended_at',
        'raw_digits',
        'user_id',
        'tags',
        'user_name',
        'team',
        'day'
    ]
    run_fetches_mock = mocker.patch.object(
        AircallConnector,
        'run_fetches',
        return_value=[filtered_teams, filtered_calls]
    )
    con, ds = build_con_and_ds('calls')

    df = con._retrieve_data(ds)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (10, 11)
    assert list(df.columns) == order_of_cols
    assert df['team'].isna().sum() == 0
    assert df['team'].eq('Team 1').sum() == 6
    assert df['team'].eq('Team 2').sum() == 4


def test__retrieve_data_tags_happy_case(mocker):
    """Tests case when calls call has data"""
    order_of_cols = [
        'id',
        'name',
        'color',
        'description'
    ]
    run_fetches_mock = mocker.patch.object(
        AircallConnector,
        'run_fetches_for_tags',
        return_value=filtered_tags
    )
    con, ds = build_con_and_ds('tags')

    df = con._retrieve_data(ds)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (3, 4)
    assert list(df.columns) == order_of_cols


def test__retrieve_data_no_data_case(mocker):
    """Tests case when there is no data returned"""
    order_of_cols = [
        'team',
        'user_id',
        'user_name',
        'user_created_at'
    ]
    run_fetches_mock = mocker.patch.object(
        AircallConnector,
        'run_fetches',
        return_value=[[], []]
    )
    con, ds = build_con_and_ds('users')

    df = con._retrieve_data(ds)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (0, 4)
    assert list(df.columns) == order_of_cols


# @pytest.mark.flaky(reruns=5, reruns_delay=2)
# def test_aircall_params_default_limit(con, mocker):
#     """It should retrieve 100 entries by default"""
#     get_page_data_spy = mocker.spy(AircallConnector, '_get_page_data')
#     ds = AircallDataSource(
#         name='test_name', domain='test_domain', endpoint='/calls', filter='.calls | map({id})',
#     )

#     df = con.get_df(ds)
#     assert len(df) == 100
#     assert get_page_data_spy.call_count == 2


# def test_aircall_params_with_no_limit(con, mocker):
#     """It should retrieve all entries if limit is -1"""
#     get_page_data_mock = mocker.patch.object(
#         AircallConnector,
#         '_get_page_data',
#         side_effect=[
#             ([{'a': 1}] * 50, False),
#             ([{'a': 1}] * 50, False),
#             ([{'a': 1}] * 50, False),
#             ([{'a': 1}] * 17, True),
#         ],
#     )

#     ds = AircallDataSource(
#         name='test_name',
#         domain='test_domain',
#         endpoint='/calls',
#         limit=-1,
#         filter='.calls | map({id})',
#     )
#     df = con.get_df(ds)
#     assert len(df) == 167
#     assert get_page_data_mock.call_count == 4


# def test_aircall_params_negative_limit():
#     """It should be forbidden to set a negative limit (except -1)"""
#     with pytest.raises(ValueError):
#         AircallDataSource(
#             name='test_name', domain='test_domain', endpoint='/calls', limit=-2,
#         )


# @pytest.mark.flaky(reruns=5, reruns_delay=2)
# def test_aircall_params_limit_filter(con):
#     """It should filter properly the retrieved data"""
#     ds = AircallDataSource(
#         name='test_name',
#         domain='test_domain',
#         endpoint='/calls',
#         query={'order': 'asc', 'order_by': 'ended_at'},
#         limit=10,
#         filter='.calls | map({id, duration, ended_at})',
#     )

#     df = con.get_df(ds)
#     assert df.shape == (10, 3)
#     assert list(df.columns) == ['id', 'duration', 'ended_at']
#     assert df.ended_at.sort_values(ascending=True).equals(df.ended_at)


def test_aircall_params_no_meta(con, mocker):
    """It should work if no meta is sent"""
    ds = AircallDataSource(name='test_name', domain='test_domain', endpoint='/calls/1',)
    mocker.patch(
        'toucan_connectors.toucan_connector.ToucanConnector.bearer_oauth_get_endpoint',
        return_value={'id': 1},
    )

    df = con.get_df(ds)
    assert len(df) == 1
