import pytest
from pydantic import ValidationError

from tests.aircall.helpers import build_con_and_ds, handle_mock_data
from tests.aircall.mock_results import (
    fake_tags,
    fake_teams,
    fake_users,
    filtered_calls,
    filtered_tags,
    filtered_teams,
    filtered_users,
)
from toucan_connectors.aircall.aircall_connector import AircallConnector, AircallDataSource

columns_for_calls = [
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
    'day',
]
columns_for_tags = ['id', 'name', 'color', 'description']
columns_for_teams = ['team', 'user_id', 'user_name', 'user_created_at']
columns_for_users = ['user_id', 'user_name', 'user_created_at']


@pytest.fixture
def con(bearer_aircall_auth_id):
    return AircallConnector(name='test_name', bearer_auth_id=bearer_aircall_auth_id)


def test__get_data_calls(event_loop):
    """Tests _get_data() with calls call (E2E)"""
    dataset = 'calls'
    columns_for_red_calls = [
        'id',
        'direction',
        'duration',
        'answered_at',
        'ended_at',
        'raw_digits',
        'user_id',
        'tags',
        'user_name',
    ]
    con, ds = build_con_and_ds(dataset)
    res = event_loop.run_until_complete(con._get_data(dataset, {}, 1))
    first_part, second_part = res

    if len(first_part) > 0:
        first_ele = first_part[0]
        keys = list(first_ele)
        assert keys == columns_for_teams  # insures order of columns

    if len(second_part):
        second_ele = second_part[0]
        keys = list(second_ele)
        assert keys == columns_for_red_calls  # insures order of columns


def test__get_data_users(event_loop):
    """Tests _get_data() with users call (E2E)"""
    dataset = 'users'
    con, ds = build_con_and_ds(dataset)
    res = event_loop.run_until_complete(con._get_data(dataset, {}, 1))

    first_part, second_part = res

    if len(first_part) > 0:
        first_ele = first_part[0]
        keys = list(first_ele)
        assert keys == columns_for_teams  # insures order of columns

    if len(second_part):
        second_ele = second_part[0]
        keys = list(second_ele)
        assert keys == columns_for_users  # insures order of columns


@pytest.mark.asyncio
async def test__get_data_tags_case(mocker):
    """Tests with tags happy case"""
    dataset = 'tags'
    fake_res = handle_mock_data(fake_tags)
    fake_fetch_page = mocker.patch(
        'toucan_connectors.aircall.aircall_connector.fetch_page', return_value=fake_res
    )
    con, ds = build_con_and_ds(dataset)
    res = await con._get_tags(ds.dataset, {}, 10)

    assert fake_fetch_page.call_count == 1
    assert len(res) == 3


@pytest.mark.asyncio
async def test__get_data_users_case(mocker):
    """Tests users call happy case"""
    dataset = 'users'
    fake_res = handle_mock_data([fake_teams, fake_users])
    fake_fetch_page = mocker.patch(
        'toucan_connectors.aircall.aircall_connector.fetch_page', side_effect=fake_res
    )
    con, ds = build_con_and_ds(dataset)
    res = await con._get_data(ds.dataset, {}, 10)

    assert fake_fetch_page.call_count == 2
    assert len(res) == 2
    teams, users = res
    assert len(teams) == 7
    assert list(teams[0].keys()) == columns_for_teams
    assert len(users) == 11
    assert list(users[0].keys()) == columns_for_users


def test__retrieve_data_users_happy_case(mocker):
    """Tests case when users call has data"""
    # NOTE: this test is only cursory because 'users' call is tested more
    # thoroughly in helpers test file
    run_fetches_mock = mocker.patch.object(
        AircallConnector, 'run_fetches', return_value=[filtered_teams, filtered_users]
    )
    con, ds = build_con_and_ds('users')

    df = con._retrieve_data(ds)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (6, 4)

    assert list(df.columns) == columns_for_teams


def test__retrieve_data_calls_happy_case(mocker):
    """Tests case when calls call has data"""
    run_fetches_mock = mocker.patch.object(
        AircallConnector, 'run_fetches', return_value=[filtered_teams, filtered_calls]
    )
    con, ds = build_con_and_ds('calls')

    df = con._retrieve_data(ds)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (10, 11)
    assert list(df.columns) == columns_for_calls
    assert df['team'].isna().sum() == 0
    assert df['team'].eq('Team 1').sum() == 6
    assert df['team'].eq('Team 2').sum() == 4


def test__retrieve_data_tags_happy_case(mocker):
    """Tests case when tags call has data"""
    run_fetches_mock = mocker.patch.object(
        AircallConnector, 'run_fetches_for_tags', return_value=filtered_tags
    )
    con, ds = build_con_and_ds('tags')

    df = con._retrieve_data(ds)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (3, 4)
    assert list(df.columns) == columns_for_tags


def test__retrieve_data_no_data_case(mocker):
    """Tests case when there is no data returned"""
    run_fetches_mock = mocker.patch.object(AircallConnector, 'run_fetches', return_value=[[], []])
    con, ds = build_con_and_ds('users')

    df = con._retrieve_data(ds)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (0, 4)
    assert list(df.columns) == columns_for_teams


def test_run_fetches_for_tags(mocker):
    """Tests the loop generator function for tags call"""
    dataset = 'tags'
    spy = mocker.spy(AircallConnector, 'run_fetches_for_tags')
    con, ds = build_con_and_ds(dataset)
    con.run_fetches_for_tags(dataset, {}, 1)
    assert spy.call_count == 1


def test_run_fetches(mocker):
    """Tests the loop generator function for calls/users call"""
    dataset = 'users'
    spy = mocker.spy(AircallConnector, 'run_fetches')
    con, ds = build_con_and_ds(dataset)
    con.run_fetches(dataset, {}, 1)
    assert spy.call_count == 1


def test__retrieve_data_no_teams_case(mocker):
    """Tests case when there is no team data but there is calls data"""
    run_fetches_mock = mocker.patch.object(
        AircallConnector, 'run_fetches', return_value=[[], filtered_calls]
    )
    con, ds = build_con_and_ds('calls')
    df = con._retrieve_data(ds)

    # must have calls and still have a team column even if everything is NaN
    assert run_fetches_mock.call_count == 1
    assert df.shape == (10, 11)
    assert df['team'].isna().any()


def test_bad_limit():
    """Tests case when user passes a bad limit"""
    # limit less than 1 (want to be able to do at least one run)
    with pytest.raises(ValidationError):
        AircallDataSource(name='bar', domain='test_domain', dataset='tags', limit=-6)


def test_default_limit(mocker):
    """If no limit is provided, the default is chosen"""
    con = AircallConnector(name='mah_test', bearer_auth_id='abc123efg')
    ds = AircallDataSource(name='mah_ds', domain='test_domain', dataset='tags')
    mocker.patch.object(AircallConnector, 'run_fetches_for_tags', return_value=filtered_tags)

    con._retrieve_data(ds)
