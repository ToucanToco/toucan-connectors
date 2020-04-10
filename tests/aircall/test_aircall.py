import pytest
from pydantic import ValidationError

from tests.aircall.helpers import handle_mock_data
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

fetch_fn_name = 'toucan_connectors.aircall.aircall_connector.fetch_page'


@pytest.fixture
def con(bearer_aircall_auth_id):
    return AircallConnector(name='test_name', bearer_auth_id=bearer_aircall_auth_id)


def build_ds(dataset: str):
    """Builds test datasource"""
    return AircallDataSource(name='mah_ds', domain='mah_domain', dataset=dataset, limit=1,)


@pytest.mark.asyncio
async def test__get_data_tags_case(con, mocker):
    """Tests with tags happy case"""
    dataset = 'tags'
    fake_res = handle_mock_data(fake_tags)
    fake_fetch_page = mocker.patch(fetch_fn_name, return_value=fake_res)
    ds = build_ds(dataset)
    res = await con._get_tags(ds.dataset, {}, 10)

    assert fake_fetch_page.call_count == 1
    assert len(res) == 3


@pytest.mark.asyncio
async def test__get_data_tags_unhappy_case(con, mocker):
    """Tests what happens when tags call returns an error"""
    dataset = 'tags'
    mocker.patch(fetch_fn_name, return_value=Exception('OMGERD OOPS!!!'))
    ds = build_ds(dataset)
    with pytest.raises(Exception):
        await con._get_tags(ds.dataset, {}, 10)


@pytest.mark.asyncio
async def test__get_data_users_case(con, mocker):
    """Tests users call happy case"""
    dataset = 'users'
    fake_res = handle_mock_data([fake_teams, fake_users])
    fake_fetch_page = mocker.patch(fetch_fn_name, side_effect=fake_res)
    ds = build_ds(dataset)
    res = await con._get_data(ds.dataset, {}, 10)

    assert fake_fetch_page.call_count == 2
    assert len(res) == 2
    teams, users = res
    assert len(teams) == 7
    assert list(teams[0].keys()) == columns_for_teams
    assert len(users) == 11
    assert list(users[0].keys()) == columns_for_users


def test__retrieve_data_users_happy_case(con, mocker):
    """Tests case when users call has data"""
    # NOTE: this test is only cursory because 'users' call is tested more
    # thoroughly in helpers test file
    run_fetches_mock = mocker.patch.object(
        AircallConnector, 'run_fetches', return_value=[filtered_teams, filtered_users]
    )
    ds = build_ds('users')

    df = con._retrieve_data(ds)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (6, 4)

    assert list(df.columns) == columns_for_teams


def test__retrieve_data_calls_happy_case(con, mocker):
    """Tests case when calls call has data"""
    run_fetches_mock = mocker.patch.object(
        AircallConnector, 'run_fetches', return_value=[filtered_teams, filtered_calls]
    )
    ds = build_ds('calls')

    df = con._retrieve_data(ds)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (10, 11)
    assert list(df.columns) == columns_for_calls
    assert df['team'].isna().sum() == 0
    assert df['team'].eq('Team 1').sum() == 6
    assert df['team'].eq('Team 2').sum() == 4


def test__retrieve_data_tags_happy_case(con, mocker):
    """Tests case when tags call has data"""
    run_fetches_mock = mocker.patch.object(
        AircallConnector, 'run_fetches_for_tags', return_value=filtered_tags
    )
    ds = build_ds('tags')

    df = con._retrieve_data(ds)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (3, 4)
    assert list(df.columns) == columns_for_tags


def test__retrieve_data_no_data_case(con, mocker):
    """Tests case when there is no data returned"""
    run_fetches_mock = mocker.patch.object(AircallConnector, 'run_fetches', return_value=[[], []])
    ds = build_ds('users')

    df = con._retrieve_data(ds)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (0, 4)
    assert list(df.columns) == columns_for_teams


def test_run_fetches_for_tags(con, mocker):
    """Tests the loop generator function for tags call"""
    print('run fetches for tags')
    dataset = 'tags'
    spy = mocker.spy(AircallConnector, 'run_fetches_for_tags')
    ds = build_ds(dataset)
    con.run_fetches_for_tags(ds.dataset, {}, 1)
    assert spy.call_count == 1


def test_run_fetches(con, mocker):
    """Tests the loop generator function for calls/users call"""
    print('run fetches')
    dataset = 'users'
    spy = mocker.spy(AircallConnector, 'run_fetches')
    ds = build_ds(dataset)
    con.run_fetches(ds.dataset, {}, 1)
    assert spy.call_count == 1


def test__retrieve_data_no_teams_case(con, mocker):
    """Tests case when there is no team data but there is calls data"""
    run_fetches_mock = mocker.patch.object(
        AircallConnector, 'run_fetches', return_value=[[], filtered_calls]
    )
    ds = build_ds('calls')
    df = con._retrieve_data(ds)

    # must have calls and still have a team column even if everything is NaN
    assert run_fetches_mock.call_count == 1
    assert df.shape == (10, 11)
    assert df['team'].isna().any()


def test__retrieve_tags_from_fetch(con, mocker):
    """Tests _retrieve_tages from the fetch_page function on"""
    fake_res = handle_mock_data(fake_tags)
    mocker.patch(fetch_fn_name, return_value=fake_res)
    dataset = 'tags'
    ds = build_ds(dataset)

    df = con._retrieve_data(ds)
    assert df.shape == (3, 4)

    mocker.patch(fetch_fn_name, side_effect=Exception('Oh noez !!!'))

    with pytest.raises(Exception):
        con._retrieve_data(ds)


def test__retrieve_users_from_fetch(con, mocker):
    """Tests _retrieve_data for users from fetch_page function on"""
    fake_res = handle_mock_data([fake_teams, fake_users])
    mocker.patch(fetch_fn_name, side_effect=fake_res)
    dataset = 'users'
    ds = build_ds(dataset)

    df = con._retrieve_data(ds)
    assert df.shape == (11, 4)
    mocker.patch(fetch_fn_name, side_effect=Exception('Youch!'))
    with pytest.raises(Exception):
        con._retrieve_data(ds)


def test_bad_limit():
    """Tests case when user passes a bad limit"""
    # limit less than 1 (want to be able to do at least one run)
    with pytest.raises(ValidationError):
        AircallDataSource(name='bar', domain='test_domain', dataset='tags', limit=-6)


def test_default_limit(con, mocker):
    """If no limit is provided, the default is chosen"""
    ds = AircallDataSource(name='mah_ds', domain='test_domain', dataset='tags')
    mock_run_fetches_for_tags = mocker.patch.object(
        AircallConnector, 'run_fetches_for_tags', return_value=filtered_tags
    )

    con._retrieve_data(ds)
    assert ds.limit == 60
    assert mock_run_fetches_for_tags.call_count == 1
    assert mock_run_fetches_for_tags.call_args[0][2] == 60
