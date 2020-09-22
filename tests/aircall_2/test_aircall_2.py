from functools import partial

import pytest
from pydantic import ValidationError

import tests.general_helpers as helpers
from tests.aircall.mock_results import (
    fake_tags,
    fake_teams,
    fake_users,
    filtered_calls,
    filtered_tags,
    filtered_teams,
    filtered_users,
    more_filtered_teams,
    more_filtered_users,
)
from toucan_connectors.aircall_2.aircall_2_connector import (
    Aircall2Connector,
    Aircall2DataSource,
    AircallError,
)

columns_for_calls = [
    'id',
    'direction',
    'duration',
    'answered_at',
    'ended_at',
    'user_id',
    'tags',
    'user_name',
    'team',
    'day',
]
columns_for_tags = ['id', 'name', 'color', 'description']
columns_for_teams = ['team', 'user_id', 'user_name', 'user_created_at']
columns_for_users = ['user_id', 'user_name', 'user_created_at']

fetch_fn_name = 'toucan_connectors.aircall_2.aircall_2_connector.fetch_page'


@pytest.fixture
def con():
    return Aircall2Connector(name='test_name')


@pytest.fixture
def access_token():
    return 'myaccesstoken'


@pytest.fixture
def fake_kwargs(access_token):
    def fake_fetch_secrets(small_app_id, connector_type, auth_flow_id):
        return {'access_token': access_token}

    return {'secrets': partial(fake_fetch_secrets, 'laputa', 'Aircall2')}


def build_ds(dataset: str):
    """Builds test datasource"""
    return Aircall2DataSource(
        name='mah_ds',
        domain='mah_domain',
        dataset=dataset,
        limit=1,
    )


@pytest.mark.asyncio
async def test__get_data_tags_case(con, mocker, access_token):
    """Tests with tags happy case"""
    dataset = 'tags'
    fake_res = helpers.build_future(fake_tags)
    fake_fetch_page = mocker.patch(fetch_fn_name, return_value=fake_res)
    ds = build_ds(dataset)
    res = await con._get_tags(ds.dataset, 10, access_token)

    assert fake_fetch_page.call_count == 1
    assert res == filtered_tags


@pytest.mark.asyncio
async def test__get_data_tags_unhappy_case(con, mocker, access_token):
    """Tests what happens when tags call returns an error"""
    dataset = 'tags'
    mocker.patch(fetch_fn_name, side_effect=AircallError('OMGERD OOPS!!!'))
    ds = build_ds(dataset)
    with pytest.raises(AircallError) as err:
        await con._get_tags(ds.dataset, 10, access_token)
    assert str(err.value) == 'OMGERD OOPS!!!'


@pytest.mark.asyncio
async def test__get_data_users_case(con, mocker, access_token):
    """Tests users call happy case"""
    dataset = 'users'
    fake_res = [fake_teams, fake_users]
    fake_res = [helpers.build_future(item) for item in fake_res]
    fake_fetch_page = mocker.patch(fetch_fn_name, side_effect=fake_res)
    ds = build_ds(dataset)
    res = await con._get_data(ds.dataset, 10, access_token)

    assert fake_fetch_page.call_count == 2
    assert len(res) == 2
    teams, users = res
    assert teams == more_filtered_teams
    assert list(teams[0].keys()) == columns_for_teams
    assert users == more_filtered_users
    assert list(users[0].keys()) == columns_for_users


def test__retrieve_data_users_happy_case(con, mocker, fake_kwargs):
    """Tests case when users call has data"""
    # NOTE: this test is only cursory because 'users' call is tested more
    # thoroughly in helpers test file
    run_fetches_mock = mocker.patch.object(
        Aircall2Connector, 'run_fetches', return_value=[filtered_teams, filtered_users]
    )
    ds = build_ds('users')
    df = con._retrieve_data(ds, **fake_kwargs)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (6, 4)

    assert list(df.columns) == columns_for_teams


def test__retrieve_data_calls_happy_case(con, mocker, fake_kwargs):
    """Tests case when calls call has data"""
    run_fetches_mock = mocker.patch.object(
        Aircall2Connector, 'run_fetches', return_value=[filtered_teams, filtered_calls]
    )
    ds = build_ds('calls')

    df = con._retrieve_data(ds, **fake_kwargs)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (10, 10)
    assert list(df.columns) == columns_for_calls
    assert df['team'].isna().sum() == 0
    assert df['team'].eq('Team 1').sum() == 6
    assert df['team'].eq('Team 2').sum() == 4


def test__retrieve_data_tags_happy_case(con, mocker, fake_kwargs):
    """Tests case when tags call has data"""
    run_fetches_mock = mocker.patch.object(
        Aircall2Connector, 'run_fetches_for_tags', return_value=filtered_tags
    )
    ds = build_ds('tags')

    df = con._retrieve_data(ds, **fake_kwargs)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (3, 4)
    assert list(df.columns) == columns_for_tags


def test__retrieve_data_no_data_case(con, mocker, fake_kwargs):
    """Tests case when there is no data returned"""
    run_fetches_mock = mocker.patch.object(Aircall2Connector, 'run_fetches', return_value=[[], []])
    ds = build_ds('users')

    df = con._retrieve_data(ds, **fake_kwargs)
    assert run_fetches_mock.call_count == 1
    assert df.shape == (0, 4)
    assert list(df.columns) == columns_for_teams


def test__retrieve_data_no_teams_case(con, mocker, fake_kwargs):
    """Tests case when there is no team data but there is calls data"""
    run_fetches_mock = mocker.patch.object(
        Aircall2Connector, 'run_fetches', return_value=[[], filtered_calls]
    )
    ds = build_ds('calls')
    df = con._retrieve_data(ds, **fake_kwargs)

    # must have calls and still have a team column even if everything is NaN
    assert run_fetches_mock.call_count == 1
    assert df.shape == (10, 10)
    assert df['team'].isin(['NO TEAM']).all()


def test__retrieve_tags_from_fetch(con, mocker, fake_kwargs):
    """Tests _retrieve_tags from the fetch_page function"""
    fake_res = fake_tags
    fake_res = helpers.build_future(fake_tags)
    mocker.patch(fetch_fn_name, return_value=fake_res)
    ds = build_ds('tags')

    df = con._retrieve_data(ds, **fake_kwargs)
    assert df.shape == (3, 4)

    mocker.patch(fetch_fn_name, side_effect=AircallError('Oh noez !!!'))

    with pytest.raises(AircallError) as err:
        con._retrieve_data(ds, **fake_kwargs)
    assert str(err.value) == 'Oh noez !!!'


def test__retrieve_users_from_fetch(con, mocker, fake_kwargs):
    """Tests _retrieve_data for users from fetch_page function"""
    fake_res = [fake_teams, fake_users]
    fake_res = [helpers.build_future(item) for item in fake_res]
    mocker.patch(fetch_fn_name, side_effect=fake_res)
    ds = build_ds('users')

    df = con._retrieve_data(ds, **fake_kwargs)
    assert df.shape == (11, 4)
    mocker.patch(fetch_fn_name, side_effect=AircallError('Youch!'))
    with pytest.raises(AircallError):
        con._retrieve_data(ds, **fake_kwargs)


def test_bad_limit():
    """Tests case when user passes a bad limit"""
    # limit less than 1 (want to be able to do at least one run)
    with pytest.raises(ValidationError):
        Aircall2DataSource(name='bar', domain='test_domain', dataset='tags', limit=-6)


def test_default_limit(con, mocker, fake_kwargs):
    """If no limit is provided, the default is chosen"""
    ds = Aircall2DataSource(name='mah_ds', domain='test_domain', dataset='tags')
    mock_run_fetches_for_tags = mocker.patch.object(
        Aircall2Connector, 'run_fetches_for_tags', return_value=filtered_tags
    )

    con._retrieve_data(ds, **fake_kwargs)
    assert ds.limit == 1
    assert mock_run_fetches_for_tags.call_count == 1
    assert mock_run_fetches_for_tags.call_args[0][1] == 1


def test_limit_of_zero(con, mocker, fake_kwargs):
    """A limit of zero triggers no fetch"""
    # Test the calls/users branch
    ds_calls = Aircall2DataSource(name='bar', domain='test_domain', dataset='calls', limit=0)
    spy_calls = mocker.spy(Aircall2Connector, 'run_fetches')
    con._retrieve_data(ds_calls, **fake_kwargs)
    assert spy_calls.call_count == 0

    # Test the tags branch
    ds_tags = Aircall2DataSource(name='baz', domain='test_domain', dataset='calls', limit=0)
    spy_tags = mocker.spy(Aircall2Connector, 'run_fetches_for_tags')
    con._retrieve_data(ds_tags, **fake_kwargs)
    assert spy_tags.call_count == 0


def test_datasource():
    """Tests that default dataset on datasource is 'calls'"""
    ds = Aircall2DataSource(
        name='mah_ds',
        domain='test_domain',
        limit=1,
    )
    assert ds.dataset == 'calls'
