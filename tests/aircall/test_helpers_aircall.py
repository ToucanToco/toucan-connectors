"""Module containing tests for AirCall helpers"""
import pandas as pd

from tests.aircall.mock_results import (
    fake_calls,
    fake_calls_no_user,
    fake_calls_none,
    fake_teams,
    fake_teams_none,
    fake_users,
    fake_users_none,
    filtered_calls,
    filtered_teams,
    filtered_users,
    more_filtered_teams,
    more_filtered_users,
)
from toucan_connectors.aircall.helpers import (
    DICTIONARY_OF_FORMATTERS,
    build_df,
    build_empty_df,
    resolve_calls_df,
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


def test_resolve_calls_df():
    """Tests if resolver for calls works"""
    teams_df = pd.DataFrame(filtered_teams)
    calls_df = pd.DataFrame(filtered_calls)

    # tests result for data in both dataframes
    full_df = resolve_calls_df(teams_df, calls_df)
    assert full_df.shape == (10, 10)

    empty_df = pd.DataFrame([])

    # tests empty teams case
    empty_teams_df = resolve_calls_df(empty_df, calls_df)
    assert empty_teams_df.shape == (10, 8)

    # tests empty calls case
    empty_calls_df = resolve_calls_df(teams_df, empty_df)
    assert empty_calls_df.shape == (4, 4)

    # tests no data case
    empty_data = resolve_calls_df(empty_df, empty_df)
    assert empty_data.shape == (0, 0)


def test_build_users_df():
    """Tests dataframes built with filtered users data"""
    empty_df = build_empty_df('users')
    empty_var_df = pd.DataFrame([])
    order_of_columns = ['team', 'user_id', 'user_name', 'user_created_at']
    teams_df = pd.DataFrame(filtered_teams)
    users_df = pd.DataFrame(filtered_users)

    # teams and users arrays are filled
    fake_list_of_data_1 = [empty_df, teams_df, users_df]
    df_1 = build_df('users', fake_list_of_data_1)

    assert df_1.shape == (6, 4)
    assert list(df_1.columns) == order_of_columns
    assert 'NO TEAM' in df_1['team'].values
    assert df_1['team'].isin(['NO TEAM']).sum() == 2

    # only empty arrays
    fake_list_of_data_2 = [empty_df, empty_var_df, empty_var_df]
    df_2 = build_df('users', fake_list_of_data_2)

    assert df_2.shape == (0, 4)
    assert list(df_2.columns) == order_of_columns

    # empty teams array, filled users
    fake_list_of_data_3 = [empty_df, empty_var_df, pd.DataFrame(filtered_users)]
    df_3 = build_df('users', fake_list_of_data_3)

    assert df_3.shape == (6, 4)
    assert df_3['team'].isin(['NO TEAM']).all()

    # filled teams array, empty users - NOTE: normally this should never occur
    fake_list_of_data_4 = [empty_df, teams_df, empty_var_df]
    df_4 = build_df('users', fake_list_of_data_4)

    assert df_4.shape == (4, 4)
    assert not df_4['team'].isna().any()


def test_build_calls_df():
    """Tests dataframes built with filtered calls data"""
    empty_df = build_empty_df('calls')
    empty_var_df = pd.DataFrame([])
    teams_df = pd.DataFrame(filtered_teams)
    calls_df = pd.DataFrame(filtered_calls)

    # teams and calls arrays are filled
    fake_list_of_data_1 = [empty_df, teams_df, calls_df]
    df_1 = build_df('calls', fake_list_of_data_1)
    assert df_1.shape == (10, 10)
    assert list(df_1.columns) == columns_for_calls

    # filled teams array, empty calls
    fake_list_of_data_2 = [empty_df, teams_df, empty_var_df]
    df_2 = build_df('calls', fake_list_of_data_2)
    assert df_2.shape == (4, 10)

    # empty teams array, filled calls
    fake_list_of_data_3 = [empty_df, empty_var_df, calls_df]
    df_3 = build_df('calls', fake_list_of_data_3)
    assert df_3.shape == (10, 10)
    assert df_3['team'].isin(['NO TEAM']).all()

    # empty arrays
    fake_list_of_data_4 = [empty_df, empty_var_df, empty_var_df]
    df_4 = build_df('calls', fake_list_of_data_4)

    assert df_4.shape == (0, 10)


def test_build_empty_df():
    """Tests the empty dataframe builder"""
    empty_df = build_empty_df('calls')

    assert empty_df.shape == (0, 10)
    assert list(empty_df.columns) == columns_for_calls


def test_format_calls_data():
    """Tests format calls data filter function"""
    # we are only interested in the first call, which is embedded in a response like:
    # [{'calls': [{...call data...}]}]
    first_data_point = fake_calls[0]['calls'][0]
    first_filtered_obj = filtered_calls[0]
    result = DICTIONARY_OF_FORMATTERS['calls'](first_data_point)
    assert result == first_filtered_obj

    # we want to check that no user defined in call result won't generate an error
    first_data_point_no_user = fake_calls_no_user[0]['calls'][0]
    DICTIONARY_OF_FORMATTERS['calls'](first_data_point_no_user)

    first_data_point_none = fake_calls_none[0]['calls'][0]
    DICTIONARY_OF_FORMATTERS['calls'](first_data_point_none)


def test_format_teams_data():
    """Tests format teams data filter function"""
    # we are only interested in the first team, which is embedded in a response like:
    # [{'teams': [{...team data...}]}]
    first_data_point = fake_teams[0]['teams'][0]
    # only want first 5 elements of the more_filtered_teams array
    # in other words, those belonging to Team 1
    filtered_team_users = more_filtered_teams[:5]
    result = DICTIONARY_OF_FORMATTERS['teams'](first_data_point)
    assert result == filtered_team_users

    first_data_point_none = fake_teams_none[0]['teams'][0]
    DICTIONARY_OF_FORMATTERS['teams'](first_data_point_none)


def test_format_users_data():
    """Tests format users data filter function"""
    # we are only interested in the first user, which is embedded in a response like:
    # [{'user': [{...user data...}]}]
    first_data_point = fake_users[0]['users'][0]
    first_filtered_obj = more_filtered_users[0]
    result = DICTIONARY_OF_FORMATTERS['users'](first_data_point)
    assert result == first_filtered_obj

    first_data_point_none = fake_users_none[0]['users'][0]
    DICTIONARY_OF_FORMATTERS['users'](first_data_point_none)
