import pandas as pd
# import pytest

from tests.aircall.mock_results import (
    empty_teams, empty_users,
    filtered_teams,
    filtered_users,
    team_data, user_data
)
from toucan_connectors.aircall.helpers import build_df, build_empty_df


def test_build_df():
    """Tests dataframes being with filtered data"""
    empty_df = build_empty_df('users')
    empty_var_df = pd.DataFrame([])
    order_of_columns = ['user_id', 'user_name', 'team', 'user_created_at']
    teams_df = pd.DataFrame(filtered_teams)
    users_df = pd.DataFrame(filtered_users)

    # teams and users arrays are filled
    fake_list_of_data_1 = [empty_df, teams_df, users_df]
    df_1 = build_df('users', fake_list_of_data_1)

    assert df_1.shape == (6, 4)
    assert list(df_1.columns) == order_of_columns
    assert df_1['team'].isna().sum() == 2

    # only empty arrays
    fake_list_of_data_2 = [empty_df, empty_var_df, empty_var_df]
    df_2 = build_df('users', fake_list_of_data_2)

    assert df_2.shape == (0, 4)
    assert list(df_2.columns) == order_of_columns

    # empty teams array, filled users
    fake_list_of_data_3 = [empty_df, empty_var_df, pd.DataFrame(filtered_users)]
    df_3 = build_df('users', fake_list_of_data_3)

    assert df_3.shape == (6, 4)
    assert df_3['team'].isna().all()

    # filled teams array, empty users - NOTE: normally this should never occur
    fake_list_of_data_4 = [empty_df, teams_df, empty_var_df]
    df_4 = build_df('users', fake_list_of_data_4)

    assert df_4.shape == (4, 4)
    assert not df_4['team'].isna().any()
