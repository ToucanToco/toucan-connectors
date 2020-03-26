import pandas as pd
# import pytest

from tests.aircall.mock_results import (
    filtered_teams,
    filtered_users,
    team_data, user_data
)
from toucan_connectors.aircall.helpers import build_df, build_empty_df


def test_build_df():
    """Tests dataframes being with filtered data"""
    fake_list_of_data = [build_empty_df('users'), pd.DataFrame(filtered_teams), pd.DataFrame(filtered_users)]
    # print(fake_list_of_data)
    df_1 = build_df('users', fake_list_of_data)

    assert len(df_1) == 6
    assert df_1.shape == (6, 4)
    assert list(df_1.columns) == ['user_id', 'user_name', 'team', 'user_created_at']
