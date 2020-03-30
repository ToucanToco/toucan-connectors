import pandas as pd

from tests.aircall.mock_results import (
    filtered_teams,
    filtered_users
)
from toucan_connectors.aircall.helpers import (
    build_df, build_empty_df,
    generate_multiple_jq_filters, generate_tags_filter
)


def test_build_df():
    """Tests dataframes being with filtered data"""
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


def test_build_empty_df():
    """Test the empty dataframe builder"""
    empty_df = build_empty_df('calls')

    assert empty_df.shape == (0, 11)
    assert list(empty_df.columns) == [
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


def test_generate_multiple_jq_filters():
    """Test the multiple jq filter generator"""
    # test a valid dataset
    dataset = 'calls'
    jq_filters = generate_multiple_jq_filters(dataset)

    assert type(jq_filters) == list
    assert len(jq_filters) == 2

    team_filter, variable_filter = jq_filters

    assert 'teams' in team_filter
    assert 'calls' in variable_filter

    # test a bad dataset
    bad_dataset = 'turkeys'

    default_filters = generate_multiple_jq_filters(bad_dataset)

    assert len(jq_filters) == 2

    team_filter, default_filter = default_filters

    assert 'teams' in team_filter
    assert 'users' in default_filter


def test_generate_tags_filter():
    """Tests the tags jq filter generator"""
    dataset = 'tags'

    jq_filter = generate_tags_filter(dataset)

    assert type(jq_filter) == str
    assert 'tags' in jq_filter

    bad_dataset = 'totos'

    default_filter = generate_tags_filter(bad_dataset)

    assert 'tags' in default_filter
