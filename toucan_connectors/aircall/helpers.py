"""Module containing helpers for the Aircall connector"""

from typing import List

import pandas as pd

from .constants import COLUMN_DICTIONARY, FILTER_DICTIONARY


def resolve_calls_df(team_data, call_data) -> pd.DataFrame:
    """
    Resolves the shape of calls df depending on response

    - if there is both team data and calls data, then merge the dataframes
    on user_id
    - if there is no team data but there is call data,
      then return the call data df
    - if there is team data but no call data, then return the team df
    - if there is nothing, then just return an empty df
    This is to prevent bugs due to pandas dataframe merge
    (not as permissive as dataframe concatenation with empty dataframes)
    """
    df = pd.DataFrame([])

    if len(team_data) > 0 and len(call_data):
        df = team_data.merge(call_data, sort=False, on='user_id', how='right').drop(
            columns=['user_name_y', 'user_created_at']
        )
    elif len(call_data) > 0:
        df = call_data
    elif len(team_data) > 0:
        df = team_data
    return df


def build_df(dataset: str, list_of_data: List[dict]) -> pd.DataFrame:
    """
    builds a dataframe for the users and calls datasets

    dataset is the identifier for the dataset we're fetching
    for example, 'calls' searches data from /teams and /calls
    list_of_data is a list of three dataframes:
    - an empty dataframe built for the specific dataset
    - team data dataframe (if any)
    - either user data or call data dataframe (if any)
    """
    if dataset == 'users':
        return (
            pd.concat(list_of_data, sort=False, ignore_index=True)
            .drop_duplicates(['user_id'], keep='first')
            .assign(**{'user_created_at': lambda x: x['user_created_at'].str[:10]})
        )
    elif dataset == 'calls':
        empty_df, team_data, call_data = list_of_data

        df = resolve_calls_df(team_data, call_data)

        total_df = pd.concat([empty_df, df], sort=False, ignore_index=True).assign(
            **{
                'answered_at': lambda t: pd.to_datetime(t['answered_at'], unit='s'),
                'ended_at': lambda t: pd.to_datetime(t['ended_at'], unit='s'),
                'day': lambda t: t['ended_at'].astype(str).str[:10],
            }
        )
        return total_df[COLUMN_DICTIONARY[dataset]]


def build_empty_df(dataset: str) -> pd.DataFrame:
    """Provides column headers for empty dataframes"""
    return pd.DataFrame(columns=COLUMN_DICTIONARY[dataset])


def generate_multiple_jq_filters(dataset: str) -> List[str]:
    """
    Provides two separate jq filters;
    used in calls and users datasets
    """

    teams_jq_filter: str = FILTER_DICTIONARY['teams']

    # NOTE: 'users' is the default dataset
    variable_jq_filter: str = FILTER_DICTIONARY.get(dataset, FILTER_DICTIONARY['users'])

    return [teams_jq_filter, variable_jq_filter]


def generate_tags_filter(dataset: str) -> str:
    """Provides a single, simple jq filter; only for tags call"""
    return FILTER_DICTIONARY.get(dataset, 'tags')
