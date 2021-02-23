"""Module containing helpers for the Aircall connector"""
from typing import List

import numpy as np
import pandas as pd

from .constants import COLUMN_DICTIONARY


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
        df = (
            team_data.merge(call_data, sort=False, on='user_id', how='right')
            .drop(columns=['user_name_y', 'user_created_at'])
            .assign(user_name=lambda x: x['user_name_x'])
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
            .assign(
                user_created_at=lambda x: x['user_created_at'].str[:10],
                team=lambda x: x['team'].replace({np.NaN: 'NO TEAM'}),
            )
        )
    elif dataset == 'calls':
        empty_df, team_data, call_data = list_of_data

        df = resolve_calls_df(team_data, call_data)

        total_df = pd.concat([empty_df, df], sort=False, ignore_index=True).assign(
            answered_at=lambda t: pd.to_datetime(t['answered_at'], unit='s'),
            ended_at=lambda t: pd.to_datetime(t['ended_at'], unit='s'),
            day=lambda t: t['ended_at'].astype(str).str[:10],
            team=lambda x: x['team'].replace({np.NaN: 'NO TEAM'}),
        )
        return total_df[COLUMN_DICTIONARY[dataset]]


def build_empty_df(dataset: str) -> pd.DataFrame:
    """Provides column headers for empty dataframes"""
    return pd.DataFrame(columns=COLUMN_DICTIONARY[dataset])


def format_calls_data(call_obj: dict) -> dict:
    """Provides a filter for calls"""
    if call_obj:
        return {
            'id': call_obj.get('id'),
            'direction': call_obj.get('direction'),
            'duration': call_obj.get('duration'),
            'answered_at': call_obj.get('answered_at'),
            'ended_at': call_obj.get('ended_at'),
            'user_id': call_obj.get('user').get('id') if call_obj.get('user') else None,
            'tags': [tag.get('name') for tag in call_obj['tags']],
            'user_name': call_obj.get('user').get('name') if call_obj.get('user') else None,
        }


def format_teams_data(team_obj: dict):
    """Provides a filter for teams"""
    if team_obj:
        return list(
            map(
                lambda user: {
                    'team': team_obj.get('name'),
                    'user_id': user.get('id'),
                    'user_name': user.get('name'),
                    'user_created_at': user.get('created_at'),
                },
                team_obj.get('users'),
            )
        )


def format_users_data(user_obj: dict) -> dict:
    """Provides a filter for users"""
    if user_obj:
        return {
            'user_id': user_obj.get('id'),
            'user_name': user_obj.get('name'),
            'user_created_at': user_obj.get('created_at'),
        }


DICTIONARY_OF_FORMATTERS = {
    'calls': format_calls_data,
    'teams': format_teams_data,
    'users': format_users_data,
}
