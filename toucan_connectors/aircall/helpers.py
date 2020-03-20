"""Module containing helpers for the Aircall connector"""

import pandas as pd

from typing import List

from .constants import COLUMN_DICTIONARY


def build_empty_df(dataset: str):
    return pd.DataFrame(columns=COLUMN_DICTIONARY[dataset])


def reshape_users_in_calls(calls):
    updated_calls = []
    for call in calls:
        user = call.get('user', None)
        # print("user ", user)
        if user:
            # print("there's a user")
            new_user = {
                'id': user['id'],
                'name': user['name']
            }
            call['user'] = new_user
        updated_calls.append(call)
    return updated_calls


def generate_users_jq_filters(dataset: str) -> List[str]:
    if dataset == 'users':
        teams_jq_filter = '[.teams[] | .name as $team | .users[] | {user_name: .name , team: $team, user_id: .id, user_created_at: .created_at}]'
        users_jq_filter = '[.users[] | {user_name: .name, user_id: .id, user_created_at: .created_at}]'
        return [teams_jq_filter, users_jq_filter]


def generate_single_jq_filters(dataset: str) -> str:
    return f'.{dataset}'
