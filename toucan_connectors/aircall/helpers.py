"""Module containing helpers for the Aircall connector"""

import pandas as pd

from typing import List

from .constants import COLUMN_DICTIONARY


def build_empty_df(dataset: str):
    return pd.DataFrame(columns=COLUMN_DICTIONARY[dataset])


def generate_users_jq_filters(dataset: str) -> List[str]:
    teams_jq_filter = '[.teams[] | .name as $team | .users[] | {user_name: .name , team: $team, user_id: .id, user_created_at: .created_at}]'
    variable_jq_filter = None

    if dataset == 'users':
        variable_jq_filter = '[.users[] | {user_name: .name, user_id: .id, user_created_at: .created_at}]'
    else:
        variable_jq_filter = """
        .calls
        | map({
            id,
            direction,
            duration,
            answered_at,
            ended_at,
            raw_digits,
            user_id: .user.id,
            tags : .tags | map({name}),
            user_name: .user.name
        })
        """

    return [teams_jq_filter, variable_jq_filter]


def generate_single_jq_filters(dataset: str) -> str:
    return f'.{dataset}'
