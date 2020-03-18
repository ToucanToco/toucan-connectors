"""Module containing helpers for the Aircall connector"""

from typing import List


def process_team_info_on_users(teams: List[dict]) -> List[dict]:
    """takes raw team data and adds team field to each user in team"""
    pool_of_team_users = []

    for team in teams:
        teams_users = team["users"]

        for user in teams_users:
            user.update({"team" : team["name"]})
            pool_of_team_users.append(user)

    return pool_of_team_users


def build_full_user_list(users: List[dict], teams: List[dict]) -> List[dict]:
    """builds a list of users from two separate Aircall API requests"""
    pool_of_users = []

    if len(teams) > 0:
        pool_of_users = process_team_info_on_users(teams)
        team_user_ids = [team_user["id"] for team_user in pool_of_users]

        for user in users:
            if user["id"] not in team_user_ids:
                user["team"] = None
                pool_of_users.append(user)
    else:
        pool_of_users = users

    return pool_of_users


def reshape_users_in_calls(calls):
    updated_calls = []
    for call in calls:
        user = call.get("user", None)
        # print("user ", user)
        if user:
            # print("there's a user")
            new_user = {
                "id": user["id"],
                "name": user["name"]
            }
            call["user"] = new_user
        updated_calls.append(call)
    return updated_calls
