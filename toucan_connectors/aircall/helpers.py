# Module containing helpers for the Aircall connector


def process_team_info_on_users(teams):
    pool_of_team_users = []
    for team in teams:
        teams_users = team["users"]

        for user in teams_users:
            user.update({"team" : team["name"]})
            pool_of_team_users.append(user)

    return pool_of_team_users

# build_full_user_list - this builds a list of users from two separate Aircall API requests
# one request is to /teams and returns an array of objects with a field called "users" that contains a list
# of user objects and another field called "name" that is the name of the team
# the second request contains a list of user objects obtained from calling the /users route
# the rationale behind this is that we obtain a list of all users belonging to a client's Aircall and not just those belonging to a team
# and the team information has been added to each user
# if a user does not belong to a team, their "team" field is null


def build_full_user_list(users, teams):
    pool_of_users = []

    if len(teams) > 0:
        pool_of_users = process_team_info_on_users(teams)
        team_user_ids = [team_user["id"] for team_user in pool_of_users]

        for user in users:
            if user["id"] not in team_user_ids:
                # need to check if this will be accepted by pandas
                user["team"] = None
                pool_of_users.append(user)
    else:
        pool_of_users = users

    return pool_of_users
