# import pytest

from toucan_connectors.aircall.helpers import build_full_user_list

fake_users = [
    {
        "id": 456,
        "direct_link": "https://api.aircall.io/v1/users/456",
        "name": "Madelaine Dupont",
        "email": "madelaine.dupont@aircall.io",
        "available": "false",
        "numbers": [
            {
                "id": 123,
                "direct_link": "https://api.aircall.io/v1/numbers/123",
                "name": "My first Aircall Number",
                "digits": "+33 1 76 36 06 95",
                "country": "FR",
                "time_zone": "Europe/Paris",
                "open": True
            }
        ],
        "created_at": "2016-10-26T08:20:53.000Z"
    },
    {
        "id": 457,
        "direct_link": "https://api.aircall.io/v1/users/456",
        "name": "Jeanne Deau",
        "email": "jeanne.deau@aircall.io",
        "available": "false",
        "numbers": [
            {
                "id": 456,
                "direct_link": "https://api.aircall.io/v1/numbers/456",
                "name": "My first Aircall Number",
                "digits": "+33 6 66 66 66 66",
                "country": "FR",
                "time_zone": "Europe/Paris",
                "open": True
            }
        ],
        "created_at": "2016-10-26T08:20:53.000Z"
    },
    {
        "id": 458,
        "direct_link": "https://api.aircall.io/v1/users/458",
        "name": "Laleli Lolu",
        "email": "laleli.lolu@aircall.io",
        "available": "false",
        "numbers": [
            {
                "id": 124,
                "direct_link": "https://api.aircall.io/v1/numbers/124",
                "name": "My first Aircall Number",
                "digits": "+33 6 66 66 66 67",
                "country": "FR",
                "time_zone": "Europe/Paris",
                "open": True
            }
        ],
        "created_at": "2016-10-26T08:20:53.000Z"
    },
    {
        "id": 459,
        "direct_link": "https://api.aircall.io/v1/users/459",
        "name": "Patati Patata",
        "email": "patati.patata@aircall.io",
        "available": "false",
        "numbers": [
            {
                "id": 125,
                "direct_link": "https://api.aircall.io/v1/numbers/125",
                "name": "My first Aircall Number",
                "digits": "+33 6 66 66 66 68",
                "country": "FR",
                "time_zone": "Europe/Paris",
                "open": True
            }
        ],
        "created_at": "2016-10-26T08:20:53.000Z"
    }
]

fake_teams = [
    {
        "id": 1,
        "direct_link": "https://api.aircall.io/v1/team/1",
        "name": "Test Team",
        "users": [
            {
                "id": 456,
                "direct_link": "https://api.aircall.io/v1/users/456",
                "name": "Madeleine Dupont",
                "email": "madeleine.dupont@aircall.io",
                "available": "false"
            }
        ],
        "created_at": "2017-04-06T15:07:07.000Z"
    },
    {
        "id": 2,
        "direct_link": "https://api.aircall.io/v1/team/2",
        "name": "Another Test Team",
        "users": [
            {
                "id": 457,
                "direct_link": "https://api.aircall.io/v1/users/457",
                "name": "Jeanne Deau",
                "email": "jeanne.deau@aircall.io",
                "available": "false"
            }
        ],
        "created_at": "2017-04-06T15:07:07.000Z"
    }
]


def test_build_full_user_list():
    """
    Tests the functions that concatenate information from users and teams
    """
    fake_full_user_list = build_full_user_list(fake_users, fake_teams)

    assert len(fake_full_user_list) == 4

    # since we fill up users associated with teams first, we must make sure that the team property is not None
    first_fake_user = fake_full_user_list[0]

    assert first_fake_user.get("team") is not None

    # check that users in common between both lists are not added twice
    fake_dup_user_id = 456
    fake_dup_user_list = [fake_user for fake_user in fake_full_user_list if fake_user["id"] == fake_dup_user_id]

    assert len(fake_dup_user_list) == 1

    # check that the lack of team does not affect the list of users
    fake_user_list_no_team = build_full_user_list(fake_users, [])

    assert len(fake_user_list_no_team) == 4

    # make sure that at least the first user in the list has a "team" property that is None
    first_fake_user = fake_user_list_no_team[0]

    assert first_fake_user.get("team") is None
