team_data = {
    'teams': [
        {
            'id': 1,
            'name': 'Team 1',
            'direct_link': 'https: //api.aircall.io/v1/teams/1',
            'created_at': '2019-08-27T09:13:25.000Z',
            'users': [
                {
                    'id': 10,
                    'direct_link': 'https://api.aircall.io/v1/users/10',
                    'name': 'Jane Doe',
                    'email': 'jane.doe@eg.com',
                    'available': True,
                    'availability_status': 'available',
                    'created_at': '2019-04-26T14:41:09.000Z'
                },
                {
                    'id': 11,
                    'direct_link': 'https://api.aircall.io/v1/users/11',
                    'name': 'John Doe',
                    'email': 'john.doe@eg.com',
                    'available': True,
                    'availability_status': 'available',
                    'created_at': '2019-08-28T07:11:28.000Z'
                },
                {
                    'id': 12,
                    'direct_link': 'https://api.aircall.io/v1/users/12',
                    'name': 'Laleli Lolu',
                    'email': 'laleli.lolu@eg.com',
                    'available': True,
                    'availability_status': 'available',
                    'created_at': '2019-08-28T07:12:17.000Z'
                },
                {
                    'id': 13,
                    'direct_link': 'https://api.aircall.io/v1/users/13',
                    'name': 'Patati Patata',
                    'email': 'patati.patata@eg.com',
                    'available': True,
                    'availability_status': 'custom',
                    'created_at': '2019-08-28T07:14:25.000Z'
                },
                {
                    'id': 14,
                    'direct_link': 'https://api.aircall.io/v1/users/14',
                    'name': 'Jean Dupont',
                    'email': 'jean.dupont@eg.com',
                    'available': True,
                    'availability_status': 'custom',
                    'created_at': '2020-02-12T11:00:25.000Z'
                }
            ]
        },
        {
            'id': 2,
            'name': 'Team 2',
            'direct_link': 'https://api.aircall.io/v1/teams/2',
            'created_at': '2019-10-30T09:19:29.000Z',
            'users': [
                {
                    'id': 15,
                    'direct_link': 'https://api.aircall.io/v1/users/15',
                    'name': 'Seymour Butts',
                    'email': 'seymour.butts@eg.com',
                    'available': True,
                    'availability_status': 'available',
                    'created_at': '2019-06-03T15:49:29.000Z'
                },
                {
                    'id': 16,
                    'direct_link': 'https://api.aircall.io/v1/users/16',
                    'name': 'Robin Hood',
                    'email': 'robin.hood@eg.com',
                    'available': True,
                    'availability_status': 'available',
                    'created_at': '2019-06-03T16:08:14.000Z'
                }
            ]
        }
    ],
    'meta': {
        'count': 2,
        'total': 2,
        'current_page': 1,
        'per_page': 20,
        'next_page_link': None,
        'previous_page_link': None
    }
}

empty_team_users = {
    'teams': [
        {
            'id': 1,
            'name': 'Team 1',
            'direct_link': 'https: //api.aircall.io/v1/teams/1',
            'created_at': '2019-08-27T09:13:25.000Z',
            'users': []
        },
        {
            'id': 2,
            'name': 'Team 2',
            'direct_link': 'https://api.aircall.io/v1/teams/2',
            'created_at': '2019-10-30T09:19:29.000Z',
            'users': []
        }
    ],
    'meta': {
        'count': 2,
        'total': 2,
        'current_page': 1,
        'per_page': 20,
        'next_page_link': None,
        'previous_page_link': None
    }
}

empty_teams = {
    'teams': [],
    'meta': {
        'count': 2,
        'total': 2,
        'current_page': 1,
        'per_page': 20,
        'next_page_link': None,
        'previous_page_link': None
    }
}

user_json = {
    'users': [
        {
            'id': 10,
            'direct_link': 'https: //api.aircall.io/v1/users/10',
            'name': 'Jane Doe',
            'email': 'jane.doe@eg.com',
            'available': True,
            'availability_status': 'available',
            'created_at': '2019-04-26T14:41:09.000Z'
        },
        {
            'id': 17,
            'direct_link': 'https://api.aircall.io/v1/users/17',
            'name': 'Mini Me',
            'email': 'mini.me@eg.com',
            'available': False,
            'availability_status': 'custom',
            'created_at': '2019-06-03T15:45:49.000Z'
        },
        {
            'id': 18,
            'direct_link': 'https://api.aircall.io/v1/users/18',
            'name': 'Doctor Evil',
            'email': 'doctor.evil@eg.com',
            'available': True,
            'availability_status': 'available',
            'created_at': '2019-06-03T15:47:51.000Z'
        },
        {
            'id': 15,
            'direct_link': 'https://api.aircall.io/v1/users/15',
            'name': 'Seymour Butts',
            'email': 'seymour.butts@eg.com',
            'available': False,
            'availability_status': 'available',
            'created_at': '2019-06-03T15:49:29.000Z'
        },
        {
            'id': 16,
            'direct_link': 'https://api.aircall.io/v1/users/16',
            'name': 'Robin Hood',
            'email': 'robin.hood@eg.com',
            'available': True,
            'availability_status': 'available',
            'created_at': '2019-06-03T16:08:14.000Z'
        },
        {
            'id': 11,
            'direct_link': 'https://api.aircall.io/v1/users/11',
            'name': 'John Doe',
            'email': 'john.doe@eg.com',
            'available': True,
            'availability_status': 'available',
            'created_at': '2019-08-28T07:11:28.000Z'
        },
        {
            'id': 12,
            'direct_link': 'https://api.aircall.io/v1/users/12',
            'name': 'Laleli Lolu',
            'email': 'laleli.lolu@eg.com',
            'available': True,
            'availability_status': 'available',
            'created_at': '2019-08-28T07:12:17.000Z'
        },
        {
            'id': 13,
            'direct_link': 'https://api.aircall.io/v1/users/13',
            'name': 'Patati Patata',
            'email': 'patati.patata@eg.com',
            'available': False,
            'availability_status': 'custom',
            'created_at': '2019-08-28T07:14:25.000Z'
        },
        {
            'id': 19,
            'direct_link': 'https://api.aircall.io/v1/users/19',
            'name': 'Fefifo Fum',
            'email': 'fefifo.fum@eg.com',
            'available': True,
            'availability_status': 'available',
            'created_at': '2019-09-02T07:13:28.000Z'
        },
        {
            'id': 14,
            'direct_link': 'https://api.aircall.io/v1/users/14',
            'name': 'Jean Dupont',
            'email': 'jean.dupont@eg.com',
            'available': False,
            'availability_status': 'custom',
            'created_at': '2020-02-12T11:00:25.000Z'
        },
        {
            'id': 20,
            'direct_link': 'https://api.aircall.io/v1/users/20',
            'name': 'Jeron Imo',
            'email': 'jeron.imo@eg.com',
            'available': False,
            'availability_status': 'custom',
            'created_at': '2020-03-11T14:11:13.000Z'
        }
    ],
    'meta': {
        'count': 11,
        'total': 11,
        'current_page': 1,
        'per_page': 20,
        'next_page_link': None,
        'previous_page_link': None
    }
}

empty_users = {'users' : []}
