fake_teams = [
    {
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
                        'created_at': '2019-04-26T14:41:09.000Z',
                    },
                    {
                        'id': 11,
                        'direct_link': 'https://api.aircall.io/v1/users/11',
                        'name': 'John Doe',
                        'email': 'john.doe@eg.com',
                        'available': True,
                        'availability_status': 'available',
                        'created_at': '2019-08-28T07:11:28.000Z',
                    },
                    {
                        'id': 12,
                        'direct_link': 'https://api.aircall.io/v1/users/12',
                        'name': 'Laleli Lolu',
                        'email': 'laleli.lolu@eg.com',
                        'available': True,
                        'availability_status': 'available',
                        'created_at': '2019-08-28T07:12:17.000Z',
                    },
                    {
                        'id': 13,
                        'direct_link': 'https://api.aircall.io/v1/users/13',
                        'name': 'Patati Patata',
                        'email': 'patati.patata@eg.com',
                        'available': True,
                        'availability_status': 'custom',
                        'created_at': '2019-08-28T07:14:25.000Z',
                    },
                    {
                        'id': 14,
                        'direct_link': 'https://api.aircall.io/v1/users/14',
                        'name': 'Jean Dupont',
                        'email': 'jean.dupont@eg.com',
                        'available': True,
                        'availability_status': 'custom',
                        'created_at': '2020-02-12T11:00:25.000Z',
                    },
                ],
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
                        'created_at': '2019-06-03T15:49:29.000Z',
                    },
                    {
                        'id': 16,
                        'direct_link': 'https://api.aircall.io/v1/users/16',
                        'name': 'Robin Hood',
                        'email': 'robin.hood@eg.com',
                        'available': True,
                        'availability_status': 'available',
                        'created_at': '2019-06-03T16:08:14.000Z',
                    },
                ],
            },
        ],
        'meta': {
            'count': 2,
            'total': 2,
            'current_page': 1,
            'per_page': 20,
            'next_page_link': None,
            'previous_page_link': None,
        },
    }
]

empty_team_users = {
    'teams': [
        {
            'id': 1,
            'name': 'Team 1',
            'direct_link': 'https: //api.aircall.io/v1/teams/1',
            'created_at': '2019-08-27T09:13:25.000Z',
            'users': [],
        },
        {
            'id': 2,
            'name': 'Team 2',
            'direct_link': 'https://api.aircall.io/v1/teams/2',
            'created_at': '2019-10-30T09:19:29.000Z',
            'users': [],
        },
    ],
    'meta': {
        'count': 2,
        'total': 2,
        'current_page': 1,
        'per_page': 20,
        'next_page_link': None,
        'previous_page_link': None,
    },
}

empty_teams = {
    'teams': [],
    'meta': {
        'count': 2,
        'total': 2,
        'current_page': 1,
        'per_page': 20,
        'next_page_link': None,
        'previous_page_link': None,
    },
}

fake_users = [
    {
        'users': [
            {
                'id': 10,
                'direct_link': 'https: //api.aircall.io/v1/users/10',
                'name': 'Jane Doe',
                'email': 'jane.doe@eg.com',
                'available': True,
                'availability_status': 'available',
                'created_at': '2019-04-26T14:41:09.000Z',
            },
            {
                'id': 17,
                'direct_link': 'https://api.aircall.io/v1/users/17',
                'name': 'Mini Me',
                'email': 'mini.me@eg.com',
                'available': False,
                'availability_status': 'custom',
                'created_at': '2019-06-03T15:45:49.000Z',
            },
            {
                'id': 18,
                'direct_link': 'https://api.aircall.io/v1/users/18',
                'name': 'Doctor Evil',
                'email': 'doctor.evil@eg.com',
                'available': True,
                'availability_status': 'available',
                'created_at': '2019-06-03T15:47:51.000Z',
            },
            {
                'id': 15,
                'direct_link': 'https://api.aircall.io/v1/users/15',
                'name': 'Seymour Butts',
                'email': 'seymour.butts@eg.com',
                'available': False,
                'availability_status': 'available',
                'created_at': '2019-06-03T15:49:29.000Z',
            },
            {
                'id': 16,
                'direct_link': 'https://api.aircall.io/v1/users/16',
                'name': 'Robin Hood',
                'email': 'robin.hood@eg.com',
                'available': True,
                'availability_status': 'available',
                'created_at': '2019-06-03T16:08:14.000Z',
            },
            {
                'id': 11,
                'direct_link': 'https://api.aircall.io/v1/users/11',
                'name': 'John Doe',
                'email': 'john.doe@eg.com',
                'available': True,
                'availability_status': 'available',
                'created_at': '2019-08-28T07:11:28.000Z',
            },
            {
                'id': 12,
                'direct_link': 'https://api.aircall.io/v1/users/12',
                'name': 'Laleli Lolu',
                'email': 'laleli.lolu@eg.com',
                'available': True,
                'availability_status': 'available',
                'created_at': '2019-08-28T07:12:17.000Z',
            },
            {
                'id': 13,
                'direct_link': 'https://api.aircall.io/v1/users/13',
                'name': 'Patati Patata',
                'email': 'patati.patata@eg.com',
                'available': False,
                'availability_status': 'custom',
                'created_at': '2019-08-28T07:14:25.000Z',
            },
            {
                'id': 19,
                'direct_link': 'https://api.aircall.io/v1/users/19',
                'name': 'Fefifo Fum',
                'email': 'fefifo.fum@eg.com',
                'available': True,
                'availability_status': 'available',
                'created_at': '2019-09-02T07:13:28.000Z',
            },
            {
                'id': 14,
                'direct_link': 'https://api.aircall.io/v1/users/14',
                'name': 'Jean Dupont',
                'email': 'jean.dupont@eg.com',
                'available': False,
                'availability_status': 'custom',
                'created_at': '2020-02-12T11:00:25.000Z',
            },
            {
                'id': 20,
                'direct_link': 'https://api.aircall.io/v1/users/20',
                'name': 'Jeron Imo',
                'email': 'jeron.imo@eg.com',
                'available': False,
                'availability_status': 'custom',
                'created_at': '2020-03-11T14:11:13.000Z',
            },
        ],
        'meta': {
            'count': 11,
            'total': 11,
            'current_page': 1,
            'per_page': 20,
            'next_page_link': None,
            'previous_page_link': None,
        },
    }
]

empty_users = {'users': []}


empty_calls = {
    'calls': [],
    'meta': {
        'count': 50,
        'total': 6649,
        'current_page': 1,
        'per_page': 50,
        'next_page_link': 'https://api.aircall.io/v1/calls?order=asc&page=2&per_page=50',
        'previous_page_link': None,
    },
}

fake_tags = [
    {
        'tags': [
            {'id': 10000, 'name': 'Tag 1', 'color': '#f00', 'description': 'foo'},
            {'id': 10001, 'name': 'Tag 2', 'color': '#0f0', 'description': 'bar'},
            {'id': 10002, 'name': 'Tag 3', 'color': '#00f', 'description': 'baz'},
        ],
        'meta': {
            'count': 3,
            'total': 3,
            'current_page': 1,
            'per_page': 50,
            'next_page_link': None,
            'previous_page_link': None,
        },
    }
]

filtered_teams = [
    {
        'team': 'Team 1',
        'user_id': 100,
        'user_name': 'User 1',
        'user_created_at': '2020-03-25T14:41:09.000Z',
    },
    {
        'team': 'Team 2',
        'user_id': 200,
        'user_name': 'User 2',
        'user_created_at': '2020-03-25T14:41:09.000Z',
    },
    {
        'team': 'Team 2',
        'user_id': 300,
        'user_name': 'User 3',
        'user_created_at': '2020-03-25T14:41:09.000Z',
    },
    {
        'team': 'Team 3',
        'user_id': 400,
        'user_name': 'User 4',
        'user_created_at': '2020-03-25T14:41:09.000Z',
    },
]

filtered_users = [
    {'user_id': 100, 'user_name': 'User 1', 'user_created_at': '2020-03-25T14:41:09.000Z'},
    {'user_id': 200, 'user_name': 'User 2', 'user_created_at': '2020-03-25T14:41:09.000Z'},
    {'user_id': 300, 'user_name': 'User 3', 'user_created_at': '2020-03-25T14:41:09.000Z'},
    {'user_id': 400, 'user_name': 'User 4', 'user_created_at': '2020-03-25T14:41:09.000Z'},
    {'user_id': 500, 'user_name': 'User 5', 'user_created_at': '2020-03-25T14:41:09.000Z'},
    {'user_id': 600, 'user_name': 'User 6', 'user_created_at': '2020-03-25T14:41:09.000Z'},
]

more_filtered_teams = [
    {
        'team': 'Team 1',
        'user_id': 10,
        'user_name': 'Jane Doe',
        'user_created_at': '2019-04-26T14:41:09.000Z',
    },
    {
        'team': 'Team 1',
        'user_id': 11,
        'user_name': 'John Doe',
        'user_created_at': '2019-08-28T07:11:28.000Z',
    },
    {
        'team': 'Team 1',
        'user_id': 12,
        'user_name': 'Laleli Lolu',
        'user_created_at': '2019-08-28T07:12:17.000Z',
    },
    {
        'team': 'Team 1',
        'user_id': 13,
        'user_name': 'Patati Patata',
        'user_created_at': '2019-08-28T07:14:25.000Z',
    },
    {
        'team': 'Team 1',
        'user_id': 14,
        'user_name': 'Jean Dupont',
        'user_created_at': '2020-02-12T11:00:25.000Z',
    },
    {
        'team': 'Team 2',
        'user_id': 15,
        'user_name': 'Seymour Butts',
        'user_created_at': '2019-06-03T15:49:29.000Z',
    },
    {
        'team': 'Team 2',
        'user_id': 16,
        'user_name': 'Robin Hood',
        'user_created_at': '2019-06-03T16:08:14.000Z',
    },
]

more_filtered_users = [
    {'user_id': 10, 'user_name': 'Jane Doe', 'user_created_at': '2019-04-26T14:41:09.000Z'},
    {'user_id': 17, 'user_name': 'Mini Me', 'user_created_at': '2019-06-03T15:45:49.000Z'},
    {'user_id': 18, 'user_name': 'Doctor Evil', 'user_created_at': '2019-06-03T15:47:51.000Z'},
    {'user_id': 15, 'user_name': 'Seymour Butts', 'user_created_at': '2019-06-03T15:49:29.000Z'},
    {'user_id': 16, 'user_name': 'Robin Hood', 'user_created_at': '2019-06-03T16:08:14.000Z'},
    {'user_id': 11, 'user_name': 'John Doe', 'user_created_at': '2019-08-28T07:11:28.000Z'},
    {'user_id': 12, 'user_name': 'Laleli Lolu', 'user_created_at': '2019-08-28T07:12:17.000Z'},
    {'user_id': 13, 'user_name': 'Patati Patata', 'user_created_at': '2019-08-28T07:14:25.000Z'},
    {'user_id': 19, 'user_name': 'Fefifo Fum', 'user_created_at': '2019-09-02T07:13:28.000Z'},
    {'user_id': 14, 'user_name': 'Jean Dupont', 'user_created_at': '2020-02-12T11:00:25.000Z'},
    {'user_id': 20, 'user_name': 'Jeron Imo', 'user_created_at': '2020-03-11T14:11:13.000Z'},
]


filtered_calls = [
    {
        'id': 1000,
        'direction': 'outbound',
        'duration': 20,
        'answered_at': 1572618630,
        'ended_at': 1572618639,
        'raw_digits': '+1 111-111-1111',
        'user_id': 100,
        'tags': [],
        'user_name': 'User 1',
    },
    {
        'id': 1001,
        'direction': 'outbound',
        'duration': 37,
        'answered_at': 1572618774,
        'ended_at': 1572618784,
        'raw_digits': '+1 111-111-1111',
        'user_id': 100,
        'tags': [],
        'user_name': 'User 1',
    },
    {
        'id': 1002,
        'direction': 'outbound',
        'duration': 10,
        'answered_at': 1572618808,
        'ended_at': 1572618813,
        'raw_digits': '+1 111-111-1112',
        'user_id': 100,
        'tags': [],
        'user_name': 'User 1',
    },
    {
        'id': 1003,
        'direction': 'outbound',
        'duration': 12,
        'answered_at': 1572619086,
        'ended_at': 1572619092,
        'raw_digits': '+1 111-111-1113',
        'user_id': 100,
        'tags': [],
        'user_name': 'User 1',
    },
    {
        'id': 1004,
        'direction': 'outbound',
        'duration': 23,
        'answered_at': 1572619311,
        'ended_at': 1572619316,
        'raw_digits': '+1 111-111-1111',
        'user_id': 100,
        'tags': [],
        'user_name': 'User 1',
    },
    {
        'id': 1005,
        'direction': 'outbound',
        'duration': 47,
        'answered_at': 1572622181,
        'ended_at': 1572622227,
        'raw_digits': '+1 111-111-1114',
        'user_id': 100,
        'tags': [],
        'user_name': 'User 1',
    },
    {
        'id': 1006,
        'direction': 'outbound',
        'duration': 24,
        'answered_at': 1572631701,
        'ended_at': 1572631723,
        'raw_digits': '+1 111-111-1115',
        'user_id': 200,
        'tags': [],
        'user_name': 'User 2',
    },
    {
        'id': 1007,
        'direction': 'outbound',
        'duration': 25,
        'answered_at': 1572631741,
        'ended_at': 1572631764,
        'raw_digits': '+1 111-111-1116',
        'user_id': 200,
        'tags': [],
        'user_name': 'User 2',
    },
    {
        'id': 1008,
        'direction': 'outbound',
        'duration': 15,
        'answered_at': 1572631774,
        'ended_at': 1572631788,
        'raw_digits': '+1 111-111-1117',
        'user_id': 200,
        'tags': [],
        'user_name': 'User 2',
    },
    {
        'id': 1009,
        'direction': 'outbound',
        'duration': 18,
        'answered_at': 1572632320,
        'ended_at': 1572632335,
        'raw_digits': '+1 111-111-1117',
        'user_id': 200,
        'tags': [],
        'user_name': 'User 2',
    },
]

filtered_tags = [
    {'id': 10000, 'name': 'Tag 1', 'color': '#f00', 'description': 'foo'},
    {'id': 10001, 'name': 'Tag 2', 'color': '#0f0', 'description': 'bar'},
    {'id': 10002, 'name': 'Tag 3', 'color': '#00f', 'description': 'baz'},
]
