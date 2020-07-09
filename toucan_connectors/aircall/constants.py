"""File containing constants for AirCall connector"""
COLUMN_DICTIONARY = {
    'calls': [
        'id',
        'direction',
        'duration',
        'answered_at',
        'ended_at',
        'user_id',
        'tags',
        'user_name',
        'team',
        'day',
    ],
    'tags': ['id', 'name', 'color', 'description'],
    'users': ['team', 'user_id', 'user_name', 'user_created_at'],
}

MAX_RUNS = 1
PER_PAGE = 50
