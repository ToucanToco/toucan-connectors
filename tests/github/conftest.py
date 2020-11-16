from typing import Any

import pytest

from toucan_connectors.oauth2_connector.oauth2connector import SecretsKeeper


@pytest.fixture
def secrets_keeper():
    class SimpleSecretsKeeper(SecretsKeeper):
        def __init__(self):
            self.store = {}

        def load(self, key: str) -> Any:
            if key not in self.store:
                return None
            return self.store[key]

        def save(self, key: str, value: Any):
            self.store[key] = value

    return SimpleSecretsKeeper()


@pytest.fixture(scope='session')
def extracted_pr_no_pr():
    return {
        'data': {
            'organization': {
                'repositories': {
                    'nodes': [
                        {
                            'name': 'empty_repo',
                            'pullRequests': {
                                'nodes': [],
                                'pageInfo': {'hasNextPage': False, 'endCursor': None},
                            },
                        }
                    ],
                    'pageInfo': {'hasNextPage': False, 'endCursor': None},
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_pr_list():
    return {
        'data': {
            'organization': {
                'repositories': {
                    'nodes': [
                        {
                            'name': 'tucblabla',
                            'pullRequests': {
                                'nodes': [
                                    {
                                        'createdAt': '2020-11-09T12:48:16Z',
                                        'mergedAt': '2020-11-12T12:48:16Z',
                                        'deletions': 3,
                                        'additions': 3,
                                        'title': 'fix(charts): blablabla',
                                        'labels': {
                                            'edges': [
                                                {'node': {'name': 'foo'}},
                                                {'node': {'name': 'fix'}},
                                                {'node': {'name': 'barr'}},
                                            ]
                                        },
                                        'commits': {
                                            'edges': [
                                                {
                                                    'node': {
                                                        'commit': {
                                                            'author': {'user': {'login': 'okidoki'}}
                                                        }
                                                    }
                                                }
                                            ]
                                        },
                                    },
                                    {
                                        'createdAt': '2020-11-09T11:13:23Z',
                                        'mergedAt': None,
                                        'deletions': 4,
                                        'additions': 10,
                                        'title': 'fix(something): Fix something',
                                        'labels': {
                                            'edges': [
                                                {'node': {'name': 'foo'}},
                                                {'node': {'name': 'fix'}},
                                                {'node': {'name': 'barz'}},
                                            ]
                                        },
                                        'commits': {
                                            'edges': [
                                                {
                                                    'node': {
                                                        'commit': {
                                                            'author': {
                                                                'user': {'login': 'jeand' 'upont'}
                                                            }
                                                        }
                                                    }
                                                }
                                            ]
                                        },
                                    },
                                ],
                                'pageInfo': {
                                    'hasNextPage': False,
                                    'endCursor': 'Y3Vyc29y'
                                    'OnYyOpK5M'
                                    'jAyMC0xMS0'
                                    'wOVQxMjoxM'
                                    'zoyMyswMTowMM4e2z5W',
                                },
                            },
                        }
                    ],
                    'pageInfo': {
                        'hasNextPage': False,
                        'endCursor': 'Y3Vyc29yOnYyOpK5'
                        'MjAyMC0xMS0wOVQxN'
                        'DowODo0MSswMTowMM4BZVra',
                    },
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_pr_first_repo():
    return {
        'data': {
            'organization': {
                'repositories': {
                    'nodes': [
                        {
                            'name': 'lablabla',
                            'pullRequests': {
                                'nodes': [
                                    {
                                        'createdAt': '2020-11-09T12:48:16Z',
                                        'mergedAt': '2020-11-12T12:48:16Z',
                                        'deletions': 3,
                                        'additions': 3,
                                        'title': 'fix(charts): blablabla',
                                        'labels': {
                                            'edges': [
                                                {'node': {'name': 'foo'}},
                                                {'node': {'name': 'fix'}},
                                                {'node': {'name': 'barr'}},
                                            ]
                                        },
                                        'commits': {
                                            'edges': [
                                                {
                                                    'node': {
                                                        'commit': {
                                                            'author': {'user': {'login': 'okidoki'}}
                                                        }
                                                    }
                                                }
                                            ]
                                        },
                                    },
                                    {
                                        'createdAt': '2020-11-09T11:13:23Z',
                                        'mergedAt': None,
                                        'deletions': 4,
                                        'additions': 10,
                                        'title': 'fix(something):' ' Fix something',
                                        'labels': {
                                            'edges': [
                                                {'node': {'name': 'foo'}},
                                                {'node': {'name': 'fix'}},
                                                {'node': {'name': 'barz'}},
                                            ]
                                        },
                                        'commits': {
                                            'edges': [
                                                {
                                                    'node': {
                                                        'commit': {
                                                            'author': {
                                                                'user': {'login': 'jeand' 'upont'}
                                                            }
                                                        }
                                                    }
                                                }
                                            ]
                                        },
                                    },
                                ],
                                'pageInfo': {
                                    'hasNextPage': False,
                                    'endCursor': 'Y3Vyc2'
                                    '9yOnYyOpK'
                                    '5MjAyMC0xM'
                                    'S0wOVQxMjox'
                                    'MzoyMyswMTo'
                                    'wMM4e2z5W',
                                },
                            },
                        }
                    ],
                    'pageInfo': {
                        'hasNextPage': True,
                        'endCursor': 'Y3Vyc29y'
                        'OnYyOpK5M'
                        'jAyMC0xM'
                        'S0wOVQxND'
                        'owODo0MSsw'
                        'MTowMM4BZVra',
                    },
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_pr_first_prs():
    return {
        'data': {
            'organization': {
                'repositories': {
                    'nodes': [
                        {
                            'name': 'lablabla',
                            'pullRequests': {
                                'nodes': [
                                    {
                                        'createdAt': '2020-11-09T12:48:16Z',
                                        'mergedAt': '2020-11-12T12:48:16Z',
                                        'deletions': 3,
                                        'additions': 3,
                                        'title': 'first pr',
                                        'labels': {
                                            'edges': [
                                                {'node': {'name': 'foo'}},
                                                {'node': {'name': 'fix'}},
                                                {'node': {'name': 'barr'}},
                                            ]
                                        },
                                        'commits': {
                                            'edges': [
                                                {
                                                    'node': {
                                                        'commit': {
                                                            'author': {'user': {'login': 'okidoki'}}
                                                        }
                                                    }
                                                }
                                            ]
                                        },
                                    },
                                    {
                                        'createdAt': '2020-11-09T11:13:23Z',
                                        'mergedAt': None,
                                        'deletions': 4,
                                        'additions': 10,
                                        'title': 'second pr',
                                        'labels': {
                                            'edges': [
                                                {'node': {'name': 'foo'}},
                                                {'node': {'name': 'fix'}},
                                                {'node': {'name': 'barz'}},
                                            ]
                                        },
                                        'commits': {
                                            'edges': [
                                                {
                                                    'node': {
                                                        'commit': {
                                                            'author': {
                                                                'user': {'login': 'jean' 'dupont'}
                                                            }
                                                        }
                                                    }
                                                }
                                            ]
                                        },
                                    },
                                ],
                                'pageInfo': {
                                    'hasNextPage': True,
                                    'endCursor': 'Y3Vyc29yO'
                                    'nYyOpK5MjA'
                                    'yMC0xMS0wO'
                                    'VQxMjoxMzo'
                                    'yMyswMTowMM4e2z5W',
                                },
                            },
                        }
                    ],
                    'pageInfo': {
                        'hasNextPage': False,
                        'endCursor': 'Y3Vyc29yOn'
                        'YyOpK5MjAy'
                        'MC0xMS0wOV'
                        'QxNDowODo0'
                        'MSswMTowMM'
                        '4BZVra',
                    },
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_pr_no_login():
    return {
        'createdAt': '2020-11-09T12:48:16Z',
        'mergedAt': '2020-11-12T12:48:16Z',
        'deletions': 3,
        'additions': 3,
        'title': 'fix(charts): blablabla',
        'labels': {'edges': [{'node': {'name': 'foo'}}]},
        'commits': {'edges': [{'node': {'commit': {'author': {'user': None}}}}]},
    }


@pytest.fixture(scope='session')
def extracted_pr_no_commits():
    return {
        'createdAt': '2020-11-09T12:48:16Z',
        'mergedAt': '2020-11-12T12:48:16Z',
        'deletions': 3,
        'additions': 3,
        'title': 'fix(charts): blablabla',
        'labels': {'edges': [{'node': {'name': 'foo'}}]},
    }


@pytest.fixture(scope='session')
def extracted_pr():
    return {
        'createdAt': '2020-11-09T12:48:16Z',
        'mergedAt': '2020-11-12T12:48:16Z',
        'deletions': 3,
        'additions': 3,
        'title': 'fix(charts): blablabla',
        'labels': {
            'edges': [
                {'node': {'name': 'foo'}},
                {'node': {'name': 'fix'}},
                {'node': {'name': 'need review'}},
            ]
        },
        'commits': {'edges': [{'node': {'commit': {'author': {'user': {'login': 'okidoki'}}}}}]},
    }


@pytest.fixture(scope='session')
def extracted_team():
    return {
        'name': 'faketeam',
        'members': {
            'edges': [
                {'node': {'login': 'foobar'}},
                {'node': {'login': 'foobuzz'}},
                {'node': {'login': 'barfoo'}},
            ],
            'pageInfo': {
                'hasNextPage': False,
                'endCursor': 'Y3Vyc29yOnYyOpKvbGVvY2FyZWwtdG91Y2FuzgQ_Ovs=',
            },
        },
    }


@pytest.fixture(scope='session')
def extracted_teams():
    return {
        'data': {
            'organization': {
                'teams': {
                    'nodes': [
                        {
                            'name': 'faketeam',
                            'members': {
                                'edges': [
                                    {'node': {'login': 'foobar'}},
                                    {'node': {'login': 'foobuzz'}},
                                    {'node': {'login': 'barfoo'}},
                                ],
                                'pageInfo': {
                                    'hasNextPage': True,
                                    'endCursor': 'Y3Vyc29yO'
                                    'nYyOpKvbG'
                                    'VvY2FyZWwt'
                                    'dG91Y2Fuzg'
                                    'Q_Ovs=',
                                },
                            },
                        }
                    ],
                    'pageInfo': {
                        'endCursor': 'Y3Vyc29yOn' 'YyOpKkQ2FyZ' 'c4APmsu',
                        'hasNextPage': True,
                    },
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_teams_one_page():
    return {
        'data': {
            'organization': {
                'teams': {
                    'nodes': [
                        {
                            'name': 'faketeam',
                            'members': {
                                'edges': [
                                    {'node': {'login': 'foobar'}},
                                    {'node': {'login': 'foobuzz'}},
                                    {'node': {'login': 'barfoo'}},
                                ],
                                'pageInfo': {
                                    'hasNextPage': False,
                                    'endCursor': 'Y3Vyc29yOnY'
                                    'yOpKvbGVvY2'
                                    'FyZWwtdG91Y2Fuzg'
                                    'Q_Ovs=',
                                },
                            },
                        }
                    ],
                    'pageInfo': {
                        'endCursor': 'Y3Vyc29yOnYyOpKkQ2FyZc4APmsu',
                        'hasNextPage': False,
                    },
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_teams_first_member_page():
    return {
        'data': {
            'organization': {
                'teams': {
                    'nodes': [
                        {
                            'name': 'faketeam',
                            'members': {
                                'edges': [
                                    {'node': {'login': 'foobar'}},
                                    {'node': {'login': 'foobuzz'}},
                                    {'node': {'login': 'barfoo'}},
                                ],
                                'pageInfo': {
                                    'hasNextPage': True,
                                    'endCursor': 'Y3Vyc29yO'
                                    'nYyOpKvbGV'
                                    'vY2FyZWwtd'
                                    'G91Y2FuzgQ_Ovs=',
                                },
                            },
                        }
                    ],
                    'pageInfo': {
                        'endCursor': 'Y3Vyc29yO' 'nYyOpKkQ2' 'FyZc4APmsu',
                        'hasNextPage': False,
                    },
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_teams_second_members_page():
    return {
        'data': {
            'organization': {
                'teams': {
                    'nodes': [
                        {
                            'name': 'faketeam',
                            'members': {
                                'edges': [
                                    {'node': {'login': 'foobar2'}},
                                    {'node': {'login': 'foobuzz2'}},
                                    {'node': {'login': 'barfoo2'}},
                                ],
                                'pageInfo': {
                                    'hasNextPage': False,
                                    'endCursor': 'Y3Vyc29yOnYy'
                                    'OpKvbGVvY2FyZ'
                                    'WwtdG91Y2FuzgQ'
                                    '_Ovs=',
                                },
                            },
                        }
                    ],
                    'pageInfo': {
                        'endCursor': 'Y3Vyc29yOnYyOpKkQ2FyZc4APmsu',
                        'hasNextPage': False,
                    },
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_teams_first_team_page():
    return {
        'data': {
            'organization': {
                'teams': {
                    'nodes': [
                        {
                            'name': 'faketeam',
                            'members': {
                                'edges': [
                                    {'node': {'login': 'foobar'}},
                                    {'node': {'login': 'foobuzz'}},
                                    {'node': {'login': 'barfoo'}},
                                ],
                                'pageInfo': {
                                    'hasNextPage': False,
                                    'endCursor': 'Y3Vyc29yOnY'
                                    'yOpKvbGVvY2F'
                                    'yZWwtdG91Y2Fuz'
                                    'gQ_Ovs=',
                                },
                            },
                        }
                    ],
                    'pageInfo': {
                        'endCursor': 'Y3Vyc29yOnYyOpKkQ2FyZc4APmsu',
                        'hasNextPage': True,
                    },
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_teams_second_team_page():
    return {
        'data': {
            'organization': {
                'teams': {
                    'nodes': [
                        {
                            'name': 'faketeam',
                            'members': {
                                'edges': [
                                    {'node': {'login': 'okidoki'}},
                                    {'node': {'login': 'foobuzza'}},
                                    {'node': {'login': 'jeandupont'}},
                                ],
                                'pageInfo': {
                                    'hasNextPage': False,
                                    'endCursor': 'Y3Vyc29yOn'
                                    'YyOpKvbGVv'
                                    'Y2FyZWwtdG9'
                                    '1Y2FuzgQ_Ovs=',
                                },
                            },
                        }
                    ],
                    'pageInfo': {
                        'endCursor': 'Y3Vyc29yOnYyOpKkQ2FyZc4APmsu',
                        'hasNextPage': False,
                    },
                }
            }
        }
    }


@pytest.fixture(scope='session')
def team_rows():
    return [
        {'foobar': 'faketeam', 'foobuzz': 'faketeam', 'barfoo': 'faketeam'},
        {'foobara': 'faketeam1', 'foobuzza': 'faketeam', 'barfoo': 'faketeam2'},
    ]


@pytest.fixture(scope='session')
def error_response():
    return {
        'errors': [
            {
                'path': ['query dataset', 'organization', 'login'],
                'extensions': {
                    'code': 'argumentLiteralsIncompatible',
                    'typeName': 'Field',
                    'argumentName': 'login',
                },
                'locations': '[{"line": 3,"column": 7}]',
                'message': 'Argument ""foobar" on Field "blabla"'
                ' has an invalid value (hahahaha).'
                ' Expected type "String!".',
            }
        ]
    }
