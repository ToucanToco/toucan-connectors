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
def extracted_team_slugs():
    return {
        'data': {
            'organization': {
                'teams': {
                    'nodes': [
                        {'slug': 'foo'},
                        {'slug': 'bar'},
                        {'slug': 'ofo'},
                    ],
                    'pageInfo': {'endCursor': 'Y3Vyc29yOnYyOpKkVGVhbc4ADwiK', 'hasNextPage': True},
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_team_slugs_2():
    return {
        'data': {
            'organization': {
                'teams': {
                    'nodes': [
                        {'slug': 'fob'},
                        {'slug': 'bao'},
                        {'slug': 'oof'},
                    ],
                    'pageInfo': {'endCursor': 'Y3Vyc29yOnYyOpKkVGVhbc4ADwiK', 'hasNextPage': False},
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_pr_list():
    return {
        'nodes': [
            {
                'createdAt': '2020-11-18T15:58:44Z',
                'mergedAt': '2020-11-18T15:59:44Z',
                'deletions': 45,
                'additions': 162,
                'title': 'feat(something):blabla ',
                'state': 'MERGED',
                'labels': {
                    'edges': [
                        {'node': {'name': 'feature'}},
                        {'node': {'name': '✍ NEED REVIEW ✍'}},
                        {'node': {'name': 'label'}},
                    ]
                },
                'commits': {
                    'edges': [{'node': {'commit': {'author': {'user': {'login': 'user1'}}}}}]
                },
            },
            {
                'createdAt': '2020-11-18T15:21:21Z',
                'mergedAt': '2020-11-19T09:52:38Z',
                'deletions': 20,
                'additions': 17,
                'title': 'build: something',
                'state': 'MERGED',
                'labels': {
                    'edges': [
                        {'node': {'name': 'feature'}},
                        {'node': {'name': 'label2'}},
                        {'node': {'name': '✌️ TO MERGE ✌️'}},
                    ]
                },
                'commits': {
                    'edges': [{'node': {'commit': {'author': {'user': {'login': 'michel'}}}}}]
                },
            },
            {
                'createdAt': '2020-11-18T14:18:20Z',
                'mergedAt': '2020-11-18T18:22:16Z',
                'deletions': 69,
                'additions': 98,
                'title': 'chore(somethinh):bla',
                'state': 'CLOSED',
                'labels': {'edges': [{'node': {'name': 'tech'}}]},
                'commits': {
                    'edges': [{'node': {'commit': {'author': {'user': {'login': 'jeanlouis'}}}}}]
                },
            },
        ],
        'pageInfo': {
            'hasNextPage': True,
            'endCursor': 'Y3Vyc29yOnYyOpK5MjAyMC0xMS0xOFQxNToxODoyMCswMTowMM4fL6u3',
        },
    }


@pytest.fixture(scope='session')
def extracted_pr_no_commits_2():
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
def extracted_pr_no_commits():
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
        'commits': {'edges': None},
    }


@pytest.fixture(scope='session')
def extracted_team_page_1():
    return {
        'data': {
            'organization': {
                'team': {
                    'members': {
                        'edges': [
                            {'node': {'login': 'bar'}},
                            {'node': {'login': 'foo'}},
                            {'node': {'login': 'ofo'}},
                        ],
                        'pageInfo': {
                            'hasNextPage': True,
                            'endCursor': 'Y3Vyc29yOnYyOpKvbGVvY2FyZWwtdG91Y2FuzgQ_Ovs=',
                        },
                    }
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_team_page_2():
    return {
        'data': {
            'organization': {
                'team': {
                    'members': {
                        'edges': [
                            {'node': {'login': 'br'}},
                            {'node': {'login': 'foo'}},
                            {'node': {'login': 'buzz'}},
                        ],
                        'pageInfo': {'hasNextPage': False, 'endCursor': 'aaaaa='},
                    }
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


@pytest.fixture(scope='session')
def extracted_repositories_names():
    return {
        'data': {
            'organization': {
                'repositories': {
                    'nodes': [{'name': 'repo1'}, {'name': 'repo2'}],
                    'pageInfo': {
                        'hasNextPage': True,
                        'endCursor': 'Y3Vyc29yOnYyOpK5MjAyMC0xMS0xOVQxMToxMToxNyswMTowMM4BZVra',
                    },
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_repositories_names_2():
    return {
        'data': {
            'organization': {
                'repositories': {
                    'nodes': [{'name': 'repo3'}, {'name': 'repo4'}],
                    'pageInfo': {
                        'hasNextPage': False,
                        'endCursor': 'Y3Vyc29yOnYyOpK5MjAyMC0xMS0xOVQxMToxMToxNyswMTowMM4BZVra',
                    },
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_prs_1():
    return {
        'data': {
            'organization': {
                'repository': {
                    'name': 'repo3',
                    'pullRequests': {
                        'nodes': [
                            {
                                'createdAt': '2020-11-18T15:58:44Z',
                                'mergedAt': None,
                                'deletions': 45,
                                'additions': 162,
                                'title': 'feat(blalbla):blalba ',
                                'state': 'OPEN',
                                'labels': {
                                    'edges': [
                                        {'node': {'name': 'feature'}},
                                        {'node': {'name': 'Label'}},
                                        {'node': {'name': 'Other Label'}},
                                    ]
                                },
                                'commits': {
                                    'edges': [
                                        {
                                            'node': {
                                                'commit': {
                                                    'author': {'user': {'login': 'jeanlouis'}}
                                                }
                                            }
                                        }
                                    ]
                                },
                            },
                            {
                                'createdAt': '2020-11-18T15:21:21Z',
                                'mergedAt': '2020-11-19T09:52:38Z',
                                'deletions': 20,
                                'additions': 17,
                                'title': 'build: something',
                                'state': 'MERGED',
                                'labels': {
                                    'edges': [
                                        {'node': {'name': 'feature'}},
                                        {'node': {'name': 'label'}},
                                        {'node': {'name': 'Label'}},
                                    ]
                                },
                                'commits': {
                                    'edges': [
                                        {
                                            'node': {
                                                'commit': {'author': {'user': {'login': 'michel'}}}
                                            }
                                        }
                                    ]
                                },
                            },
                            {
                                'createdAt': '2020-11-18T14:18:20Z',
                                'mergedAt': '2020-11-18T18:22:16Z',
                                'deletions': 69,
                                'additions': 98,
                                'title': 'chore(something): somethin',
                                'state': 'MERGED',
                                'labels': {'edges': [{'node': {'name': 'tech'}}]},
                                'commits': {
                                    'edges': [
                                        {'node': {'commit': {'author': {'user': {'login': 'boo'}}}}}
                                    ]
                                },
                            },
                        ],
                        'pageInfo': {
                            'hasNextPage': True,
                            'endCursor': 'Y3Vyc29yOnYyOpK5MjAyMC0xMS0xOFQxNToxODoyMCswMTowMM4fL6u3',
                        },
                    },
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_prs_2():
    return {
        'data': {
            'organization': {
                'repository': {
                    'name': 'repo3',
                    'pullRequests': {
                        'nodes': [
                            {
                                'createdAt': '2020-11-18T15:58:44Z',
                                'mergedAt': None,
                                'deletions': 45,
                                'additions': 162,
                                'title': 'feat(blalbla):bla',
                                'state': 'OPEN',
                                'labels': {
                                    'edges': [
                                        {'node': {'name': 'feature'}},
                                        {'node': {'name': 'Label'}},
                                        {'node': {'name': 'Other Label'}},
                                    ]
                                },
                                'commits': {
                                    'edges': [
                                        {
                                            'node': {
                                                'commit': {
                                                    'author': {'user': {'login': 'jeanlouis'}}
                                                }
                                            }
                                        }
                                    ]
                                },
                            }
                        ],
                        'pageInfo': {'hasNextPage': False, 'endCursor': '123'},
                    },
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_prs_3():
    return {
        'data': {
            'organization': {
                'repository': {
                    'name': 'repo4',
                    'pullRequests': {
                        'nodes': [
                            {
                                'createdAt': '2020-11-18T15:58:44Z',
                                'mergedAt': None,
                                'deletions': 45,
                                'additions': 162,
                                'title': 'feat(blalbla):blalba ',
                                'state': 'OPEN',
                                'labels': {
                                    'edges': [
                                        {'node': {'name': 'feature'}},
                                        {'node': {'name': 'Label'}},
                                        {'node': {'name': 'Other Label'}},
                                    ]
                                },
                                'commits': {
                                    'edges': [
                                        {
                                            'node': {
                                                'commit': {
                                                    'author': {'user': {'login': 'jeanlouis'}}
                                                }
                                            }
                                        }
                                    ]
                                },
                            },
                        ],
                        'pageInfo': {'hasNextPage': False, 'endCursor': '123'},
                    },
                }
            }
        }
    }


@pytest.fixture(scope='session')
def extracted_prs_4():
    return {
        'data': {
            'organization': {
                'repository': {
                    'name': 'repo4',
                    'pullRequests': {
                        'nodes': [
                            {
                                'createdAt': '2020-11-18T15:58:44Z',
                                'mergedAt': None,
                                'deletions': 45,
                                'additions': 162,
                                'title': 'feat(blalbla):blalba ',
                                'state': 'OPEN',
                                'labels': {
                                    'edges': [
                                        {'node': {'name': 'feature'}},
                                        {'node': {'name': 'Label'}},
                                        {'node': {'name': 'Other Label'}},
                                    ]
                                },
                                'commits': {
                                    'edges': [
                                        {
                                            'node': {
                                                'commit': {
                                                    'author': {'user': {'login': 'jeanlouis'}}
                                                }
                                            }
                                        }
                                    ]
                                },
                            },
                        ],
                        'pageInfo': {'hasNextPage': False, 'endCursor': '123'},
                        'rateLimit': {'remaining': 0, 'resetAt': '2021-02-23T13:26:47Z'},
                    },
                }
            }
        }
    }
