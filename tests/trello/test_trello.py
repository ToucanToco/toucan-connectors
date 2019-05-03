from toucan_connectors.trello.trello_connector import TrelloConnector, TrelloDataSource
import numpy as np
import responses
import json
import requests
from urllib.parse import urlencode

mock_trello_api_json_responses = {
    "cards_1": [
        {
            "id": "5cc053e710e111466de80ce1",
            "name": "Carte 1",
            "idList": "5b2775500401ad42967638a8",
            "url": "https://trello.com/c/FaFjLrWE/95-carte-1",
            "idMembers": [
                "5b2765d1be05487dbefe6c8f"
            ],
            "labels": [],
            "customFieldItems": []
        },
        {
            "id": "5cc053ea86f3a4726f5c92bb",
            "name": "Carte 2",
            "idList": "5b2775500401ad42967638a8",
            "url": "https://trello.com/c/eBKpDgu8/96-carte-2",
            "idMembers": [
                "5b2765d1be05487dbefe6c8f"
            ],
            "labels": [
                {
                    "id": "5cc0544fc8dc0f1361c4bfa9",
                    "idBoard": "5b2775500401ad42967638a5",
                    "name": "Label test",
                    "color": "orange"
                }
            ],
            "customFieldItems": [
                {
                    "id": "5cc054304a63c42ceb904698",
                    "value": {
                        "number": "2"
                    },
                    "idCustomField": "5cc0542df10ae830637b0db8",
                    "idModel": "5cc053ea86f3a4726f5c92bb",
                    "modelType": "card"
                },
                {
                    "id": "5cc05433510467658e913470",
                    "value": {
                        "text": "blabla"
                    },
                    "idCustomField": "5cc0541e11a84674b6e27eab",
                    "idModel": "5cc053ea86f3a4726f5c92bb",
                    "modelType": "card"
                },
                {
                    "id": "5cc0543c06f17d5d4a8b2c9d",
                    "idValue": "5cc0541047b41361c704eb26",
                    "idCustomField": "5cc05409d7fb73643ba432c2",
                    "idModel": "5cc053ea86f3a4726f5c92bb",
                    "modelType": "card"
                },
                {
                    "id": "5cc0543a9fc3c3854955c791",
                    "value": {
                        "date": "2019-04-10T10:00:00.000Z"
                    },
                    "idCustomField": "5cc053fc6b5a407723ef8cc9",
                    "idModel": "5cc053ea86f3a4726f5c92bb",
                    "modelType": "card"
                },
                {
                    "id": "5cc05435041a015bf9d7169e",
                    "value": {
                        "checked": "true"
                    },
                    "idCustomField": "5cc053f628f36673d18161a3",
                    "idModel": "5cc053ea86f3a4726f5c92bb",
                    "modelType": "card"
                }
            ]
        },
        {
            "id": "5cc05443c458e030eaa2af5d",
            "name": "Carte 3",
            "idList": "5b2775500401ad42967638a8",
            "url": "https://trello.com/c/RIvp0RO6/97-carte-3",
            "idMembers": [],
            "labels": [
                {
                  "id": "5cc0544fc8dc0f1361c4bfa9",
                  "idBoard": "5b2775500401ad42967638a5",
                  "name": "Label test",
                  "color": "orange"
                }
            ],
            "customFieldItems": []
        },
        {
            "id": "5ccaefd335003e72af35332c",
            "name": "carte 4",
            "idList": "5ccace365cc56d252a4b14a9",
            "url": "https://trello.com/c/QGQqqHNQ/98-carte-4",
            "idMembers": [],
            "labels": [
                {
                  "id": "5b2775500401ad42967639b4",
                  "idBoard": "5b2775500401ad42967638a5",
                  "name": "Objectifs",
                  "color": "sky"
                },
                {
                    "id": "5b2775500401ad42967639b2",
                    "idBoard": "5b2775500401ad42967638a5",
                    "name": "Tooling",
                    "color": "yellow"
                }
            ],
            "customFieldItems": [
                {
                    "id": "5ccaefe5d548517764223cfe",
                    "value": {
                        "number": "2"
                    },
                    "idCustomField": "5cc0542df10ae830637b0db8",
                    "idModel": "5ccaefd335003e72af35332c",
                    "modelType": "card"
                },
                {
                    "id": "5ccaefeb82f3178ccf7e8ee0",
                    "value": {
                        "text": "zorro"
                    },
                    "idCustomField": "5cc0541e11a84674b6e27eab",
                    "idModel": "5ccaefd335003e72af35332c",
                    "modelType": "card"
                },
                {
                    "id": "5ccaefe341e8d182a71bf64e",
                    "idValue": "5cc054114b09533d3bb6e224",
                    "idCustomField": "5cc05409d7fb73643ba432c2",
                    "idModel": "5ccaefd335003e72af35332c",
                    "modelType": "card"
                },
                {
                    "id": "5ccaefdf02dc6a3cae9b280a",
                    "value": {
                        "date": "2019-05-03T10:00:00.000Z"
                    },
                    "idCustomField": "5cc053fc6b5a407723ef8cc9",
                    "idModel": "5ccaefd335003e72af35332c",
                    "modelType": "card"
                }
            ]
        }
    ],
    "customFields": [
        {
            "id": "5cc053f628f36673d18161a3",
            "idModel": "5b2775500401ad42967638a5",
            "modelType": "board",
            "fieldGroup": "e136446d828797a1a7dab7991fe2e6323372ffb92a0bf499732286a109482534",
            "name": "case a cocher test",
            "pos": 16384,
            "type": "checkbox"
        },
        {
            "id": "5cc053fc6b5a407723ef8cc9",
            "idModel": "5b2775500401ad42967638a5",
            "modelType": "board",
            "fieldGroup": "5b7e9b62eaf0af74342efb61de6cb12a60282c2892db9f9e60f3d382bc73f8bf",
            "name": "Date test",
            "pos": 32768,
            "type": "date"
        },
        {
            "id": "5cc05409d7fb73643ba432c2",
            "idModel": "5b2775500401ad42967638a5",
            "modelType": "board",
            "fieldGroup": "80611c2023a4261623cc5871f3273aecd5d4b0b4ecf143fd73ba26daeb0df70d",
            "name": "Menu deroulant test",
            "pos": 49152,
            "type": "list",
            "options": [
                {
                  "id": "5cc0541047b41361c704eb26",
                  "idCustomField": "5cc05409d7fb73643ba432c2",
                  "value": {
                      "text": "A"
                  },
                    "color": "none",
                    "pos": 16384
                },
                {
                    "id": "5cc054114b09533d3bb6e224",
                    "idCustomField": "5cc05409d7fb73643ba432c2",
                    "value": {
                        "text": "B"
                    },
                    "color": "none",
                    "pos": 32768
                },
                {
                    "id": "5cc05413452bf84dd1eaed85",
                    "idCustomField": "5cc05409d7fb73643ba432c2",
                    "value": {
                        "text": "C"
                    },
                    "color": "none",
                    "pos": 49152
                }
            ]
        },
        {
            "id": "5cc0541e11a84674b6e27eab",
            "idModel": "5b2775500401ad42967638a5",
            "modelType": "board",
            "fieldGroup": "76b50cb1d4f0de2d1c9c20fd0522149776fb831d7a70828203ec3b9ce39dc99e",
            "name": "Text test",
            "pos": 65536,
            "type": "text"
        },
        {
            "id": "5cc0542df10ae830637b0db8",
            "idModel": "5b2775500401ad42967638a5",
            "modelType": "board",
            "fieldGroup": "02d2d839cb71f3bd3f73d6e844a37da5245d550bd811abf52befb18d563b7f63",
            "name": "Nombre test",
            "pos": 81920,
            "type": "number"
        }
    ],
    "lists": [
        {
            "id": "5b2775500401ad42967638a8",
            "name": "zorro"
        },
        {
            "id": "5ccace365cc56d252a4b14a9",
            "name": "bernardo"
        }
    ],
    "members": [
        {
            "id": "5b2765d1be05487dbefe6c8f",
            "fullName": "Jean-Jacques Goldman"
        }
    ],
    "labels": [
        {
            "id": "5b2775500401ad42967639b4",
            "name": "toto"
        },
        {
            "id": "5b2775500401ad42967639b2",
            "name": "tata"
        },
        {
            "id": "5cc0544fc8dc0f1361c4bfa9",
            "name": 'titi'
        }
    ]
}

trello_connector = TrelloConnector(
    name='trello',
)

baseroute = "https://api.trello.com/1/boards/dsjhdejbdkeb"
default = {'key': '', 'token': ''}


@responses.activate
def test_get_board_method():
    responses.add(
        responses.GET,
        'https://api.trello.com/1/boards/dsjhdejbdkeb/lists?key=&token=&fields=name',
        json=mock_trello_api_json_responses['lists'],
        status=200
    )

    lists = trello_connector.get_board(fields='name', path='dsjhdejbdkeb/lists')

    assert len(lists) == 2
    assert set(lists[0].keys()) == {'id', 'name'}
    assert set(lists[0].values()) == {'5b2775500401ad42967638a8', 'zorro'}


@responses.activate
def test_get_df():
    responses.add(
        responses.GET,
        f"{baseroute}/lists?" + urlencode({**default, "fields": 'name'}),
        json=mock_trello_api_json_responses['lists'],
        status=200
    )
    responses.add(
        responses.GET,
        f"{baseroute}/members?" + urlencode({**default, "fields": 'fullName'}),
        json=mock_trello_api_json_responses['members'],
        status=200
    )
    responses.add(
        responses.GET,
        f"{baseroute}/labels?" + urlencode({**default, "fields": 'name'}),
        json=mock_trello_api_json_responses['labels'],
        status=200
    )
    responses.add(
        responses.GET,
        f"{baseroute}/customFields?" + urlencode({**default, "fields": 'name'}),
        json=mock_trello_api_json_responses['customFields'],
        status=200
    )
    # import pdb
    # pdb.set_trace()
    responses.add(
        responses.GET,
        "https://api.trello.com/1/boards/dsjhdejbdkeb/cards?key=&token=&fields=name&fields=url&fields=idList&fields=labels&fields=idMembers&customFieldItems=true",
        json=mock_trello_api_json_responses['cards_1'],
        status=200
    )

    df = trello_connector.get_df(TrelloDataSource(
        board_id='dsjhdejbdkeb',
        name='trello',
        domain='my_domain',
    ))

    expected_columns = ['Date test', 'Menu deroulant test', 'Nombre test', 'Text test',
                        'case a cocher test', 'id', 'labels', 'lists', 'members', 'name', 'url']

    # test global structure
    assert set(df.columns) == set(expected_columns)
    assert len(df) == 4

    # test row 1
    row_1 = dict(df.loc[1])
    assert row_1['name'] == 'Carte 2'
    assert row_1['lists'] == 'zorro'
    assert row_1['Nombre test'] == 2
    assert row_1['Menu deroulant test'] == 'A'
    assert row_1['Date test'] == '2019-04-10T10:00:00.000Z'
    assert row_1['Text test'] == 'blabla'
    assert type(row_1['members']) == list
    assert row_1['members'][0] == 'Jean-Jacques Goldman'

    # generic fields
    assert set(df.name) == {'Carte 1', 'Carte 2', 'Carte 3', 'carte 4'}
    assert set(df.lists) == {'zorro', 'bernardo'}
    assert set(df.labels.str[0]) == {np.nan, 'toto', 'titi'}
    assert set(df.labels.str[1]) == {np.nan, 'tata'}
    assert set(df.members.str[0]) == {np.nan, 'Jean-Jacques Goldman'}

    # custom fields
    assert set(df['Text test']) == {np.nan, 'zorro', 'blabla'}
    assert set(df['case a cocher test']) == {True, np.nan}
    assert set(df['Menu deroulant test']) == {np.nan, 'A', 'B'}
    assert set(df['Date test']) == {np.nan, '2019-04-10T10:00:00.000Z', '2019-05-03T10:00:00.000Z'}
    assert 2 in set(df['Nombre test'])


# def test_get_df_with_set_of_fields():
#     df = trello_connector.get_df(TrelloDataSource(
#         board_id='dsjhdejbdkeb',
#         name='trello',
#         domain='my_domain',
#         custom_fields=False,
#         fields_list=['name', 'members', 'lists']
#     ))

#     assert set(df.columns) == {'id', 'name', 'members', 'lists'}
