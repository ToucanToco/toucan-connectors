from toucan_connectors.trello.trello_connector import TrelloConnector, TrelloDataSource
import numpy as np

"""
Board trello for these tests:
https://trello.com/b/fhUR3kVQ/test-connector-trello-ne-pas-changer
"""


trello_connector = TrelloConnector(
    name='trello',
    key_id='0e4ed2bf23042d92aea7219b41121ee5',
    token='a6781451b2c641c786b22c8a5cec51d444a132ff1ac7d6f51abb5f8ba15382dc'
)


def test_get_board_method():
    lists = trello_connector.get_board(fields='name', path='fhUR3kVQ/lists')

    assert len(lists) == 2
    assert set(lists[0].keys()) == {'id', 'name'}
    assert set(lists[0].values()) == {'5b2775500401ad42967638a8', 'List1'}


def test_get_df():
    df = trello_connector.get_df(TrelloDataSource(
        board_id='fhUR3kVQ',
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
    assert row_1['lists'] == 'List1'
    assert row_1['Nombre test'] == 2
    assert row_1['Menu deroulant test'] == 'A'
    assert row_1['Date test'] == '2019-04-10T10:00:00.000Z'
    assert row_1['Text test'] == 'blabla'
    assert type(row_1['members']) == list
    assert row_1['members'][0] == 'Raphaël Huille'

    # generic fields
    assert set(df.name) == {'Carte 1', 'Carte 2', 'Carte 3', 'carte 4'}
    assert set(df.lists) == {'List1', 'List2'}
    assert set(df.labels.str[0]) == {np.nan, 'Objectifs', 'Label test'}
    assert set(df.labels.str[1]) == {np.nan, 'Tooling'}
    assert set(df.members.str[0]) == {np.nan, 'Raphaël Huille'}

    # custom fields
    assert set(df['Text test']) == {np.nan, 'zorro', 'blabla'}
    assert set(df['case a cocher test']) == {True, np.nan}
    assert set(df['Menu deroulant test']) == {np.nan, 'A', 'B'}
    assert set(df['Date test']) == {np.nan, '2019-04-10T10:00:00.000Z', '2019-05-03T10:00:00.000Z'}
    assert 2 in set(df['Nombre test'])


def test_get_df_with_set_of_fields():
    df = trello_connector.get_df(TrelloDataSource(
        board_id='fhUR3kVQ',
        name='trello',
        domain='my_domain',
        custom_fields=False,
        fields_list=['name', 'members', 'lists']
    ))

    assert set(df.columns) == {'id', 'name', 'members', 'lists'}
