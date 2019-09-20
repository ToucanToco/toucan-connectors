import json

import numpy as np
import responses

from toucan_connectors.trello.trello_connector import TrelloConnector, TrelloDataSource

with open("tests/trello/fixtures/fixture.json") as f:
    mock_trello_api_json_responses = json.load(f)

trello_connector = TrelloConnector(name="trello")

baseroute = "https://api.trello.com/1/boards/dsjhdejbdkeb"
default_param = "key=&token=&"


@responses.activate
def test_get_board_method():
    responses.add(
        responses.GET,
        f"{baseroute}/lists?fields=name",
        json=mock_trello_api_json_responses["lists"],
        status=200,
    )

    lists = trello_connector.get_board(fields="name", path="dsjhdejbdkeb/lists")

    assert len(lists) == 2
    assert set(lists[0].keys()) == {"id", "name"}
    assert set(lists[0].values()) == {"5b2775500401ad42967638a8", "zorro"}


@responses.activate
def test_get_df():
    responses.add(
        responses.GET,
        f"{baseroute}/lists?{default_param}fields=name",
        json=mock_trello_api_json_responses["lists"],
        status=200,
    )
    responses.add(
        responses.GET,
        f"{baseroute}/members?{default_param}fields=fullName",
        json=mock_trello_api_json_responses["members"],
        status=200,
    )
    responses.add(
        responses.GET,
        f"{baseroute}/labels?{default_param}fields=name",
        json=mock_trello_api_json_responses["labels"],
        status=200,
    )
    responses.add(
        responses.GET,
        f"{baseroute}/customFields?{default_param}fields=name",
        json=mock_trello_api_json_responses["customFields"],
        status=200,
    )
    responses.add(
        responses.GET,
        f"{baseroute}/cards?key=&token=&fields=name&fields=url&"
        "fields=idList&fields=labels&fields=idMembers&customFieldItems=true",
        json=mock_trello_api_json_responses["cards_1"],
        status=200,
    )

    df = trello_connector.get_df(
        TrelloDataSource(board_id="dsjhdejbdkeb", name="trello", domain="my_domain")
    )

    expected_columns = [
        "Date test",
        "Menu deroulant test",
        "Nombre test",
        "Text test",
        "case a cocher test",
        "id",
        "labels",
        "lists",
        "members",
        "name",
        "url",
    ]

    # test global structure
    assert set(df.columns) == set(expected_columns)
    assert len(df) == 4

    # test row 1
    row_1 = dict(df.loc[1])
    assert row_1["name"] == "Carte 2"
    assert row_1["lists"] == "zorro"
    assert row_1["Nombre test"] == 2
    assert row_1["Menu deroulant test"] == "A"
    assert row_1["Date test"] == "2019-04-10T10:00:00.000Z"
    assert row_1["Text test"] == "blabla"
    assert type(row_1["members"]) == list
    assert row_1["members"][0] == "Jean-Jacques Goldman"

    # generic fields
    assert set(df.name) == {"Carte 1", "Carte 2", "Carte 3", "carte 4"}
    assert set(df.lists) == {"zorro", "bernardo"}
    assert set(df.labels.str[0]) == {np.nan, "toto", "titi"}
    assert set(df.labels.str[1]) == {np.nan, "tata"}
    assert set(df.members.str[0]) == {np.nan, "Jean-Jacques Goldman"}

    # custom fields
    assert set(df["Text test"]) == {np.nan, "zorro", "blabla"}
    assert set(df["case a cocher test"]) == {True, np.nan}
    assert set(df["Menu deroulant test"]) == {np.nan, "A", "B"}
    assert set(df["Date test"]) == {np.nan, "2019-04-10T10:00:00.000Z", "2019-05-03T10:00:00.000Z"}
    assert 2 in set(df["Nombre test"])


@responses.activate
def test_get_df_with_set_of_fields():
    responses.add(
        responses.GET,
        f"{baseroute}/lists?{default_param}fields=name",
        json=mock_trello_api_json_responses["lists"],
        status=200,
    )
    responses.add(
        responses.GET,
        f"{baseroute}/members?{default_param}fields=fullName",
        json=mock_trello_api_json_responses["members"],
        status=200,
    )
    responses.add(
        responses.GET,
        f"{baseroute}/cards?{default_param}fields=name&fields=idList"
        "&fields=idMembers&customFieldItems=false",
        json=mock_trello_api_json_responses["cards_2"],
        status=200,
    )
    df = trello_connector.get_df(
        TrelloDataSource(
            board_id="dsjhdejbdkeb",
            name="trello",
            domain="my_domain",
            custom_fields=False,
            fields_list=["name", "members", "lists"],
        )
    )

    assert set(df.columns) == {"id", "name", "members", "lists"}
