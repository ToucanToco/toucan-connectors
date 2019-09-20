import json

import pytest
import responses

from toucan_connectors.micro_strategy.data import fill_viewfilter_with_ids, get_definition
from toucan_connectors.micro_strategy.micro_strategy_connector import (
    MicroStrategyConnector,
    MicroStrategyDataSource,
)

md = MicroStrategyDataSource(
    name="microtest", domain="microstratdata", id="6137E0964C68D84F107816AA694C2209", dataset="cube"
)

md_filtered = MicroStrategyDataSource(
    name="microtest",
    domain="microstratdata",
    id="6137E0964C68D84F107816AA694C2209",
    dataset="cube",
    viewfilter={
        "operator": "Equals",
        "operands": [{"attribute": "Call Center@DESC"}, {"constant": "Miami"}],
    },
)

mdr = MicroStrategyDataSource(
    name="microtest",
    domain="microstratdata",
    id="TDjAuKmfGeKnqbxKr1TPfcFr4vBTlWIKDWDvODTSKsQ",
    dataset="report",
)

mds = MicroStrategyDataSource(
    name="microtest", domain="microstratdata", dataset="search", limit=5, id="revenue analysis"
)

mc = MicroStrategyConnector(
    name="microtest",
    base_url="https://demo.microstrategy.com/MicroStrategyLibrary2/api/",
    username="guest",
    password="",
    project_id="B7CA92F04B9FAE8D941C3E9B7E0CD754",
)


@responses.activate
def test_micro_strategy():
    js = json.load(open('tests/micro_strategy/fixtures/fixture.json'))

    # login
    responses.add(
        responses.POST,
        'https://demo.microstrategy.com/MicroStrategyLibrary2/api/auth/login',
        headers={'x-mstr-authtoken': 'x'},
        status=200,
    )

    # cube
    responses.add(
        responses.POST,
        'https://demo.microstrategy.com/MicroStrategyLibrary2/api/cubes/6137E0964C68D84F107816AA694C2209/instances?limit=100&offset=0',  # noqa: E501
        json=js,
        status=200,
    )

    df = mc.get_df(md)
    assert df.shape == (100, 40)

    # report
    responses.add(
        responses.POST,
        'https://demo.microstrategy.com/MicroStrategyLibrary2/api/reports/TDjAuKmfGeKnqbxKr1TPfcFr4vBTlWIKDWDvODTSKsQ/instances?limit=100&offset=0',  # noqa: E501
        json=js,
        status=200,
    )

    df = mc.get_df(mdr)
    assert df.shape == (100, 40)


@responses.activate
def test_search():
    js = json.load(open('tests/micro_strategy/fixtures/fixture_search.json'))

    # login
    responses.add(
        responses.POST,
        'https://demo.microstrategy.com/MicroStrategyLibrary2/api/auth/login',
        headers={'x-mstr-authtoken': 'x'},
        status=200,
    )

    # search
    responses.add(
        responses.GET,
        'https://demo.microstrategy.com/MicroStrategyLibrary2/api/searches/results?type=776&type=768&offset=0&limit=5&name=revenue+analysis',  # noqa: E501
        json=js,
        status=200,
    )
    df = mc.get_df(mds)
    assert df.shape == (5, 15)


def test_fill_viewfilter_with_ids():
    results = json.load(open('tests/micro_strategy/fixtures/fixture.json'))
    dfn = get_definition(results)
    viewfilter = {
        'plop': {'attribute': 'Call Center'},
        'plop_id': {'attribute': '8D679D3511D3E4981000E787EC6DE8A4'},
        'plip': {'attribute': 'Call Center@DESC'},
        'plip_id': {'attribute': '8D679D3511D3E4981000E787EC6DE8A4@DESC'},
        'ploup': {'metric': '% Change to Profit'},
        'ploup_id': {'metric': '965C42404FD62829356000B0B955F267'},
        'poulp': {'constant': 42},
    }

    res = fill_viewfilter_with_ids(viewfilter, dfn)
    assert res['plop'] == {'type': 'attribute', 'id': '8D679D3511D3E4981000E787EC6DE8A4'}
    assert res['plop'] == res['plop_id']
    assert res['plip'] == {
        'type': 'form',
        'attribute': {'id': '8D679D3511D3E4981000E787EC6DE8A4'},
        'form': {'id': 'CCFBE2A5EADB4F50941FB879CCF1721C'},
    }
    assert res['plip'] == res['plip_id']
    assert res['ploup'] == {'type': 'metric', 'id': '965C42404FD62829356000B0B955F267'}
    assert res['ploup'] == res['ploup_id']
    assert res['poulp'] == {'type': 'constant', 'dataType': 'Real', 'value': '42'}


@responses.activate
def test_viewfilter():
    js = json.load(open('tests/micro_strategy/fixtures/fixture.json'))
    expected_viewfilter = {
        "operator": "Equals",
        "operands": [
            {
                "type": "form",
                "attribute": {"id": "8D679D3511D3E4981000E787EC6DE8A4"},
                "form": {"id": "CCFBE2A5EADB4F50941FB879CCF1721C"},
            },
            {"type": "constant", "dataType": "Char", "value": "Miami"},
        ],
    }

    # login
    responses.add(
        responses.POST,
        'https://demo.microstrategy.com/MicroStrategyLibrary2/api/auth/login',
        headers={'x-mstr-authtoken': 'x'},
        status=200,
    )

    # get definition
    responses.add(
        responses.POST,
        'https://demo.microstrategy.com/MicroStrategyLibrary2/api/cubes/6137E0964C68D84F107816AA694C2209/instances?limit=0&offset=0',  # noqa: E501
        json=js,
        status=200,
    )

    # get cube data
    responses.add(
        responses.POST,
        'https://demo.microstrategy.com/MicroStrategyLibrary2/api/cubes/6137E0964C68D84F107816AA694C2209/instances?limit=100&offset=0',  # noqa: E501
        json=js,
        status=200,
    )

    df = mc.get_df(md_filtered)
    assert df.shape == (100, 40)

    viewfilter = json.loads(responses.calls[2].request.body)['viewFilter']
    assert viewfilter == expected_viewfilter


@pytest.mark.skip(reason="This uses a demo api that I found directly")
def test_on_live_instance():
    df = mc.get_df(md)
    assert df.shape == (100, 40)

    df = mc.get_df(mds)
    assert df.shape == (5, 15)
