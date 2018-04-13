import json

import pytest
import responses

from toucan_connectors.microstrategy.microstrategy_connector import (
    MicroStrategyConnector, MicroStrategyDataSource
)

md = MicroStrategyDataSource(
    name="microtest",
    domain="microstratdata",
    id="6137E0964C68D84F107816AA694C2209",
    dataset="cube"
)

mdr = MicroStrategyDataSource(
    name="microtest",
    domain="microstratdata",
    id="TDjAuKmfGeKnqbxKr1TPfcFr4vBTlWIKDWDvODTSKsQ",
    dataset="report"
)

mc = MicroStrategyConnector(
    name="microtest",
    base_url="https://demo.microstrategy.com/MicroStrategyLibrary2/api/",
    username="guest",
    password="",
    project_id="B7CA92F04B9FAE8D941C3E9B7E0CD754"
)


@responses.activate
def test_microstrategy(mocker):
    js = json.load(open('tests/microstrategy/fixtures/fixture.json'))

    # login
    responses.add(responses.POST,
                  'https://demo.microstrategy.com/MicroStrategyLibrary2/api/auth/login',
                  headers={'x-mstr-authtoken': 'x'}, status=200)

    # cube
    responses.add(responses.POST,
                  'https://demo.microstrategy.com/MicroStrategyLibrary2/api/cubes/'
                  '6137E0964C68D84F107816AA694C2209/instances?limit=100&offset=0',
                  json=js, status=200)

    df = mc.get_df(md)
    assert df.shape == (100, 40)

    # report
    responses.add(responses.POST,
                  'https://demo.microstrategy.com/MicroStrategyLibrary2/api/reports/'
                  'TDjAuKmfGeKnqbxKr1TPfcFr4vBTlWIKDWDvODTSKsQ/instances?limit=100&offset=0',
                  json=js, status=200)

    df = mc.get_df(mdr)
    assert df.shape == (100, 40)


@pytest.mark.skip(reason="This uses a demo api that I found directly")
def test_on_live_instance():
    df = mc.get_df(md)
    assert df.shape == (100, 40)
