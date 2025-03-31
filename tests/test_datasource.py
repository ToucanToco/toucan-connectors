from typing import Any

import pytest
from pydantic import ValidationError

from toucan_connectors.toucan_connector import ToucanDataSource


class DataSource(ToucanDataSource):
    collection: str  # required, validated against type
    query: Any  # required, not validated
    comment: str = None  # not required, no default, validated against type when present
    test_default: int = 101  # not required because it has a default, validated


def test_instantiation():
    # no errors with required args at the right type
    data_source = {
        "domain": "my_domain",
        "name": "my_name",
        "collection": "my_collection",
        "query": {},
    }
    mds = DataSource(**data_source)
    assert mds.name == data_source["name"]
    assert mds.test_default == 101


def test_required_arg():
    # error with missing required arg
    data_source = {"name": "my_name", "collection": "my_collection", "query": {}}
    with pytest.raises(ValidationError, match="Field required"):
        DataSource(**data_source)


def test_required_arg_wrong_type():
    # error with required arg of wrong type
    data_source = {"domain": [], "name": "my_name", "collection": "my_collection", "query": {}}
    with pytest.raises(ValidationError, match="Input should be a valid string"):
        DataSource(**data_source)


def test_not_required():
    data_source = {
        "domain": "my_domain",
        "name": "my_name",
        "collection": "my_collection",
        "query": {},
        "comment": "test",
    }
    mds = DataSource(**data_source)
    assert mds.comment == "test"


def test_default_override():
    data_source = {
        "domain": "my_domain",
        "name": "my_name",
        "collection": "my_collection",
        "query": {},
        "test_default": 102,
    }
    mds = DataSource(**data_source)
    assert mds.test_default == 102


def test_default_override_validated():
    data_source = {
        "domain": "my_domain",
        "name": "my_name",
        "collection": "my_collection",
        "query": {},
        "test_default": {},
    }
    with pytest.raises(ValidationError):
        DataSource(**data_source)


def test_unknown_arg():
    data_source = {
        "domain": "my_domain",
        "name": "my_name",
        "collection": "my_collection",
        "query": {},
        "unk": "@",
    }
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        DataSource(**data_source)


def test_get_form():
    default_form = ToucanDataSource.get_form(None, {})
    assert default_form == {
        "additionalProperties": False,
        "properties": {
            "domain": {"title": "Domain", "type": "string"},
            "name": {"title": "Name", "type": "string"},
            "type": {
                "anyOf": [{"type": "string"}, {"type": "null"}],
                "default": None,
                "title": "Type",
            },
            "load": {"default": True, "title": "Load", "type": "boolean"},
            "live_data": {"default": False, "title": "Live Data", "type": "boolean"},
            "validation": {
                "anyOf": [{"type": "object", "additionalProperties": True}, {"type": "null"}],
                "default": None,
                "title": "Validation",
            },
            "parameters": {
                "anyOf": [{"type": "object", "additionalProperties": True}, {"type": "null"}],
                "default": None,
                "title": "Parameters",
            },
            "cache_ttl": {
                "anyOf": [{"type": "integer"}, {"type": "null"}],
                "default": None,
                "description": "In seconds. Will override the 5min instance default and/or the connector value",
                "title": "Slow Queries' Cache Expiration Time",
            },
        },
        "required": ["domain", "name"],
        "title": "ToucanDataSource",
        "type": "object",
    }
