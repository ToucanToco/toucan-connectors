from typing import Any

import pytest

from toucan_connectors.bases import ToucanDataSource, BadParameters


class DataSource(ToucanDataSource):
    collection: str          # required, validated against type
    query: Any               # required, not validated
    comment: str = None      # not required, no default, validated against type when present
    test_default: int = 101  # not required because it has a default, validated


def test_instantiation():
    # no errors with required args at the right type
    data_source = {
            'domain': 'my_domain', 'name': 'my_name',
            'collection': 'my_collection', 'query': {}}
    mds = DataSource(**data_source)
    assert mds.name == data_source['name']
    assert mds.test_default == 101


def test_required_arg():
    # error with missing required arg
    data_source = {'name': 'my_name', 'collection': 'my_collection', 'query': {}}
    with pytest.raises(BadParameters) as e:
        DataSource(**data_source)
    assert e.value.args[0][0][0] == 'domain'
    assert e.value.args[0][0][2] == 'argument is required'


def test_required_arg_wrong_type():
    # error with required arg of wrong type
    data_source = {
            'domain': 1, 'name': 'my_name',
            'collection': 'my_collection', 'query': {}}
    with pytest.raises(BadParameters) as e:
        DataSource(**data_source)
    assert e.value.args[0][0][0] == 'domain'
    assert e.value.args[0][0][3] == 'wrong type of argument'


def test_not_required():
    data_source = {
            'domain': 'my_domain', 'name': 'my_name',
            'collection': 'my_collection', 'query': {}, 'comment': 'test'}
    mds = DataSource(**data_source)
    assert mds.comment == 'test'


def test_default_override():
    data_source = {
            'domain': 'my_domain', 'name': 'my_name',
            'collection': 'my_collection', 'query': {}, 'test_default': 102}
    mds = DataSource(**data_source)
    assert mds.test_default == 102


def test_default_override_validated():
    data_source = {
            'domain': 'my_domain', 'name': 'my_name',
            'collection': 'my_collection', 'query': {}, 'test_default': '1'}
    with pytest.raises(BadParameters) as e:
        DataSource(**data_source)
    assert e.value.args[0][0][0] == 'test_default'
    assert e.value.args[0][0][3] == 'wrong type of argument'


def test_unknown_arg():
    data_source = {
            'domain': 'my_domain', 'name': 'my_name',
            'collection': 'my_collection', 'query': {}, 'unk': '@'}
    with pytest.raises(BadParameters) as e:
        DataSource(**data_source)
    assert e.value.args[0][0][0] == 'unk'
    assert e.value.args[0][0][1] == 'unknown argument'
