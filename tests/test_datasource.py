from typing import Any

from pydantic import ValidationError
import pytest

from toucan_connectors.toucan_connector import ToucanDataSource


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
    with pytest.raises(ValidationError) as e:
        DataSource(**data_source)
    assert 'domain' in e.value.errors()[0]['loc']  # Are we testing pydantic here ?
    assert e.value.errors()[0]['msg'] == 'field required'


def test_required_arg_wrong_type():
    # error with required arg of wrong type
    data_source = {
            'domain': [], 'name': 'my_name',
            'collection': 'my_collection', 'query': {}}
    with pytest.raises(ValidationError) as e:
        DataSource(**data_source)
    assert 'domain' in e.value.errors()[0]['loc']
    assert e.value.errors()[0]['msg'] == 'str type expected'


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
            'collection': 'my_collection', 'query': {}, 'test_default': {}}
    with pytest.raises(ValidationError):
        DataSource(**data_source)


def test_unknown_arg():
    data_source = {
            'domain': 'my_domain', 'name': 'my_name',
            'collection': 'my_collection', 'query': {}, 'unk': '@'}
    with pytest.raises(ValidationError) as e:
        DataSource(**data_source)
    assert 'unk' in e.value.errors()[0]['loc']
    assert e.value.errors()[0]['msg'] == 'extra fields not permitted'


def test_get_form():
    default_form = ToucanDataSource.get_form(None, {})
    assert default_form == {
        'title': 'ToucanDataSource',
        'type': 'object',
        'properties': {
            'domain': {'title': 'Domain', 'type': 'string'},
            'name': {'title': 'Name', 'type': 'string'},
            'type': {'title': 'Type', 'type': 'string'},
            'load': {'title': 'Load', 'type': 'boolean', 'default': True},
            'live_data': {'title': 'Live_Data', 'type': 'boolean', 'default': False},
            'validation': {'title': 'Validation', 'type': 'array', 'items': {}},
            'parameters': {'title': 'Parameters', 'type': 'object'},
        },
        'required': ['domain', 'name']
    }
