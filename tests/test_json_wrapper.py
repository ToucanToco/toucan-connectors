import os
from json import JSONDecodeError

import pytest
from pydantic import SecretStr

from toucan_connectors.json_wrapper import JsonWrapper, custom_json_serializer

json_not_string = None
json_string = '{"key1":"value1","key2":"value2"}'
json_not_valid_string = '{"key1":"value1","key2"}'
json_json = {"key1": "value1", "key2": "value2"}

path_dumps = "tests/fixtures/json_output.json"
path_loads = "tests/fixtures/json.json"
path_not_found = "tests/fixtures/not_found.json"


def test_json_dumps():
    result = JsonWrapper.dumps(json_json)
    assert json_string == result


def test_json_dump():
    with open(path_dumps, "w+") as f:
        JsonWrapper.dump(json_json, f)
    with open(path_dumps) as f:
        result = JsonWrapper.load(f)
        assert json_json == result
    if os.path.exists(path_dumps):
        os.remove(path_dumps)


def test_json_loads():
    result = JsonWrapper.loads(json_string)
    assert json_json == result


def test_json_loads_not_valid():
    with pytest.raises(JSONDecodeError):
        JsonWrapper.loads(json_not_valid_string)


def test_json_loads_not_string():
    with pytest.raises(TypeError):
        JsonWrapper.loads(json_not_string)


def test_json_load():
    result = JsonWrapper.load(open(path_loads))
    assert {"key1": "value1", "key2": "value2"} == result


def test_json_load_file_not_found():
    with pytest.raises(FileNotFoundError):
        JsonWrapper.load(open(path_not_found))


def test_custom_json_serializer():
    assert custom_json_serializer(SecretStr("foobar")) == "foobar"
    assert custom_json_serializer(42) == 42
