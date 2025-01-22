import pytest

from toucan_connectors.query_manager import QueryManager


def fixture_execute_method(execute_method, query: str, query_parameters: dict | None):
    return True


def fixture_describe_method(describe_method, query: str):
    return True


def test_execute_success():
    result = QueryManager().execute(
        execute_method=fixture_execute_method,
        connection={},
        query="SELECT * FROM my_table",
        query_parameters={},
    )
    assert result is True


def test_execute_exception():
    with pytest.raises(Exception):
        QueryManager().execute(
            execute_method="tortank",
            connection={},
            query="SELECT * FROM my_table",
            query_parameters={},
        )


def test_describe_success():
    result = QueryManager().describe(
        describe_method=fixture_describe_method,
        connection={},
        query="SELECT * FROM my_table",
    )
    assert result is True


def test_describe_failure():
    with pytest.raises(Exception):
        QueryManager().describe(
            describe_method="fugazzi",
            connection={},
            query="SELECT * FROM my_table",
        )
