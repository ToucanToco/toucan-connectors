from typing import Dict, Optional

import pytest

from toucan_connectors.query_manager import QueryManager


def fixture_execute_method(execute_method, query: str, query_parameters: Optional[Dict]):
    return True


def test_execute_success():
    result = QueryManager().execute(
        execute_method=fixture_execute_method,
        connection={},
        query='SELECT * FROM my_table',
        query_parameters={},
    )
    assert result is True


def test_execute_exception():
    with pytest.raises(Exception):
        QueryManager().execute(
            execute_method='tortank',
            connection={},
            query='SELECT * FROM my_table',
            query_parameters={},
        )
