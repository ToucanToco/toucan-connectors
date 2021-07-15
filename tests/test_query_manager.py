from typing import Dict, Optional

import pytest
from mock import patch

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


@patch('psycopg2.connect')
def test_super_awesome_stuff(mock_connect):
    expected = ['fake', 'row', 1]
    mock_con = mock_connect.return_value
    mock_cur = mock_con.cursor.return_value
    mock_cur.fetchone.return_value = expected
    mock_cur.arraysize = 1

    result = QueryManager.fetchmany(mock_cur)
    assert len(result) == 1
