from mock import Mock, patch
import pandas as pd
import snowflake.connector

from toucan_connectors.snowflake_common import SnowflakeCommon

data = {
    '1 Column Name': ['1 value', '2 value', '3 value', '4 value', '5 value', '6 value', '7 value', '8 value', '9 value',
                      '10 value', ...],
    '2 Column Name': ['1 value', '2 value', '3 value', '4 value', '5 value', '6 value', '7 value', '8 value', '9 value',
                      '10 value', ...],
    '3 Column Name': ['1 value', '2 value', '3 value', '4 value', '5 value', '6 value', '7 value', '8 value', '9 value',
                      '10 value', ...],
    '4 Column Name': ['1 value', '2 value', '3 value', '4 value', '5 value', '6 value', '7 value', '8 value', '9 value',
                      '10 value', ...],
    '5 Column Name': ['1 value', '2 value', '3 value', '4 value', '5 value', '6 value', '7 value', '8 value', '9 value',
                      '10 value', ...],
    '6 Column Name': ['1 value', '2 value', '3 value', '4 value', '5 value', '6 value', '7 value', '8 value', '9 value',
                      '10 value', ...],
    '7 Column Name': ['1 value', '2 value', '3 value', '4 value', '5 value', '6 value', '7 value', '8 value', '9 value',
                      '10 value', ...],
    '8 Column Name': ['1 value', '2 value', '3 value', '4 value', '5 value', '6 value', '7 value', '8 value', '9 value',
                      '10 value', ...],
    '9 Column Name': ['1 value', '2 value', '3 value', '4 value', '5 value', '6 value', '7 value', '8 value', '9 value',
                      '10 value', ...],
    '10 Column Name': ['1 value', '2 value', '3 value', '4 value', '5 value', '6 value', '7 value', '8 value',
                       '9 value', '10 value', ...],
}
df = pd.DataFrame(data, columns=['1 Column Name', '2 Column Name', '3 Column Name', '4 Column Name', '5 Column Name',
                                 '6 Column Name',
                                 '7 Column Name', '8 Column Name', '9 Column Name', '10 Column Name', ...])


@patch('snowflake.connector.cursor.SnowflakeCursor.execute')
@patch('snowflake.connector.connect', return_value=snowflake.connector.SnowflakeConnection)
@patch('pandas.DataFrame.from_dict', return_value=['database_1', 'database_2'])
def test_get_database_without_filter(db_result, sc, exec, mocker):
    spy = mocker.spy(pd.DataFrame, 'from_dict')
    result = SnowflakeCommon().get_databases(sc)


    print('')
    print(result)

    assert result[0] == 'database_1'
    assert result[1] == 'database_2'
    assert len(result) == 2
