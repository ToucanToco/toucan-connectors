import concurrent
import json
import logging
from timeit import default_timer as timer
from typing import Dict, List, Optional

import pandas as pd
from pydantic import Field, constr
from snowflake.connector import DictCursor, SnowflakeConnection

from toucan_connectors.query_manager import QueryManager
from toucan_connectors.sql_query_helper import SqlQueryHelper
from toucan_connectors.toucan_connector import DataSlice, DataStats, QueryMetadata, ToucanDataSource

type_code_mapping = {
    0: 'float',
    1: 'real',
    2: 'text',
    3: 'date',
    4: 'timestamp',
    5: 'variant',
    6: 'timestamp_ltz',
    7: 'timestamp_tz',
    8: 'timestamp_ntz',
    9: 'object',
    10: 'array',
    11: 'binary',
    12: 'time',
    13: 'boolean',
}


class SnowflakeConnectorException(Exception):
    """Raised when something wrong happened in a snowflake context"""


class SnowflakeConnectorWarehouseDoesNotExists(Exception):
    """Raised when the specified default warehouse does not exists"""


class SfDataSource(ToucanDataSource):
    database: str = Field(..., description='The name of the database you want to query')
    warehouse: str = Field(None, description='The name of the warehouse you want to query')

    query: constr(min_length=1) = Field(
        ..., description='You can write your SQL query here', widget='sql'
    )

    query_object: Dict = Field(
        None,
        description='An object describing a simple select query'
        'For example '
        '{"schema": "SHOW_SCHEMA", "table": "MY_TABLE", "columns": ["col1", "col2"]}'
        'This field is used internally',
        **{'ui.hidden': True},
    )


class SnowflakeCommon:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.data: pd.DataFrame
        self.total_rows_count: Optional[int] = -1
        self.total_returned_rows_count: Optional[int] = -1
        self.execution_time: Optional[float] = None
        self.conversion_time: Optional[float] = None
        self.column_names_and_types: Optional[Dict[str, str]] = None

    def set_data(self, data):
        self.data = data.result()

    def set_total_rows_count(self, count):
        self.total_rows_count = count.result()['TOTAL_ROWS'][0]

    def set_total_returned_rows_count(self, count):
        self.total_returned_rows_count = count

    def set_execution_time(self, execution_time):
        self.execution_time = execution_time

    def set_conversion_time(self, conversion_time):
        self.conversion_time = conversion_time

    def _execute_query(self, connection, query: str, query_parameters: Optional[Dict] = None):
        return QueryManager().execute(
            execute_method=self._execute_query_internal,
            connection=connection,
            query=query,
            query_parameters=query_parameters,
        )

    def _execute_query_internal(
        self,
        connection: SnowflakeConnection,
        query: str,
        query_parameters: Optional[dict] = None,
    ) -> pd.DataFrame:
        execution_start = timer()
        cursor = connection.cursor(DictCursor)
        query_res = cursor.execute(query, query_parameters)
        execution_end = timer()
        execution_time = execution_end - execution_start
        self.logger.info(
            f'[benchmark][snowflake] - execute {execution_time} seconds',
            extra={
                'benchmark': {
                    'operation': 'execute',
                    'execution_time': execution_time,
                    'connector': 'snowflake',
                    'query': query,
                }
            },
        )
        self.set_execution_time(execution_time)
        convert_start = timer()
        # Here call our customized fetch
        values = pd.DataFrame.from_dict(query_res.fetchall())
        convert_end = timer()
        conversion_time = convert_end - convert_start
        self.logger.info(
            f'[benchmark][snowflake] - dataframe {conversion_time} seconds',
            extra={
                'benchmark': {
                    'operation': 'dataframe',
                    'execution_time': conversion_time,
                    'connector': 'snowflake',
                }
            },
        )
        self.set_conversion_time(conversion_time)

        return values  # do not return metadata from now

    def render_datasource(self, datasource: SfDataSource) -> dict:
        prepared_query, prepared_query_parameters = SqlQueryHelper.prepare_query(
            datasource.query, datasource.parameters
        )
        return {
            'warehouse': datasource.warehouse,
            'database': datasource.database,
            'query': prepared_query,
            'parameters': prepared_query_parameters,
        }

    def _execute_parallelized_queries(
        self,
        connection: SnowflakeConnection,
        query: str,
        query_parameters: Optional[Dict] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        get_row_count=False,
    ) -> DataSlice:
        """Call parallelized execute query to extract data & row count from query"""

        is_count_request_needed = SqlQueryHelper.count_request_needed(query, get_row_count)
        self.logger.info(f'Execute count request: {is_count_request_needed}')
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=2 if is_count_request_needed else 1
        ) as executor:
            prepared_query, prepared_query_parameters = SqlQueryHelper.prepare_limit_query(
                query, query_parameters, offset, limit
            )
            future_1 = executor.submit(
                self._execute_query,
                connection,
                prepared_query,
                prepared_query_parameters,
            )
            future_1.add_done_callback(self.set_data)
            futures = [future_1]

            if is_count_request_needed:
                (
                    prepared_query_count,
                    prepared_query_parameters_count,
                ) = SqlQueryHelper.prepare_count_query(query, query_parameters)
                future_2 = executor.submit(
                    self._execute_query,
                    connection,
                    prepared_query_count,
                    prepared_query_parameters_count,
                )
                future_2.add_done_callback(self.set_total_rows_count)
                futures.append(future_2)
            for future in concurrent.futures.as_completed(futures):
                if future.exception() is not None:
                    raise future.exception()
                else:
                    self.logger.info('query finish')
        return DataSlice(self.data)

    def fetch_data(
        self,
        connection: SnowflakeConnection,
        data_source: SfDataSource,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        get_row_count: bool = False,
    ) -> pd.DataFrame:
        if data_source.database != connection.database:
            self.logger.info(f'Connection changed to use database {connection.database}')
            self._execute_query(connection, f'USE DATABASE {data_source.database}')
        if data_source.warehouse and data_source.warehouse != connection.warehouse:
            self.logger.info(f'Connection changed to use  warehouse {connection.warehouse}')
            self._execute_query(connection, f'USE WAREHOUSE {data_source.warehouse}')

        ds = self._execute_parallelized_queries(
            connection, data_source.query, data_source.parameters, offset, limit, get_row_count
        )
        return ds.df

    def retrieve_data(
        self, connection: SnowflakeConnection, data_source: SfDataSource, get_row_count: bool = None
    ) -> pd.DataFrame:
        return self.fetch_data(connection, data_source, get_row_count=get_row_count)

    def get_slice(
        self,
        connection: SnowflakeConnection,
        data_source: SfDataSource,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        get_row_count: bool = False,
    ) -> DataSlice:
        result = self.fetch_data(connection, data_source, offset, limit, get_row_count)

        stats = DataStats(
            execution_time=self.execution_time,
            conversion_time=self.conversion_time,
            total_returned_rows=len(result),
            df_memory_size=result.memory_usage().sum(),
            total_rows=self.total_rows_count,
        )
        return DataSlice(
            df=result,
            query_metadata=QueryMetadata(columns=self.column_names_and_types),
            # In the case of user defined limit/offset, get the info
            # Not used for now
            # input_parameters={
            #     'limit': SqlQueryHelper.extract_limit(data_source.query),
            #     'offset': SqlQueryHelper.extract_offset(data_source.query),
            # },
            stats=stats,
        )

    def get_warehouses(self, connection, warehouse_name: Optional[str] = None) -> List[str]:
        query = 'SHOW WAREHOUSES'
        if warehouse_name:
            query = f"{query} LIKE '{warehouse_name}'"
        res = self._execute_query(connection, query).to_dict().get('name')
        return [warehouse for warehouse in res.values()] if res else []

    def get_databases(self, connection, database_name: Optional[str] = None) -> List[str]:
        query = 'SHOW DATABASES'
        if database_name:
            query = f"{query} LIKE '{database_name}'"
        res = self._execute_query(connection, query).to_dict().get('name')
        return [database for database in res.values()] if res else []

    def describe(self, connection, query):
        return QueryManager().describe(
            describe_method=self._describe,
            connection=connection,
            query=query,
        )

    def _describe(
        self,
        connection: SnowflakeConnection,
        query: str,
    ) -> Dict[str, str]:
        description_start = timer()
        cursor = connection.cursor(DictCursor)
        describe_res = cursor.describe(query)
        description_end = timer()
        description_time = description_end - description_start
        self.logger.info(
            f'[benchmark][snowflake] - description {description_time} seconds',
            extra={
                'benchmark': {
                    'operation': 'describe',
                    'execution_time': description_time,
                    'connector': 'snowflake',
                    'query': query,
                    'result': json.dumps(describe_res),
                }
            },
        )
        res = {r.name: type_code_mapping.get(r.type_code) for r in describe_res}
        return res
