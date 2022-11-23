import concurrent
import json
import logging
from timeit import default_timer as timer
from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import Field, constr
from snowflake.connector import DictCursor, SnowflakeConnection

from toucan_connectors.pagination import build_pagination_info
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
    language: str = Field('sql', **{'ui.hidden': True})


def build_database_model_extraction_query() -> str:
    return """SELECT t.table_catalog AS database, t.table_schema AS schema,
    CASE WHEN t.table_type = 'BASE TABLE' THEN 'table' ELSE lower(t.table_type) END AS type,
    t.table_name AS name,
    ARRAY_AGG(object_construct('name', c.column_name, 'type', c.data_type)) AS columns
    FROM
        information_schema.tables t
    INNER JOIN information_schema.columns c ON
        t.table_name = c.table_name AND t.table_schema = c.table_schema
    WHERE t.table_type IN ('BASE TABLE', 'VIEW')
    AND t.table_schema NOT IN  ('PG_CATALOG', 'INFORMATION_SCHEMA', 'PG_INTERNAL')
    AND t.table_name NOT IN ('LOAD_HISTORY')
    GROUP BY t.table_catalog, t.table_schema, t.table_name, t.table_type;"""


class SnowflakeCommon:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.data: pd.DataFrame
        self.total_rows_count: Optional[int] = -1
        self.total_returned_rows_count: Optional[int] = -1
        self.query_generation_time: Optional[float] = None
        self.data_extraction_time: Optional[float] = None
        self.data_conversion_time: Optional[float] = None
        self.data_filtered_from_permission_time: Optional[float] = None
        self.compute_stats_time: Optional[float] = None
        self.column_names_and_types: Optional[Dict[str, str]] = None

    def set_data(self, data):
        self.data = data.result()

    def set_total_rows_count(self, count):
        self.total_rows_count = count.result()['TOTAL_ROWS'][0]

    def set_total_returned_rows_count(self, count):
        self.total_returned_rows_count = count

    def set_query_generation_time(self, query_generation_time):
        self.query_generation_time = query_generation_time

    def set_data_conversion_time(self, data_conversion_time):
        self.data_conversion_time = data_conversion_time

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

        query_generation_time = timer() - execution_start
        self.logger.info(
            f'[benchmark][snowflake] - execute {query_generation_time} seconds',
            extra={
                'benchmark': {
                    'operation': 'execute',
                    'query_generation_time': query_generation_time,
                    'connector': 'snowflake',
                    'query': query,
                }
            },
        )
        self.set_query_generation_time(query_generation_time)
        convert_start = timer()
        # Here call our customized fetch
        values = pd.DataFrame.from_dict(query_res.fetchall())

        data_conversion_time = timer() - convert_start
        self.logger.info(
            f'[benchmark][snowflake] - dataframe {data_conversion_time} seconds',
            extra={
                'benchmark': {
                    'operation': 'dataframe',
                    'data_conversion_time': data_conversion_time,
                    'connector': 'snowflake',
                }
            },
        )
        self.set_data_conversion_time(data_conversion_time)

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

        run_count_request = get_row_count and SqlQueryHelper.count_query_needed(query)
        self.logger.info(f'Execute count request: {run_count_request}')
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=2 if run_count_request else 1
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

            if run_count_request:
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

        if run_count_request:
            total_rows = self.total_rows_count
        # FIXME: Buggy in case the length of the dataset is a multiple of offset
        elif limit is None or (limit and len(self.data)) < limit:
            total_rows = (offset or 0) + len(self.data)
        else:
            total_rows = None

        return DataSlice(
            self.data,
            pagination_info=build_pagination_info(
                offset=offset or 0,
                limit=limit,
                retrieved_rows=len(self.data),
                total_rows=total_rows,
            ),
        )

    def fetch_data(
        self,
        connection: SnowflakeConnection,
        data_source: SfDataSource,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        get_row_count: bool = False,
    ) -> DataSlice:
        extraction_start = timer()
        if data_source.database != connection.database:
            self.logger.info(f'Connection changed to use database {connection.database}')
            self._execute_query(connection, f'USE DATABASE {data_source.database}')
        if data_source.warehouse and data_source.warehouse != connection.warehouse:
            self.logger.info(f'Connection changed to use  warehouse {connection.warehouse}')
            self._execute_query(connection, f'USE WAREHOUSE {data_source.warehouse}')

        ds = self._execute_parallelized_queries(
            connection, data_source.query, data_source.parameters, offset, limit, get_row_count
        )
        self.data_extraction_time = timer() - extraction_start

        return ds

    def retrieve_data(
        self, connection: SnowflakeConnection, data_source: SfDataSource, get_row_count: bool = None
    ) -> pd.DataFrame:
        return self.fetch_data(connection, data_source, get_row_count=get_row_count).df

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
            query_generation_time=self.query_generation_time,
            data_extraction_time=self.data_extraction_time,
            data_conversion_time=self.data_conversion_time,
            df_memory_size=result.df.memory_usage().sum(),
        )
        return DataSlice(
            df=result.df,
            query_metadata=QueryMetadata(columns=self.column_names_and_types),
            # In the case of user defined limit/offset, get the info
            # Not used for now
            # input_parameters={
            #     'limit': SqlQueryHelper.extract_limit(data_source.query),
            #     'offset': SqlQueryHelper.extract_offset(data_source.query),
            # },
            stats=stats,
            pagination_info=result.pagination_info,
        )

    def get_warehouses(
        self, connection: SnowflakeConnection, warehouse_name: Optional[str] = None
    ) -> List[str]:
        query = 'SHOW WAREHOUSES'
        if warehouse_name:
            query = f"{query} LIKE '{warehouse_name}'"
        res = self._execute_query(connection, query).to_dict().get('name')
        return [warehouse for warehouse in res.values()] if res else []

    def get_databases(
        self, connection: SnowflakeConnection, database_name: Optional[str] = None
    ) -> List[str]:
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

        description_time = timer() - description_start
        self.logger.info(
            f'[benchmark][snowflake] - description {description_time} seconds',
            extra={
                'benchmark': {
                    'operation': 'describe',
                    'description_time': description_time,
                    'connector': 'snowflake',
                    'query': query,
                    'result': json.dumps(describe_res),
                }
            },
        )
        res = {r.name: type_code_mapping.get(r.type_code) for r in describe_res}
        return res

    def get_db_content(self, connection: SnowflakeConnection) -> List[Dict[str, Any]]:
        query = build_database_model_extraction_query()
        return self._execute_query(connection, query)
