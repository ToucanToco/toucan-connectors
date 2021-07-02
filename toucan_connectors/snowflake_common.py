import logging
from timeit import default_timer as timer
from typing import Dict, List, Optional

import pandas as pd
from pydantic import Field, constr
from snowflake.connector import DictCursor

from toucan_connectors.common import convert_to_printf_templating_style, convert_to_qmark_paramstyle
from toucan_connectors.toucan_connector import DataSlice, ToucanDataSource


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


class SnowflakeCommon:

    logger = logging.getLogger(__name__)

    def _execute_query(
        self,
        c,
        query: str,
        query_parameters: Optional[Dict] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        execution_start = timer()
        query = convert_to_printf_templating_style(query)
        converted_query, ordered_values = convert_to_qmark_paramstyle(query, query_parameters)

        cursor = c.cursor(DictCursor)
        query_res = cursor.execute(converted_query, ordered_values)

        execution_end = timer()
        self.logger.info(
            f'[benchmark] - execute {execution_end - execution_start} seconds',
            extra={
                'benchmark': {
                    'operation': 'execute',
                    'execution_time': execution_end - execution_start,
                }
            },
        )

        convert_start = timer()
        if offset and limit:
            self.logger.debug('limit & offset')
            rows = limit + offset
            values = pd.DataFrame.from_dict(query_res.fetchmany(rows))
        elif limit and not offset:
            self.logger.debug('limit & not offset')
            values = pd.DataFrame.from_dict(query_res.fetchmany(limit))
        elif not limit and not offset:
            self.logger.debug('not limit & not offset')
            values = pd.DataFrame.from_dict(query_res.fetchall())
        else:
            values = pd.DataFrame.from_dict(query_res.fetchall())

        convert_end = timer()
        self.logger.info(
            f'[benchmark] - dataframe {convert_end - convert_start} seconds',
            extra={
                'benchmark': {
                    'operation': 'dataframe',
                    'execution_time': convert_end - convert_start,
                }
            },
        )
        return values

    def _fetch_data(
        self,
        c,
        data_source: SfDataSource,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        df = self._execute_query(c, data_source.query, data_source.parameters, offset, limit)
        return df

    def retrieve_data(self, c, data_source: SfDataSource) -> pd.DataFrame:
        return self._fetch_data(c, data_source)

    def get_slice(
        self,
        c,
        data_source: SfDataSource,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> DataSlice:
        df: pd.DataFrame = self._fetch_data(c, data_source, offset, limit)
        if offset and limit:
            result = df[offset : limit + offset]
        else:
            result = df
        return DataSlice(result, len(result))

    def get_warehouses(self, c, warehouse_name: Optional[str] = None) -> List[str]:
        query = 'SHOW WAREHOUSES'
        if warehouse_name:
            query = query + ' LIKE ' + warehouse_name
        res = self._execute_query(c, query).to_dict().get('name')
        return [warehouse for warehouse in res.values()] if res else []

    def get_databases(self, c, database_name: Optional[str] = None) -> List[str]:
        query = 'SHOW DATABASES'
        if database_name:
            query = query + ' LIKE ' + database_name
        res = self._execute_query(c, query).to_dict().get('name')
        return [database for database in res.values()] if res else []
