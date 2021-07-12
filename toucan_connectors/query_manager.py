import re
from typing import Dict, Optional, Tuple

from toucan_connectors.common import convert_to_printf_templating_style, convert_to_qmark_paramstyle


class QueryManager:
    check_regex = []

    @staticmethod
    def prepare_count_query(
        query_string: str,
        query_parameters: Optional[Dict] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Tuple[str, list]:
        """ Build the count(*) query from input query """
        prepared_query, prepared_values = QueryManager.prepare_query(
            query_string, query_parameters, offset, limit
        )
        prepared_query = prepared_query.lower()
        prepared_query = re.sub(r'\s*offset\s*\d*', '', prepared_query)
        prepared_query = re.sub(r'\s*limit\s*\d*', '', prepared_query)
        prepared_query = prepared_query.replace(';', '')
        prepared_query = f'SELECT COUNT(*) AS TOTAL_ROWS FROM ({prepared_query});'
        return prepared_query, prepared_values

    @staticmethod
    def count_request_needed(
        query: str,
        get_row_count: bool,
        limit: Optional[int] = None,
    ) -> bool:
        if get_row_count and 'select' in query.lower():
            if 'limit' in query.lower() or limit:
                return True
        return False

    @staticmethod
    def prepare_query(
        query: str,
        query_parameters: Optional[Dict] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> Tuple[str, list]:
        """Prepare actual query by applying parameters and limit / offset retrictions"""
        query = convert_to_printf_templating_style(query)

        converted_query, ordered_values = convert_to_qmark_paramstyle(query, query_parameters)
        extracted_limit = QueryManager.extract_limit(query)
        extracted_offset = QueryManager.extract_offset(query)

        if limit and offset:
            if extracted_limit and limit < extracted_limit:
                converted_query = re.sub(r'(?<=limit)\s*\d*', str(limit), converted_query)
                if not extracted_offset and offset:
                    converted_query = f'{converted_query.replace(";", "")} OFFSET {offset};'

            if not extracted_limit and limit:
                converted_query = f'{converted_query.replace(";", "")} LIMIT {limit};'
                if offset:
                    converted_query = f'{converted_query.replace(";", "")} OFFSET {offset};'

        return converted_query, ordered_values

    @staticmethod
    def filter_request(query: str, limit: Optional[int]):
        with_sub_query = len(re.findall(r'(?i)^select.*select.*', query)) >= 1
        if with_sub_query:
            print('sub_query')
            return len(re.findall(r'(?i).*(limit|count|max|min|sum|avg).*select.*', query)) >= 1
        else:
            print('query')
            return len(re.findall(r'(?i).*(limit|count|max|min|sum|avg).*', query)) >= 1

    @staticmethod
    def extract_limit(query: str) -> Optional[int]:
        m = re.search(r'(?<=\slimit)\s*\d*\s*', query, re.I)
        if m:
            try:
                return int(m[0].strip())
            except (TypeError, IndexError):
                return None
        else:
            return None

    @staticmethod
    def extract_offset(query: str) -> Optional[int]:
        m = re.search(r'(?<=\soffset)\s*\d*\s*', query, re.I)
        if m:
            try:
                return int(m[0].strip())
            except (TypeError, IndexError):
                return None
        else:
            return None

    @staticmethod
    def fetchmany(executed_cursor):

        size = executed_cursor.arraysize
        ret = []
        while size > 0:
            row = executed_cursor.fetchone()
            if not row:
                break
            ret.append(row)
            size -= 1 if size else None

        return ret
