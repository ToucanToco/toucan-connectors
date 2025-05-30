import re

from toucan_connectors.common import convert_to_printf_templating_style, convert_to_qmark_paramstyle


class SqlQueryHelper:
    @staticmethod
    def count_query_needed(
        query: str,
    ) -> bool:
        # We can process all type of SQL queries and some return payload for which we don't want
        # or cannot get the total row count, like DESCRIBE or SHOW
        return bool(re.search(r"select.*", query, re.I))

    @staticmethod
    def prepare_count_query(query_string: str, query_parameters: dict | None = None) -> tuple[str, list]:
        """Build the count(*) query by removing the limit and the offset and adding a count query above from input
        query"""
        prepared_query, prepared_values = SqlQueryHelper.prepare_query(query_string, query_parameters)
        prepared_query = prepared_query.replace(";", "")
        prepared_query = f"SELECT COUNT(*) AS TOTAL_ROWS FROM ({prepared_query});"  # noqa: S608
        return prepared_query, prepared_values

    @staticmethod
    def prepare_limit_query(
        query_string: str,
        query_parameters: dict | None = None,
        offset: int | None = None,
        limit: int | None = None,
    ) -> tuple[str, list]:
        """Build a new query by adding a select query with a limit above from input query"""
        prepared_query, prepared_values = SqlQueryHelper.prepare_query(query_string, query_parameters)
        query_check = prepared_query.strip().lower()
        if not query_check.startswith("show") and not query_check.startswith("describe"):
            if limit and offset:
                prepared_query = prepared_query.replace(";", "")
                prepared_query = f"SELECT * FROM ({prepared_query}) LIMIT {limit} OFFSET {offset};"  # noqa: S608
            elif limit:
                prepared_query = prepared_query.replace(";", "")
                prepared_query = f"SELECT * FROM ({prepared_query}) LIMIT {limit};"  # noqa: S608

        return prepared_query, prepared_values

    @staticmethod
    def prepare_query(query: str, query_parameters: dict | None = None) -> tuple[str, list]:
        """Prepare actual query by applying parameters and limit / offset restrictions"""
        query = convert_to_printf_templating_style(query)
        converted_query, ordered_values = convert_to_qmark_paramstyle(query, query_parameters)
        return converted_query, ordered_values

    @staticmethod
    def extract_offset(query: str) -> int | None:
        m = re.search(r"(?<=\soffset)\s*\d*\s*", query, re.I)
        if m:
            try:
                return int(m[0].strip())
            except ValueError:
                return None
        else:
            return None

    @staticmethod
    def extract_limit(query: str) -> int | None:
        m = re.search(r"(?<=\slimit)\s*\d*\s*", query, re.I)
        if m:
            try:
                return int(m[0].strip())
            except ValueError:
                return None
        else:
            return None
