import json

import pandas as pd

types_map = {
    16: "BOOLEAN",
    20: "BIGINT",
    21: "SMALLINT",
    23: "INTEGER",
    700: "REAL",
    701: "DOUBLE_PRECISION",
    1008: "VARCHAR",
    1009: "VARCHAR",
    1014: "VARCHAR",
    1015: "VARCHAR",
    1082: "DATE",
    1043: "VARCHAR",
    1114: "TIMESTAMPTZ",
    1184: "TIMESTAMPTZ",
    1700: "DECIMAL",
    2951: "VARCHAR",
}


def create_columns_query(database: str):
    records = (
        """REPLACE(CHR(123) || '"name"' || ':' || '"' || c.column_name"""
        """ || '"' || ',' ||  '"type"' ||  ':'  || '"' || c.data_type  || '"' || CHR(125), '""', '"')"""
    )
    return (
        f"SELECT table_catalog as database, table_name as name, {records} as columns from information_schema.columns c "
        f"WHERE table_schema not in ('information_schema', 'pg_catalog') and table_name not like 'redshift%'"
    )


def aggregate_columns(df: pd.DataFrame):
    df["columns"] = df["columns"].apply(json.loads)
    df = pd.DataFrame(df.groupby(["database", "name"])["columns"].apply(lambda x: list(x))).reset_index()
    return df


def merge_columns_and_tables(cols: pd.DataFrame, tables: pd.DataFrame):
    output = pd.merge(tables, cols, on=["database", "name"])
    return output.to_dict("records")


def build_database_model_extraction_query(
    db_name: str,
    schema_name: str | None,
    table_name: str | None,
) -> str:
    database_name = f"'{db_name}'" if db_name else "NULL"
    # Redshift does not support JSON object creation functions nor ARRAY_AGG/JSON_ARRAY_AGG
    query = f"""
    SELECT
        {database_name} as "database",
        n.nspname AS schema,
        'table' AS "table_type",
        c.relname AS table_name,
        -- Manually create JSON objects
        '[' || LISTAGG(
            '{{"name": "' || a.attname || '", "type": "' || t.typname || '" }}',
            ', '
        ) WITHIN GROUP (ORDER BY a.attnum) || ']' AS columns
    FROM
        pg_namespace n
        -- Join pg_class to pg_namespace to get tables for each schema
        JOIN pg_class c ON c.relnamespace = n.oid
        -- Join pg_attribute to pg_class to get column information for each table
        JOIN pg_attribute a ON a.attrelid = c.oid
        -- Join pg_type to pg_attribute to get data type information for each column
        JOIN pg_type t ON a.atttypid = t.oid
    """
    where_clause = """WHERE
        n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_internal') -- Exclude system schemas
        AND c.relkind = 'r'                                                  -- Only include ordinary tables
        AND a.attnum > 0                                                     -- Exclude system columns
        AND NOT a.attisdropped                                               -- Exclude dropped columns
    """
    if schema_name:
        where_clause += f" AND schema = '{schema_name}'\n"
    if table_name:
        where_clause += f" AND table_name = '{table_name}'\n"

    group_and_order = """
    GROUP BY
        n.nspname, c.relname
    ORDER BY
        n.nspname, c.relname;
    """
    return query + where_clause + group_and_order
