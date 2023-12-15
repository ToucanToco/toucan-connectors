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


def build_database_model_extraction_query() -> str:
    return """select t.table_catalog as database,
    t.table_schema as schema,
    CASE WHEN t.table_type = 'BASE TABLE' THEN 'table' ELSE lower(t.table_type) END as type,
    t.table_name as name
    from
        information_schema.tables t
    where t.table_type in ('BASE TABLE', 'VIEW')
    and t.table_schema not in  ('pg_catalog', 'information_schema', 'pg_internal')
    and t.table_name not like 'redshift%'
    group by t.table_catalog, t.table_schema, t.table_name, t.table_type
    order by schema;"""


def merge_columns_and_tables(cols: pd.DataFrame, tables: pd.DataFrame):
    output = pd.merge(tables, cols, on=["database", "name"])
    return output.to_dict("records")
