import json

import pandas as pd

types_map = {
    16: 'BOOLEAN',
    20: 'BIGINT',
    21: 'SMALLINT',
    23: 'INTEGER',
    700: 'REAL',
    701: 'DOUBLE_PRECISION',
    1008: 'VARCHAR',
    1009: 'VARCHAR',
    1014: 'VARCHAR',
    1015: 'VARCHAR',
    1082: 'DATE',
    1043: 'VARCHAR',
    1114: 'TIMESTAMPTZ',
    1184: 'TIMESTAMPTZ',
    1700: 'DECIMAL',
    2951: 'VARCHAR',
}


def create_columns_query(database: str):
    records = """REPLACE(CHR(123) || '"name"' || ':' || '"' || c.column_name || '"' || ',' ||  '"type"' ||  ':'  || 
    '"' || c.data_type  || '"' || CHR(125), '""', '"')"""
    return f'''SELECT '{database}', table_name, {records} from information_schema.columns c'''


def aggregate_columns(cols_records: tuple):
    df = pd.DataFrame(cols_records)
    df[2] = df[2].apply(json.loads)
    df = pd.DataFrame(df.groupby([0, 1])[2].apply(lambda x: list(x))).reset_index()
    df.columns = ['database', 'table_name', 'columns']
    return df


def create_table_info_query(database: str):
    return f'''select '{database}',
    t.table_schema as schema,
    CASE WHEN t.table_type = 'BASE TABLE' THEN 'table' ELSE lower(t.table_type) END as type,
    t.table_name as name
    from
        information_schema.tables t
    inner join information_schema.columns c on
        t.table_name = c.table_name
    where t.table_type in ('BASE TABLE', 'VIEW')
    and t.table_schema not in  ('pg_catalog', 'information_schema', 'pg_internal')
    group by t.table_schema, t.table_name, t.table_type
    order by schema;
    '''


def merge_columns_and_tables(cols: pd.DataFrame, tables: tuple):
    tables = pd.DataFrame(tables)
    tables.columns = ['database', 'schema', 'table_type', 'table_name']
    output = pd.merge(tables, cols, on=['database', 'table_name'])
    return output.to_dict('records')
