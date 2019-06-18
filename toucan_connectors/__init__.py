import os
import re
from importlib import import_module

from .toucan_connector import ToucanDataSource, ToucanConnector

ALL_CONNECTORS_MAPPING = [
    ('adobe_analytics.adobe_analytics_connector', 'AdobeAnalyticsConnector'),
    ('azure_mssql.azure_mssql_connector','AzureMSSQLConnector'),
    ('dataiku.dataiku_connector', 'DataikuConnector'),
    ('elasticsearch.elasticsearch_connector', 'ElasticsearchConnector'),
    ('google_analytics.google_analytics_connector', 'GoogleAnalyticsConnector'),
    ('google_big_query.google_big_query_connector', 'GoogleBigQueryConnector'),
    ('google_cloud_mysql.google_cloud_mysql_connector', 'GoogleCloudMySQLConnector'),
    ('google_my_business.google_my_business_connector', 'GoogleMyBusinessConnector'),
    ('google_spreadsheet.google_spreadsheet_connector', 'GoogleSpreadsheetConnector'),
    ('hive.hive_connector', 'HiveConnector'),
    ('http_api.http_api_connector', 'HttpAPIConnector'),
    ('micro_strategy.micro_strategy_connector', 'MicroStrategyConnector'),
    ('mongo.mongo_connector', 'MongoConnector'),
    ('mssql.mssql_connector', 'MSSQLConnector'),
    ('mysql.mysql_connector', 'MySQLConnector'),
    ('odata.odata_connector', 'ODataConnector'),
    ('oracle_sql.oracle_sql_connector', 'OracleSQLConnector'),
    ('postgres.postgresql_connector', 'PostgresConnector'),
    ('sap_hana.sap_hana_connector', 'SapHanaConnector'),
    ('snowflake.snowflake_connector', 'SnowflakeConnector'),
    ('toucan_toco.toucan_toco_connector', 'ToucanTocoConnector'),
    ('trello.trello_connector', 'TrelloConnector'),
]
HERE = os.path.dirname(__file__)


def _import_connector(con_path, con_class):
    try:
        mod = import_module(f'.{con_path}', 'toucan_connectors')
        con_type = getattr(mod, con_class).type
    except ImportError:
        # Retrieve the connector type by parsing the file
        con_file_path = f'{HERE}/{con_path.replace(".", "/")}.py'
        with open(con_file_path) as f:
            con_code = f.read()
        con_type = re.search(con_class + r'\(ToucanConnector\):.*type\s*=\s*[\'"](\w+)[\'"]', con_code, re.S).group(1)
    return con_type


ALL_CONNECTOR_TYPES = []
for connector_path, connector_class in ALL_CONNECTORS_MAPPING:
    connector_type = _import_connector(connector_path, connector_class)
    ALL_CONNECTOR_TYPES.append(connector_type)


AVAILABLE_CONNECTORS = {child.type: child for child in ToucanConnector.__subclasses__()}
for child in ToucanConnector.__subclasses__():
    locals()[child.__name__] = child
