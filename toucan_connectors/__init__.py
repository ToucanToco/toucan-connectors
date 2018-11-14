from contextlib import suppress

from .toucan_connector import ToucanDataSource, ToucanConnector

with suppress(ImportError):
    from .adobe_analytics.adobe_analytics_connector import AdobeAnalyticsConnector
with suppress(ImportError):
    from .azure_mssql.azure_mssql_connector import AzureMSSQLConnector
with suppress(ImportError):
    from .dataiku.dataiku_connector import DataikuConnector
with suppress(ImportError):
    from .google_analytics.google_analytics_connector import GoogleAnalyticsConnector
with suppress(ImportError):
    from .google_big_query.google_big_query_connector import GoogleBigQueryConnector
with suppress(ImportError):
    from .google_cloud_mysql.google_cloud_mysql_connector import GoogleCloudMySQLConnector
with suppress(ImportError):
    from .google_spreadsheet.google_spreadsheet_connector import GoogleSpreadsheetConnector
with suppress(ImportError):
    from .hive.hive_connector import HiveConnector
with suppress(ImportError):
    from .http_api.http_api_connector import HttpAPIConnector
with suppress(ImportError):
    from .magento.magento_connector import MagentoConnector
with suppress(ImportError):
    from .micro_strategy.micro_strategy_connector import MicroStrategyConnector
with suppress(ImportError):
    from .mongo.mongo_connector import MongoConnector
with suppress(ImportError):
    from .mssql.mssql_connector import MSSQLConnector
with suppress(ImportError):
    from .mysql.mysql_connector import MySQLConnector
with suppress(ImportError):
    from .odata.odata_connector import ODataConnector
with suppress(ImportError):
    from .oracle_sql.oracle_sql_connector import OracleSQLConnector
with suppress(ImportError):
    from .postgres.postgresql_connector import PostgresConnector
with suppress(ImportError):
    from .sap_hana.sap_hana_connector import SapHanaConnector
with suppress(ImportError):
    from .snowflake.snowflake_connector import SnowflakeConnector
with suppress(ImportError):
    from .toucan_toco.toucan_toco_connector import ToucanTocoConnector

AVAILABLE_CONNECTORS = {child.type: child for child in ToucanConnector.__subclasses__()}
