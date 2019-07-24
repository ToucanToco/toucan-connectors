from importlib import import_module

from .toucan_connector import DataSlice, ToucanDataSource, ToucanConnector

CONNECTORS_CATALOGUE = {
    'AdobeAnalytics': 'adobe_analytics.adobe_analytics_connector.AdobeAnalyticsConnector',
    'AzureMSSQL': 'azure_mssql.azure_mssql_connector.AzureMSSQLConnector',
    'Dataiku': 'dataiku.dataiku_connector.DataikuConnector',
    'elasticsearch': 'elasticsearch.elasticsearch_connector.ElasticsearchConnector',
    'facebook_insights': 'facebook_insights.facebook_insights_connector.FacebookInsightsConnector',
    'GoogleAnalytics': 'google_analytics.google_analytics_connector.GoogleAnalyticsConnector',
    'GoogleBigQuery': 'google_big_query.google_big_query_connector.GoogleBigQueryConnector',
    'GoogleCloudMySQL': 'google_cloud_mysql.google_cloud_mysql_connector.GoogleCloudMySQLConnector',
    'google_my_business': 'google_my_business.google_my_business_connector.GoogleMyBusinessConnector',
    'GoogleSpreadsheet': 'google_spreadsheet.google_spreadsheet_connector.GoogleSpreadsheetConnector',
    'Hive': 'hive.hive_connector.HiveConnector',
    'HttpAPI': 'http_api.http_api_connector.HttpAPIConnector',
    'MicroStrategy': 'micro_strategy.micro_strategy_connector.MicroStrategyConnector',
    'MongoDB': 'mongo.mongo_connector.MongoConnector',
    'MSSQL': 'mssql.mssql_connector.MSSQLConnector',
    'MySQL': 'mysql.mysql_connector.MySQLConnector',
    'OData': 'odata.odata_connector.ODataConnector',
    'OracleSQL': 'oracle_sql.oracle_sql_connector.OracleSQLConnector',
    'Postgres': 'postgres.postgresql_connector.PostgresConnector',
    'SapHana': 'sap_hana.sap_hana_connector.SapHanaConnector',
    'Snowflake': 'snowflake.snowflake_connector.SnowflakeConnector',
    'ToucanToco': 'toucan_toco.toucan_toco_connector.ToucanTocoConnector',
    'Trello': 'trello.trello_connector.TrelloConnector',
    'Wootric': 'wootric.wootric_connector.WootricConnector',
}
ALL_CONNECTOR_TYPES = list(CONNECTORS_CATALOGUE)
AVAILABLE_CONNECTORS = {}

for connector_type, connector_path in CONNECTORS_CATALOGUE.items():
    module_path, connector_cls_name = connector_path.rsplit('.', 1)
    try:
        mod = import_module(f'.{module_path}', 'toucan_connectors')
    except ImportError:
        pass
    else:
        connector_cls = getattr(mod, connector_cls_name)
        AVAILABLE_CONNECTORS[connector_type] = connector_cls
