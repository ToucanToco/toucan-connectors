import base64
import mimetypes
from contextlib import suppress
from importlib import import_module
from pathlib import Path

from .toucan_connector import DataSlice, ToucanConnector, ToucanDataSource

CONNECTORS_REGISTRY = {
    'AdobeAnalytics': {
        'connector': 'adobe_analytics.adobe_analytics_connector.AdobeAnalyticsConnector',
        'label': 'Adobe Analytics',
        'logo': 'adobe_analytics/adobe-analytics.png',
    },
    'Aircall': {
        'connector': 'aircall.aircall_connector.AircallConnector',
        'logo': 'aircall/Aircall.svg',
    },
    'AWSDocumentDB': {
        'connector': 'mongo.mongo_connector.MongoConnector',
        'label': 'Amazon Document DB',
        'logo': 'aws/aws.png',
    },
    'AWSRedshift': {
        'connector': 'postgres.postgresql_connector.PostgresConnector',
        'label': 'Amazon Redshift',
        'logo': 'aws/aws.png',
    },
    'AzureMSSQL': {
        'connector': 'azure_mssql.azure_mssql_connector.AzureMSSQLConnector',
        'label': 'Microsoft Azure SQL',
        'logo': 'azure_mssql/sql-azure.png',
    },
    'Dataiku': {
        'connector': 'dataiku.dataiku_connector.DataikuConnector',
        'logo': 'dataiku/dataiku.png',
    },
    'DatabricksDeltaLake': {
        'connector': 'azure_mssql.azure_mssql_connector.AzureMSSQLConnector',
        'label': 'Databricks Delta Lake',
        'logo': 'databricks_delta_lake/databricks.png',
    },
    'Denodo': {
        'connector': 'postgres.postgresql_connector.PostgresConnector',
        'label': 'Denodo',
        'logo': 'denodo/denodo.png',
    },
    'elasticsearch': {
        'connector': 'elasticsearch.elasticsearch_connector.ElasticsearchConnector',
        'label': 'Elasticsearch',
        'logo': 'elasticsearch/elasticsearch.png',
    },
    'facebook_insights': {
        'connector': 'facebook_insights.facebook_insights_connector.FacebookInsightsConnector',
        'label': 'Facebook Insights',
        'logo': 'facebook_insights/facebook-insights.png',
    },
    'GoogleAnalytics': {
        'connector': 'google_analytics.google_analytics_connector.GoogleAnalyticsConnector',
        'label': 'Google Analytics',
        'logo': 'google_analytics/google-analytics.png',
    },
    'GoogleBigQuery': {
        'connector': 'google_big_query.google_big_query_connector.GoogleBigQueryConnector',
        'label': 'Google Big Query',
        'logo': 'google_big_query/google-bigquery.png',
    },
    'GoogleCloudMySQL': {
        'connector': 'google_cloud_mysql.google_cloud_mysql_connector.GoogleCloudMySQLConnector',
        'label': 'Google Cloud MySQL',
        'logo': 'google_cloud_mysql/google-cloud-mysql.png',
    },
    'google_my_business': {
        'connector': 'google_my_business.google_my_business_connector.GoogleMyBusinessConnector',
        'label': 'Google My Business',
        'logo': 'google_my_business/google-my-business.png',
    },
    'GoogleSheets': {
        'connector': 'google_sheets.google_sheets_connector.GoogleSheetsConnector',
        'label': 'Google Sheets',
        'logo': 'google_sheets/google-sheets.png',
    },
    'GoogleSpreadsheet': {
        'connector': 'google_spreadsheet.google_spreadsheet_connector.GoogleSpreadsheetConnector',
        'label': 'Google Spreadsheet',
        'logo': 'google_spreadsheet/google-spreadsheet.png',
    },
    'Hive': {'connector': 'hive.hive_connector.HiveConnector', 'logo': 'hive/hive.png',},
    'HttpAPI': {
        'connector': 'http_api.http_api_connector.HttpAPIConnector',
        'label': 'Http API',
        'logo': 'http_api/http-api.png',
    },
    'Indexima': {'connector': 'hive.hive_connector.HiveConnector', 'logo': 'hive/indexima.png',},
    'Lightspeed': {
        'connector': 'lightspeed.lightspeed_connector.LightspeedConnector',
        'logo': 'lightspeed/lightspeed.png',
    },
    'MicroStrategy': {
        'connector': 'micro_strategy.micro_strategy_connector.MicroStrategyConnector',
        'logo': 'micro_strategy/microstrategy.png',
    },
    'MongoDB': {'connector': 'mongo.mongo_connector.MongoConnector', 'logo': 'mongo/mongo-db.png',},
    'MSSQL': {
        'connector': 'mssql.mssql_connector.MSSQLConnector',
        'label': 'Microsoft SQL Server',
        'logo': 'mssql/mssql.png',
    },
    'MySQL': {'connector': 'mysql.mysql_connector.MySQLConnector', 'logo': 'mysql/mysql.png',},
    'OData': {'connector': 'odata.odata_connector.ODataConnector', 'logo': 'odata/odata.png',},
    'OracleSQL': {
        'connector': 'oracle_sql.oracle_sql_connector.OracleSQLConnector',
        'label': 'Oracle SQL',
        'logo': 'oracle_sql/oracle-sql.png',
    },
    'Postgres': {
        'connector': 'postgres.postgresql_connector.PostgresConnector',
        'label': 'PostgreSQL',
        'logo': 'postgres/postgres.png',
    },
    'Revinate': {
        'connector': 'revinate.revinate_connector.RevinateConnector',
        'label': 'Revinate',
        'logo': 'revinate/revinate.png',
    },
    'ROK': {'connector': 'rok.rok_connector.RokConnector', 'logo': 'rok/rok.png',},
    'SapHana': {
        'connector': 'sap_hana.sap_hana_connector.SapHanaConnector',
        'label': 'SAP HANA',
        'logo': 'sap_hana/sap-hana.png',
    },
    'Snowflake': {
        'connector': 'snowflake.snowflake_connector.SnowflakeConnector',
        'logo': 'snowflake/snowflake.png',
    },
    'ToucanToco': {
        'connector': 'toucan_toco.toucan_toco_connector.ToucanTocoConnector',
        'label': 'Toucan Toco',
        'logo': 'toucan_toco/toucan.png',
    },
    'Trello': {
        'connector': 'trello.trello_connector.TrelloConnector',
        'logo': 'trello/trello.png',
    },
    'Wootric': {
        'connector': 'wootric.wootric_connector.WootricConnector',
        'logo': 'wootric/wootric.png',
    },
}


def html_base64_image_src(image_path: str) -> str:
    """From a file path, create the html src to be used in a browser"""
    with open(image_path, 'rb') as image_file:
        base64_image = base64.b64encode(image_file.read()).decode('utf8')
    mimetype, _ = mimetypes.guess_type(image_path)
    return f'data:{mimetype};base64, {base64_image}'


for connector_type, connector_infos in CONNECTORS_REGISTRY.items():
    # Remove the path of the connector and set the connector class if available
    connector_path = connector_infos.pop('connector')
    module_path, connector_cls_name = connector_path.rsplit('.', 1)
    try:
        mod = import_module(f'.{module_path}', 'toucan_connectors')
    except ImportError:
        pass
    else:
        connector_cls = getattr(mod, connector_cls_name)
        connector_infos['connector'] = connector_cls
        with suppress(AttributeError):
            connector_infos['bearer_integration'] = connector_cls.bearer_integration
        # check if connector implements `get_status`,
        # which is hence different from `ToucanConnector.get_status`
        connector_infos['hasStatusCheck'] = (
            connector_cls.get_status is not connector_cls.__bases__[0].get_status
        )

    # Set default label if not set
    if 'label' not in connector_infos:
        connector_infos['label'] = connector_type

    # Convert logo into base64
    logo_path = Path(__file__).parent / connector_infos.get('logo', 'default-logo.png')
    connector_infos['logo'] = html_base64_image_src(str(logo_path.resolve()))
