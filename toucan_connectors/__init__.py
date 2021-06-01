import base64
import mimetypes
from contextlib import suppress
from importlib import import_module
from pathlib import Path

from .toucan_connector import DataSlice, ToucanConnector, ToucanDataSource

CONNECTORS_REGISTRY = {
    'ExampleConnector2': {
        'connector': 'exampleconnector2.exampleconnector2_connector.Exampleconnector2Connector',
        'label': 'AAA_Example',
    },

    'Github': {
        'connector': 'github.github_connector.GithubConnector',
        'label': 'Github Connector',
        'logo': 'github/GitHub_Logo.png',
    },
    'HttpAPI': {
        'connector': 'http_api.http_api_connector.HttpAPIConnector',
        'label': 'Http API',
        'logo': 'http_api/http-api.png',
    },
    'Snowflake': {
        'connector': 'snowflake.snowflake_connector.SnowflakeConnector',
        'logo': 'snowflake/snowflake.png',
        'label': 'Snowflake',
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
        with suppress(AttributeError):
            connector_infos['_auth_flow'] = connector_cls._auth_flow
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
