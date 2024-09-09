from logging import getLogger

from pydantic import Field, HttpUrl

try:
    import pandas as pd
    from odata import ODataService
    from odata.metadata import MetaData

    # monkey patch MetaData's __init__
    # (cf. https://github.com/tuomur/python-odata/issues/22)
    def metadata_init_patched(self, service):
        self._original_init(service)
        self.url = service.url.rstrip("/") + "/$metadata"

    MetaData._original_init = MetaData.__init__
    MetaData.__init__ = metadata_init_patched

    CONNECTOR_OK = True
except ImportError as exc:  # pragma: no cover
    getLogger(__name__).warning(f"Missing dependencies for {__name__}: {exc}")
    CONNECTOR_OK = False

from toucan_connectors.auth import Auth
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class ODataDataSource(ToucanDataSource):
    entity: str = Field(
        ...,
        description="The entity path that will be appended to your baseroute URL. "
        'For example "geo/countries". For more details, see this '
        '<a href="https://www.odata.org/getting-started/basic-tutorial/" target="_blank">tutorial</a>',
    )
    query: dict = Field(
        ...,
        description="JSON object of parameters with parameter name as key and value as value. "
        'For example {"$filter": "my_value", "$skip": 100} '
        '(equivalent to "$filter=my_value&$skip=100" in parameterized URL). '
        "For more details on query parameters convention, see "
        '<a href="https://www.odata.org/documentation/odata-version-2-0/uri-conventions/" target="_blank">this documentation</a>',  # noqa: E501
        widget="json",
    )


class ODataConnector(ToucanConnector, data_source_model=ODataDataSource):
    baseroute: HttpUrl = Field(..., title="API endpoint", description="Baseroute URL")
    auth: Auth | None = Field(None, title="Authentication type")

    def _retrieve_data(self, data_source: ODataDataSource) -> "pd.DataFrame":
        if self.auth:
            session = self.auth.get_session()
        else:
            session = None

        service = ODataService(str(self.baseroute), reflect_entities=True, session=session)
        entities = service.entities[data_source.entity]
        data = service.query(entities).raw(data_source.query)
        return pd.DataFrame(data)
