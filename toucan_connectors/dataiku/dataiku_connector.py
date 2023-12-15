from io import StringIO

import dataikuapi
import pandas as pd
from pydantic import Field

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class DataikuDataSource(ToucanDataSource):
    dataset: str


class DataikuConnector(ToucanConnector, data_source_model=DataikuDataSource):
    """
    This is a basic connector for [Dataiku](https://www.dataiku.com/) using their
    [DSS API](https://doc.dataiku.com/dss/2.0/api/index.html).
    """

    host: str = Field(
        ...,
        description="The domain name (preferred option as more dynamic) or "
        "the hardcoded IP address of your Dataiku server",
    )
    apiKey: str = Field(..., title="API key")  # noqa: N815
    project: str

    def _retrieve_data(self, data_source: DataikuDataSource) -> pd.DataFrame:
        client = dataikuapi.DSSClient(self.host, self.apiKey)
        data_url = f"/projects/{self.project}/datasets/{data_source.dataset}/data/"
        stream = client._perform_raw("GET", data_url, params={"format": "tsv-excel-header"})
        return pd.read_csv(StringIO(stream.text), sep="\t")
