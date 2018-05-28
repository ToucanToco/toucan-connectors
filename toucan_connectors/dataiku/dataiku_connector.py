from io import StringIO

import dataikuapi
import pandas as pd

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class DataikuDataSource(ToucanDataSource):
    dataset: str


class DataikuConnector(ToucanConnector):
    """
    This is a basic connector for [Dataiku](https://www.dataiku.com/) using their
    [DSS API](https://doc.dataiku.com/dss/2.0/api/index.html).
    """
    type = "Dataiku"
    data_source_model: DataikuDataSource

    host: str
    apiKey: str
    project: str

    def get_df(self, data_source: DataikuDataSource) -> pd.DataFrame:
        client = dataikuapi.DSSClient(self.host, self.apiKey)
        data_url = f'/projects/{self.project}/datasets/{data_source.dataset}/data/'
        stream = client._perform_raw("GET", data_url, params={"format": "tsv-excel-header"})
        return pd.read_csv(StringIO(stream.text), sep='\t')
