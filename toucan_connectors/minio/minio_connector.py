from io import StringIO

import pandas as pd

from pydantic import constr

from minio import Minio
from minio.error import ResponseError

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class MinioDataSource(ToucanDataSource):
    bucketname: constr(min_length=1)
    objectname: constr(min_length=1)
    separator: str = '\t'


class MinioConnector(ToucanConnector):
    """
    Import file from Minio Server.
    """
    type = 'Minio'
    data_source_model: MinioDataSource

    access_key: constr(min_length=1)
    secret_key: constr(min_length=1)

    def get_df(self, data_source: MinioDataSource) -> pd.DataFrame:
        try:
            client = Minio('s3.amazonaws.com',
                           access_key=self.access_key,
                           secret_key=self.secret_key,
                           secure=True)
            data = client.get_object(data_source.bucketname, data_source.objectname)
            df = pd.read_csv(StringIO(data), separator=data_source.separator)
        except ResponseError:
            return None
        return df
