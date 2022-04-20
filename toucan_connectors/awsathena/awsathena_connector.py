import awswrangler as wr
import boto3
import pandas as pd
from pydantic import Field, constr

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class AwsathenaDataSource(ToucanDataSource):
    name: str = Field(..., description='Your AWS Athena connector name')
    database: constr(min_length=1) = Field(
        ..., description='The name of the database you want to query.'
    )
    query: constr(min_length=1) = Field(
        ...,
        description='The SQL query to execute.',
        widget='sql',
    )


class AwsathenaConnector(ToucanConnector):
    data_source_model: AwsathenaDataSource

    name: str = Field(..., description='Your AWS Athena connector name')

    s3_output_bucket: str = Field(
        ..., description='Your S3 Output bucket (where query results are stored.)'
    )
    aws_access_key_id: str = Field(..., description='Your AWS access key ID')
    aws_secret_access_key: str = Field(..., description='Your AWS secret key')
    region_name: str = Field(..., description='Your AWS region name')

    def get_session(self):
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )
        return session

    def _retrieve_data(self, data_source: AwsathenaDataSource) -> pd.DataFrame:
        query_params = data_source.parameters or {}
        df = wr.athena.read_sql_query(
            data_source.query,
            database=data_source.database,
            boto3_session=self.get_session(),
            params=query_params
        )
        return df
