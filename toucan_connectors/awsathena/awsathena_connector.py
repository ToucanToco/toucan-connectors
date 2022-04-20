import awswrangler as wr
import pandas as pd
from pydantic import Field, constr
import boto3

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class AwsathenaDataSource(ToucanDataSource):
    name: str = Field(..., description='Your AWS Athena connector name')

    database: str = Field(
        None,
        description='The name of the database you want to query. '
                    "By default SQL Server selects the user's default database",
    )
    table: constr(min_length=1) = Field(
        None,
        description='The name of the data table that you want to '
                    'get (equivalent to "SELECT * FROM '
                    'your_table")',
    )
    query: constr(min_length=1) = Field(
        None,
        description='You can write a custom query against your '
                    'database here. It will take precedence over '
                    'the "table" parameter above',
        widget='sql',
    )

    def __init__(self, **data):
        super().__init__(**data)
        query = data.get('query')
        table = data.get('table')
        if query is None and table is None:
            raise ValueError("'query' or 'table' must be set")
        elif query is None and table is not None:
            self.query = f'select * from {table};'


class AwsathenaConnector(ToucanConnector):
    data_source_model: AwsathenaDataSource

    name: str = Field(..., description='Your AWS Athena connector name')

    s3_output_bucket: str = Field(..., description='Your S3 Output bucket')
    aws_access_key_id: str = Field(..., description='Your AWS access Key')
    aws_secret_access_key: str = Field(..., description='Your AWS Secret Key')
    region_name: str = Field(..., description='Your Region Name')

    def get_session(self):
        session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )
        return session

    def _retrieve_data(self, data_source: AwsathenaDataSource) -> pd.DataFrame:
        df = wr.athena.read_sql_query(
            data_source.query,
            database=data_source.database,
            boto3_session=self.get_session()
        )
        return df
