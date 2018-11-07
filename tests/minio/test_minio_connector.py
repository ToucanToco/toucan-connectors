from toucan_connectors.minio.minio_connector import MinioConnector, MinioDataSource


mc = MinioConnector(
    access_key='XXX',
    secret_key='XXX',
)

mds = MinioDataSource(
    bucketname='XXX',
    objectname='XXX'
)

def test_minio_connector():
    raise NotImplementedError('implement me')
