import os
from urllib.parse import urlparse

from pyhive import hive
import pytest

from toucan_connectors.hive.hive_connector import HiveConnector, HiveDataSource

if 'DOCKER_HOST' in os.environ:
    HOST = urlparse(os.environ['DOCKER_HOST']).hostname
else:
    HOST = 'localhost'


@pytest.fixture(scope='module')
def hive_server(service_container):
    def check_and_feed(host_port):
        cursor = hive.connect(HOST, port=host_port, username='root').cursor()
        cursor.execute('create table tests (a int)')
        cursor.execute('insert into tests (a) values (12)')
        cursor.close()

    return service_container('hive', check_and_feed, timeout=600)


@pytest.fixture
def hive_connector(hive_server):
    return HiveConnector(name='mycon', host=HOST, port=hive_server['port'], username='root')


@pytest.fixture
def hive_datasource():
    return HiveDataSource(name='mycon', domain='mydomain', query='select * from tests')


def test_get_df(hive_connector, hive_datasource):
    df = hive_connector.get_df(hive_datasource)
    assert df['tests.a'].iloc[0] == 12


def test_get_df_w_params(hive_connector):
    datasource = HiveDataSource(name='mycon', domain='mydomain',
                                query='select * from tests where a = %(i)s',
                                parameters={'i': 10})
    df = hive_connector.get_df(datasource)
    assert df.empty

    datasource.parameters['i'] = 12
    df = hive_connector.get_df(datasource)
    assert not df.empty
