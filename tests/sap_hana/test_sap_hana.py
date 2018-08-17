from pydantic.exceptions import ValidationError
import pytest

from toucan_connectors.sap_hana.sap_hana_connector import SapHanaConnector, SapHanaDataSource


def test_no_user():
    """ It should raise an error as no user is given """
    with pytest.raises(ValidationError):
        SapHanaConnector(host='some_host', name='test')


def test_raise_on_empty_query():
    with pytest.raises(ValidationError):
        SapHanaDataSource(domaine='test', name='test', query='')


def test_postgress_get_df(mocker):
    snock = mocker.patch('pyhdb.connect')
    reasq = mocker.patch('pandas.read_sql')

    postgres_connector = SapHanaConnector(
        name='test', host='localhost', port=22, user='ubuntu', password='truc'
    )
    ds = SapHanaDataSource(domain='test', name='test', query='my_query')
    postgres_connector.get_df(ds)

    snock.assert_called_once_with('localhost', '22', 'ubuntu', 'truc')
    reasq.assert_called_once_with('my_query', con=snock())
