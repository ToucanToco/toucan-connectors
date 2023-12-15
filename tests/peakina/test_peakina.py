import pandas as pd

from toucan_connectors.peakina.peakina_connector import PeakinaConnector, PeakinaDataSource


def test_get_df():
    """Should use peakina connector to get a file for a given uri"""
    file_uri = "tests/peakina/fixtures/test.csv"

    expected_df = pd.read_csv(file_uri)
    psd = PeakinaDataSource(uri=file_uri)
    psc = PeakinaConnector(name="test-peakina-connector")  # type: ignore

    assert psc.get_df(data_source=psd).equals(expected_df)
