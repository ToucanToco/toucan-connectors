from time import time

import pandas as pd
import pytest
import tenacity as tny
from pydantic import create_model

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.pagination import OffsetLimitInfo
from toucan_connectors.toucan_connector import (
    DiscoverableConnector,
    MalformedVersion,
    TableInfo,
    ToucanConnector,
    ToucanDataSource,
    UnavailableVersion,
    VersionableEngineConnector,
    strlist_to_enum,
)


class DataSource(ToucanDataSource):
    query: str
    parameters: dict = {}


class DataConnector(ToucanConnector, data_source_model=DataSource):
    type: str = "MyDB"
    a_parameter: str = ""

    def _retrieve_data(self, data_source):
        pass


################################################
def test_missing_attributes():
    # missing data_source_model
    with pytest.raises(TypeError, match="data_source_model"):

        class MissingDataConnector2(ToucanConnector):
            type: str = "MyDB"

            def _retrieve_data(self, data_source):
                pass


def test_no_get_df():
    class BadDataConnector(ToucanConnector, data_source_model=DataSource):
        type: str = "MyDB"

    with pytest.raises(TypeError):
        BadDataConnector(name="my_name")


def test_type():
    dc = DataConnector(**{"name": "my_name"})
    assert dc.type == "MyDB"
    assert dc.name == "my_name"
    assert dc.data_source_model == DataSource


def test_validate():
    dc = DataConnector(name="my_name")
    dc.data_source_model.validate({"query": "", "name": "my_name", "domain": "my_domain"})


def test_formated_engine_version():
    class DataConnector(ToucanConnector, VersionableEngineConnector, data_source_model=DataSource):
        type: str = "MyDB"

        def get_engine_version(self) -> tuple:
            return super().get_engine_version()

        def _retrieve_data(self, datasource):
            return pd.DataFrame({"A": [1, 2]})

    dc = DataConnector(name="test")
    assert dc._format_version(1) == (1,)
    assert dc._format_version("1") == (1,)
    assert dc._format_version(1.2) == (1, 2)
    assert dc._format_version("1.2") == (1, 2)
    assert dc._format_version("14.3 (Debian 14.3-1.pgdg110+1)") == (14, 3)

    with pytest.raises(MalformedVersion):
        assert dc._format_version("this is a bad version form !!!")

    with pytest.raises(UnavailableVersion):
        assert dc._format_version(None)


def test_get_df_with_permissions():
    class DataConnector(ToucanConnector, data_source_model=DataSource):
        type: str = "MyDB"

        def _retrieve_data(self, datasource):
            return pd.DataFrame({"A": [1, 2]})

    connector = DataConnector(name="my_name")
    ds = connector.data_source_model(domain="yo", name="my_name", query="")
    df = connector.get_df(ds, permissions={"column": "A", "operator": "eq", "value": 1})
    assert all(df == pd.DataFrame({"A": [1]}))


def test_get_slice():
    class DataConnector(ToucanConnector, data_source_model=DataSource):
        type: str = "MyDB"

        def _retrieve_data(self, datasource):
            return pd.DataFrame({"A": [1, 2, 3, 4, 5]})

    # without offset without limit
    res = DataConnector(name="my_name").get_slice({})
    assert res.df.reset_index(drop=True).equals(pd.DataFrame({"A": [1, 2, 3, 4, 5]}))

    # without offset with limit
    res = DataConnector(name="my_name").get_slice({}, limit=1)
    assert res.df.reset_index(drop=True).equals(pd.DataFrame({"A": [1]}))

    # with offset without limit
    res = DataConnector(name="my_name").get_slice({}, offset=2)
    assert res.df.reset_index(drop=True).equals(pd.DataFrame({"A": [3, 4, 5]}))
    assert len(res.df) == 3
    assert res.pagination_info.parameters == OffsetLimitInfo(offset=2, limit=None)
    assert res.pagination_info.pagination_info.total_rows == 5

    # with offset with limit
    res = DataConnector(name="my_name").get_slice({}, offset=2, limit=2)
    assert res.df.reset_index(drop=True).equals(pd.DataFrame({"A": [3, 4]}))
    assert len(res.df) == 2
    assert res.pagination_info.parameters == OffsetLimitInfo(offset=2, limit=2)
    assert res.pagination_info.pagination_info.total_rows == 5


def test_explain():
    class DataConnector(ToucanConnector, data_source_model=DataSource):
        type: str = "MyDB"

        def _retrieve_data(self, datasource):
            return pd.DataFrame()

    res = DataConnector(name="my_name").explain({})
    assert res is None


def test_get_status():
    assert DataConnector(name="my_name").get_status() == ConnectorStatus()


def test_get_cache_key():
    connector = DataConnector(name="my_name")
    ds = connector.data_source_model(domain="yo", name="my_name", query="much caching")

    key = connector.get_cache_key(ds)
    # We should get a deterministic identifier:
    # /!\ the identifier will change if the model of the connector or the datasource changes
    assert key == "cc66ddcd-e717-381f-838e-f960e6cb410e"

    ds.query = "wow"
    key2 = connector.get_cache_key(ds)
    assert key2 != key

    ds.query = "much caching"
    key3 = connector.get_cache_key(ds)
    assert key3 == key


def test_get_cache_key_connector_alone():
    connector_a1 = DataConnector(name="a")
    connector_a2 = DataConnector(name="a")
    connector_b = DataConnector(name="b")

    key_a1 = connector_a1.get_cache_key()
    key_a2 = connector_a2.get_cache_key()
    key_b = connector_b.get_cache_key()

    assert key_a1 == key_a2
    assert key_a1 != key_b


def test_get_cache_key_with_custom_variable_syntax():
    connector_a1 = DataConnector(name="a")
    # sample with Google BigQuery `@my_var` syntax
    ds_1 = DataSource(name="ds_1", parameters={"a": 1}, domain="foo", query="bar=@a")
    ds_2 = DataSource(name="ds_1", parameters={"a": 2}, domain="foo", query="bar=@a")
    key_a1 = connector_a1.get_cache_key(ds_1)
    key_a2 = connector_a1.get_cache_key(ds_2)

    assert key_a1 != key_a2


def test_get_cache_key_should_be_different_with_different_permissions():
    connector_a1 = DataConnector(name="a")
    ds_1 = DataSource(name="ds_1", parameters={"a": 1}, domain="foo", query="bar")
    ds_2 = DataSource(name="ds_1", parameters={"a": 2}, domain="foo", query="bar")
    permissions = {"column": "a_group", "value": "{{ a }}", "operator": "in"}
    key_a1 = connector_a1.get_cache_key(ds_1, permissions=permissions)
    key_a2 = connector_a1.get_cache_key(ds_2, permissions=permissions)

    assert key_a1 != key_a2


class UnreliableDataConnector(ToucanConnector, data_source_model=DataSource):
    type: str = "MyUnreliableDB"

    def _retrieve_data(self, data_source, logbook=None):
        if logbook is None:
            logbook = []
        if len(logbook) < 3:
            logbook.append(time())
            raise RuntimeError("try again!")
        logbook.clear()
        return 42


@pytest.mark.skip(reason="Connectors tests currently fail on GitHub CI, for an unknown reason")
def test_max_attempt_df():
    udc = UnreliableDataConnector(name="my_name", retry_policy={"max_attempts": 5})
    result = udc.get_df({})
    assert result == 42


class CustomPolicyDataConnector(ToucanConnector, data_source_model=DataSource):
    type: str = "MyUnreliableDB"

    # NOTE: Leave this as is, the test below actually relies on the param default vzlue being mutated
    def _retrieve_data(self, data_source, logbook=[]):  # noqa: B006
        if len(logbook) < 3:
            logbook.append(time())
            raise RuntimeError("try again!")
        logbook.clear()
        return pd.DataFrame({"1": [42, 32]})

    @property
    def retry_decorator(self):
        return tny.retry(stop=tny.stop_after_attempt(5))


def test_custom_max_attempt_df():
    udc = CustomPolicyDataConnector(name="my_name")
    result = udc.get_df({})
    assert result["1"].values.tolist() == [42, 32]


class CustomRetryOnDataConnector(ToucanConnector, data_source_model=DataSource):
    type: str = "MyUnreliableDB"
    _retry_on = (ValueError,)

    def _retrieve_data(self, data_source, logbook=None):
        if logbook is None:
            logbook = []
        if len(logbook) < 3:
            logbook.append(time())
            raise RuntimeError("try again!")
        logbook.clear()
        return 42


def test_custom_retry_on_df():
    udc = CustomRetryOnDataConnector(name="my_name")
    with pytest.raises(RuntimeError):
        udc.get_df({})


class CustomNoRetryOnDataConnector(ToucanConnector, data_source_model=DataSource):
    type: str = "MyUnreliableDB"

    @property
    def retry_decorator(self):
        return None

    def _retrieve_data(self, data_source, logbook=None):
        if logbook is None:
            logbook = []
        if len(logbook) == 0:
            logbook.append(time())
            raise RuntimeError("try again!")
        logbook.clear()
        return 42


def test_no_retry_on_df():
    udc = CustomNoRetryOnDataConnector(name="my_name")
    with pytest.raises(RuntimeError):
        udc.get_df({})


def test_strlist_to_enum_required():
    """It should be required by default"""
    model = create_model("Test", pokemon=strlist_to_enum("pokemon", ["pika", "bulbi"]))
    assert model.model_json_schema() == {
        "title": "Test",
        "type": "object",
        "$defs": {
            "pokemon": {
                "enum": ["pika", "bulbi"],
                "title": "pokemon",
                "type": "string",
            }
        },
        "properties": {"pokemon": {"$ref": "#/$defs/pokemon"}},
        "required": ["pokemon"],
    }


def test_strlist_to_enum_default_value():
    """It should be possible to add a default value (not required)"""
    model = create_model("Test", pokemon=strlist_to_enum("pokemon", ["pika", "bulbi"], "pika"))
    assert model.model_json_schema() == {
        "title": "Test",
        "type": "object",
        "$defs": {
            "pokemon": {
                "enum": ["pika", "bulbi"],
                "title": "pokemon",
                "type": "string",
            }
        },
        "properties": {"pokemon": {"$ref": "#/$defs/pokemon", "default": "pika"}},
    }


def test_get_df_int_column(mocker):
    """The int column should be casted as str"""

    class DataConnector(ToucanConnector, data_source_model=DataSource):
        type: str = "MyDB"

        def _retrieve_data(self, datasource):
            return pd.DataFrame({0: [1, 2]})

    dc = DataConnector(name="bla")
    assert dc.get_df(mocker.MagicMock()).columns == ["0"]


def test_default_implementation_of_discoverable_connector():
    class DataConnector(ToucanConnector, DiscoverableConnector, data_source_model=DataSource):
        type: str = "MyDB"

        def _retrieve_data(self, datasource):
            return pd.DataFrame()

        def get_model(
            self,
            db_name: str | None = None,
            schema_name: str | None = None,
            table_name: str | None = None,
            exclude_columns: bool = False,
        ) -> list[TableInfo]:
            model = [("database", "schema", "type", "name", [{"name": "column", "type": "type"}])]
            return DiscoverableConnector.format_db_model(model)

    dc = DataConnector(name="test")
    assert dc.get_model_with_info() == (
        [
            {
                "name": "name",
                "database": "database",
                "schema": "schema",
                "type": "type",
                "columns": [{"name": "column", "type": "type"}],
            }
        ],
        {},
    )
