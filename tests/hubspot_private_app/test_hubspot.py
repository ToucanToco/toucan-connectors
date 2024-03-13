from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, call

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from pytest_mock import MockFixture

from toucan_connectors.hubspot_private_app.hubspot_connector import (
    HubspotConnector,
    HubspotDataset,
    HubspotDataSource,
)


def assert_frame_equal(left: pd.DataFrame, right: pd.DataFrame) -> bool:
    return pd_assert_frame_equal(
        left.reindex(columns=sorted(left.columns.to_list())),
        right.reindex(columns=sorted(right.columns.to_list())),
    )


class _DictableDict(dict):
    def to_dict(self) -> dict[str, Any]:
        return self


@pytest.fixture
def hubspot_full_page() -> _DictableDict:
    return _DictableDict(
        **{
            "paging": None,
            "results": [
                {
                    "archived": False,
                    "archived_at": None,
                    "associations": None,
                    "created_at": datetime(2023, 2, 21, 9, 50, 48, 148000, tzinfo=timezone.utc),
                    "id": "1",
                    "properties": {
                        "createdate": "2023-02-21T09:50:48.148Z",
                        "email": "emailmaria@hubspot.com",
                        "firstname": "Maria",
                        "hs_object_id": "1",
                        "lastmodifieddate": "2023-02-21T09:50:53.651Z",
                        "lastname": "Johnson (Sample Contact)",
                    },
                    "properties_with_history": None,
                    "updated_at": datetime(2023, 2, 21, 9, 50, 53, 651000, tzinfo=timezone.utc),
                },
                {
                    "archived": False,
                    "archived_at": None,
                    "associations": None,
                    "created_at": datetime(2023, 2, 21, 9, 50, 48, 490000, tzinfo=timezone.utc),
                    "id": "51",
                    "createdate": "2023-02-21T09:50:48.490Z",
                    "email": "bh@hubspot.com",
                    "firstname": "Brian",
                    "hs_object_id": "51",
                    "lastmodifieddate": "2023-02-21T09:50:55.765Z",
                    "lastname": "Halligan (Sample Contact)",
                    "properties_with_history": None,
                    "updated_at": datetime(2023, 2, 21, 9, 50, 55, 765000, tzinfo=timezone.utc),
                },
            ],
        }
    )


@pytest.fixture
def hubspot_first_page(hubspot_full_page: _DictableDict) -> _DictableDict:
    return _DictableDict(
        **{
            "paging": {
                "next": {
                    "after": "1",
                    "link": "https://api.hubapi.com/crm/v3/objects/contacts?limit=1&after=2",
                }
            },
            "results": hubspot_full_page["results"][:1],
        }
    )


@pytest.fixture
def hubspot_second_page(hubspot_full_page: _DictableDict) -> _DictableDict:
    return _DictableDict(paging=None, results=hubspot_full_page["results"][1:])


@pytest.fixture
def hubspot_all_results(hubspot_full_page: _DictableDict) -> list[_DictableDict]:
    return [_DictableDict(r) for r in hubspot_full_page["results"]]


@pytest.fixture
def hubspot_client(mocker: MockFixture) -> MagicMock:
    basic_api = MagicMock()
    api = MagicMock()
    api.basic_api = basic_api

    mocker.patch.object(HubspotConnector, "_api_for_dataset", return_value=api)
    mocker.patch(
        "toucan_connectors.hubspot_private_app.hubspot_connector._page_api_for",
        return_value=basic_api,
    )

    return api


@pytest.fixture
def expected_df() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "created_at": [
                pd.Timestamp("2023-02-21 09:50:48.148000+0000", tz="UTC"),
                pd.Timestamp("2023-02-21 09:50:48.490000+0000", tz="UTC"),
            ],
            "updated_at": [
                pd.Timestamp("2023-02-21 09:50:53.651000+0000", tz="UTC"),
                pd.Timestamp("2023-02-21 09:50:55.765000+0000", tz="UTC"),
            ],
            "id": ["1", "51"],
            "properties_with_history": [None, None],
            "archived": [False, False],
            "archived_at": [None, None],
            "associations": [None, None],
            "createdate": ["2023-02-21T09:50:48.148Z", "2023-02-21T09:50:48.490Z"],
            "email": ["emailmaria@hubspot.com", "bh@hubspot.com"],
            "firstname": ["Maria", "Brian"],
            "hs_object_id": ["1", "51"],
            "lastmodifieddate": ["2023-02-21T09:50:53.651Z", "2023-02-21T09:50:55.765Z"],
            "lastname": ["Johnson (Sample Contact)", "Halligan (Sample Contact)"],
        }
    )
    return df.reindex(columns=sorted(df.columns.to_list()))


@pytest.fixture
def hubspot_connector() -> HubspotConnector:
    return HubspotConnector(name="hubspot", access_token="s3cr3t")


@pytest.fixture
def hubspot_data_source() -> HubspotDataSource:
    return HubspotDataSource(name="hubspot", domain="coucou", dataset=HubspotDataset.contacts)


def test_get_df(
    hubspot_connector: HubspotConnector,
    hubspot_data_source: HubspotDataSource,
    hubspot_client: MagicMock,
    hubspot_all_results: list[_DictableDict],
    expected_df: pd.DataFrame,
):
    hubspot_client.get_all.return_value = hubspot_all_results

    assert_frame_equal(hubspot_connector.get_df(hubspot_data_source), expected_df)
    hubspot_client.get_all.assert_called_once()


def test_get_slice_no_limit_or_offset(
    hubspot_connector: HubspotConnector,
    hubspot_data_source: HubspotDataSource,
    hubspot_client: MagicMock,
    hubspot_all_results: list[_DictableDict],
    expected_df: pd.DataFrame,
):
    hubspot_client.get_all.return_value = hubspot_all_results

    result = hubspot_connector.get_slice(hubspot_data_source)
    assert_frame_equal(result.df, expected_df)

    # Since no limit was specified, we should have called get_all
    hubspot_client.basic_api.get_page.assert_not_called()
    hubspot_client.get_all.assert_called_once()


def test_get_slice_with_offset_and_limit_greater_than_results(
    hubspot_connector: HubspotConnector,
    hubspot_data_source: HubspotDataSource,
    hubspot_client: MagicMock,
    hubspot_full_page: _DictableDict,
    expected_df: pd.DataFrame,
):
    hubspot_client.basic_api.get_page.return_value = hubspot_full_page

    result = hubspot_connector.get_slice(hubspot_data_source, offset=1, limit=2)
    assert_frame_equal(result.df, expected_df[1:].reset_index(drop=True))

    hubspot_client.basic_api.get_page.assert_called_once()
    hubspot_client.get_all.assert_not_called()


def test_get_slice_with_offset_only(
    hubspot_connector: HubspotConnector,
    hubspot_data_source: HubspotDataSource,
    hubspot_client: MagicMock,
    hubspot_all_results: list[_DictableDict],
    expected_df: pd.DataFrame,
):
    hubspot_client.get_all.return_value = hubspot_all_results

    result = hubspot_connector.get_slice(hubspot_data_source, offset=1)
    assert_frame_equal(result.df, expected_df[1:].reset_index(drop=True))

    hubspot_client.basic_api.get_page.assert_not_called()
    hubspot_client.get_all.assert_called_once()


def test_get_slice_with_offset_and_limit_in_bounds(
    hubspot_connector: HubspotConnector,
    hubspot_data_source: HubspotDataSource,
    hubspot_client: MagicMock,
    hubspot_first_page: _DictableDict,
    hubspot_second_page: _DictableDict,
    expected_df: pd.DataFrame,
):
    hubspot_client.basic_api.get_page.side_effect = [hubspot_first_page, hubspot_second_page]

    result = hubspot_connector.get_slice(hubspot_data_source, offset=1, limit=1)
    assert_frame_equal(result.df, expected_df[1:].reset_index(drop=True))

    assert hubspot_client.basic_api.get_page.call_args_list == [
        call(after=None, limit=1),
        call(after="1", limit=1),
    ]
    hubspot_client.get_all.assert_not_called()


def test_get_slice_with_offset_out_of_bounds(
    hubspot_connector: HubspotConnector,
    hubspot_data_source: HubspotDataSource,
    hubspot_client: MagicMock,
    hubspot_second_page: _DictableDict,
):
    hubspot_client.basic_api.get_page.side_effect = [hubspot_second_page]

    result = hubspot_connector.get_slice(hubspot_data_source, offset=140, limit=1000)
    assert_frame_equal(result.df, pd.DataFrame())

    hubspot_client.get_all.assert_not_called()
    # Limit should have been truncated to 100 results
    assert hubspot_client.basic_api.get_page.call_args_list == [call(after=None, limit=100)]


def test_get_slice_has_the_right_behaviour_even_when_too_many_results_are_returned(
    hubspot_connector: HubspotConnector,
    hubspot_data_source: HubspotDataSource,
    hubspot_client: MagicMock,
    hubspot_full_page: _DictableDict,
    expected_df: pd.DataFrame,
):
    hubspot_client.basic_api.get_page.return_value = hubspot_full_page

    result = hubspot_connector.get_slice(hubspot_data_source, offset=0, limit=1)
    assert_frame_equal(result.df, expected_df[:1])

    hubspot_client.get_all.assert_not_called()
    # Limit should have been truncated to 100 results
    assert hubspot_client.basic_api.get_page.call_args_list == [call(after=None, limit=1)]
