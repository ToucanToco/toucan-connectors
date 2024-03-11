from contextlib import suppress
from datetime import datetime
from typing import Any, Callable, List, Optional

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import Error as GoogleApiClientError
from pydantic import Field, PrivateAttr, create_model
from pydantic.json_schema import DEFAULT_REF_TEMPLATE, GenerateJsonSchema, JsonSchemaMode

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.toucan_connector import (
    PlainJsonSecretStr,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)


class GoogleSheetsDataSource(ToucanDataSource):
    domain: str = Field(
        ...,
        title="dataset",
    )
    spreadsheet_id: str = Field(
        ...,
        title="ID of the spreadsheet",
        description="Can be found in your URL: " "https://docs.google.com/spreadsheets/d/<ID of the spreadsheet>/...",
    )
    sheet: Optional[str] = Field(None, title="Sheet title", description="Title of the desired sheet")
    header_row: int = Field(0, title="Header row", description="Row of the header of the spreadsheet")
    dates_as_float: bool = Field(
        True, title="Dates as floats", description="Render Date as Floats or String from the sheet"
    )

    @classmethod
    def model_json_schema(
        cls,
        by_alias: bool = True,
        ref_template: str = DEFAULT_REF_TEMPLATE,
        schema_generator: type[GenerateJsonSchema] = GenerateJsonSchema,
        mode: JsonSchemaMode = "validation",
    ) -> dict[str, Any]:
        schema = super().model_json_schema(
            by_alias=by_alias,
            ref_template=ref_template,
            schema_generator=schema_generator,
            mode=mode,
        )
        keys = schema["properties"].keys()
        prio_keys = ["domain", "spreadsheet_id", "sheet"]
        new_keys = prio_keys + [k for k in keys if k not in prio_keys]
        schema["properties"] = {k: schema["properties"][k] for k in new_keys}
        return schema

    @classmethod
    def get_form(cls, connector: "GoogleSheetsConnector", current_config, **kwargs):
        """Retrieve a form filled with suggestions of available sheets."""
        # Always add the suggestions for the available sheets
        constraints = {}
        with suppress(Exception):
            available_sheets = connector.list_sheets(current_config["spreadsheet_id"])
            constraints["sheet"] = strlist_to_enum("sheet", available_sheets)

        return create_model("FormSchema", **constraints, __base__=cls).schema()


class GoogleSheetsConnector(ToucanConnector, data_source_model=GoogleSheetsDataSource):
    """
    This is a connector for [GoogleSheets](https://developers.google.com/sheets/api/reference/rest)

    It needs to be provided a retrieve_token method which should provide a valid OAuth2 access token.
    Not to be confused with the OAuth2 connector, which handles all the OAuth2 process byt itself!
    """

    _auth_flow = "managed_oauth2"
    _managed_oauth_service_id = "google-sheets"
    _oauth_trigger = "retrieve_token"
    _retrieve_token: Callable[[str, str], str] = PrivateAttr()

    auth_id: PlainJsonSecretStr = None

    def __init__(self, retrieve_token: Callable[[str, str], str], *args, **kwargs):
        super().__init__(**kwargs)
        self._retrieve_token = retrieve_token  # Could be async

    def _google_client_build_kwargs(self):  # pragma: no cover
        # Override it for testing purposes
        access_token = self._retrieve_token(self._managed_oauth_service_id, self.auth_id.get_secret_value())
        return {"credentials": Credentials(token=access_token)}

    def _google_client_request_kwargs(self):  # pragma: no cover
        # Override it for testing purposes
        return {}

    def build_sheets_api(self):
        return build("sheets", "v4", cache_discovery=False, **self._google_client_build_kwargs())

    def build_oauth2(self):
        return build("oauth2", "v2", cache_discovery=False, **self._google_client_build_kwargs())

    def list_sheets(self, spreadsheet_id: str) -> List[str]:
        """
        List available sheets
        """
        with self.build_sheets_api() as sheets_api:
            spreadsheet_data = (
                sheets_api.spreadsheets()
                .get(
                    spreadsheetId=spreadsheet_id,
                    fields="sheets.properties.title,sheets.properties.sheetType",
                )
                .execute(**self._google_client_request_kwargs())
            )

        return [
            sheet["properties"]["title"]
            for sheet in spreadsheet_data["sheets"]
            if sheet["properties"]["sheetType"] == "GRID"
        ]

    def get_status(self) -> ConnectorStatus:
        """
        Test the Google Sheets connexion.

        If successful, returns a message with the email of the connected user account.
        """
        try:
            access_token = self._retrieve_token(self._managed_oauth_service_id, self.auth_id.get_secret_value())
        except Exception:
            return ConnectorStatus(status=False, error="Credentials are missing")

        if not access_token:
            return ConnectorStatus(status=False, error="Credentials are missing")

        try:
            with self.build_oauth2() as oauth2_api:
                user_info = oauth2_api.userinfo().get().execute(**self._google_client_request_kwargs())
                return ConnectorStatus(status=True, message=f"Connected as {user_info.get('email')}")
        except GoogleApiClientError:
            return ConnectorStatus(status=False, error="Couldn't retrieve user infos")

    def _render_date_time_option_from_sheet(self, data_source: GoogleSheetsDataSource) -> str:
        """
        Following the documentation, to prevent loading dates as double :
        https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption
        We use FORMATTED_STRING to load as simple strings
        """
        return "SERIAL_NUMBER" if data_source.dates_as_float else "FORMATTED_STRING"

    def _retrieve_data(self, data_source: GoogleSheetsDataSource) -> pd.DataFrame:
        if data_source.sheet is None:
            # Select the first sheet by default
            sheet_names = self.list_sheets(data_source.spreadsheet_id)
            data_source.sheet = sheet_names[0]

        with self.build_sheets_api() as sheets_api:
            sheet_values = (
                sheets_api.spreadsheets()
                .values()
                .get(
                    spreadsheetId=data_source.spreadsheet_id,
                    range=f"'{data_source.sheet}'",  # FIXME what will happen is the sheet name contains a single quote?
                    dateTimeRenderOption=self._render_date_time_option_from_sheet(data_source=data_source),
                    majorDimension="ROWS",
                    valueRenderOption="UNFORMATTED_VALUE",
                )
                .execute(**self._google_client_request_kwargs())
            )
            # Fetch metadata associated with values
            sheet_cell_formats = (
                sheets_api.spreadsheets()
                .get(
                    spreadsheetId=data_source.spreadsheet_id,
                    fields="sheets.data.rowData.values.effectiveFormat.numberFormat",
                    ranges=[sheet_values["range"]],
                )
                .execute(**self._google_client_request_kwargs())
            )

        def cell_format(row_index: int, column_index: int):
            try:
                return sheet_cell_formats["sheets"][0]["data"][0]["rowData"][row_index]["values"][column_index][
                    "effectiveFormat"
                ]
            except (KeyError, IndexError):
                return None

        values = [
            [
                parse_cell_value(cell_value, cell_format(row_index, column_index))
                for column_index, cell_value in enumerate(row_values)
            ]
            for row_index, row_values in enumerate(sheet_values["values"])
        ]

        df = pd.DataFrame(columns=values[data_source.header_row], data=values[data_source.header_row + 1 :])

        # TODO Columns must be uniquely named (raise an error or suffix some of them) - otherwise, .to_json will fail
        return df


SERIAL_REFERENCE_DAY = datetime.fromisoformat("1899-12-30")


def serial_number_to_date(serial_number: float) -> datetime:
    """
    https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption
    """
    # TODO implement the time part
    return SERIAL_REFERENCE_DAY + relativedelta(days=int(serial_number))


def parse_cell_value(value: Any, format_: dict[str, Any] | None = None) -> Any:
    """
    Use the format (if provided) to parse the value in its intended type
    """
    if (
        isinstance(value, (int, float))
        and format_ is not None
        and format_.get("numberFormat", {}).get("type") == "DATE"
    ):
        return serial_number_to_date(value)
    elif isinstance(value, str) and value == "":
        return np.nan

    return value


class GoogleSheetException(Exception): ...


class InvalidSheetException(GoogleSheetException): ...


class EmptySheetException(GoogleSheetException): ...
