from contextlib import suppress
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ContextManager,
    Dict,
    List,
    Literal,
    Optional,
    Type,
    TypedDict,
    Union,
)

import pandas as pd
from dateutil.relativedelta import relativedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import Error as GoogleApiClientError
from pydantic import Field, PrivateAttr, SecretStr, constr, create_model, root_validator

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.toucan_connector import (
    DataSlice,
    DataStats,
    ToucanConnector,
    ToucanDataSource,
    strlist_to_enum,
)

if TYPE_CHECKING:
    # Resources
    Dimension = Literal['ROWS', 'COLUMNS']
    ValueRenderOption = Literal['FORMATTED_VALUE', 'UNFORMATTED_VALUE', 'FORMULA']
    DateTimeRenderOption = Literal['SERIAL_NUMBER', 'FORMATTED_STRING']
    NumberFormatType = Literal[
        'NUMBER_FORMAT_TYPE_UNSPECIFIED',
        'TEXT',
        'NUMBER',
        'PERCENT',
        'CURRENCY',
        'DATE',
        'TIME',
        'DATE_TIME',
        'SCIENTIFIC',
    ]

    class GridProperties(TypedDict):
        rowCount: int
        columnCount: int
        rowGroupControlAfter: bool
        columnGroupControlAfter: bool

    class NumberFormat(TypedDict):
        type: NumberFormatType
        pattern: str

    class CellFormat(TypedDict):
        numberFormat: NumberFormat

    class CellData(TypedDict):
        effectiveFormat: CellFormat

    class RowData(TypedDict):
        values: List[CellData]

    class GridData(TypedDict):
        startRow: int
        startColumn: int
        rowData: List[RowData]

    SheetType = Literal['GRID', 'OBJECT', 'DATA_SOURCES']

    class SheetProperties(TypedDict):
        title: str
        sheetType: SheetType
        gridProperties: GridProperties

    class Sheet(TypedDict):
        properties: SheetProperties
        data: List[GridData]

    class Spreadsheet(TypedDict):
        sheets: List[Sheet]

    CellValue = Any

    class ValueRange(TypedDict):
        range: str
        majorDimension: Dimension
        values: List[List[CellValue]]

    # Client
    from typing import Generic, TypeVar

    T = TypeVar('T')

    class ExecuteContext(Generic[T]):
        def execute(**kwargs: Any) -> T:
            ...

    class GoogleSheetsClient:
        class Spreadsheets:
            def get(
                *,
                spreadsheetId: str,
                ranges: Optional[List[str]] = None,
                fields: Optional[str] = None,
            ) -> ExecuteContext[Spreadsheet]:
                """https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/get"""

            class SpreadsheetsValues:
                def get(
                    *,
                    spreadsheetId: str,
                    range: str,
                    majorDimension: Optional[Dimension] = None,
                    valueRenderOption: Optional[ValueRenderOption] = None,
                    dateTimeRenderOption: Optional[DateTimeRenderOption] = None,
                    fields: Optional[str] = None,
                ) -> ExecuteContext[ValueRange]:
                    """https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get"""

            def values() -> SpreadsheetsValues:
                ...

        def spreadsheets() -> Spreadsheets:
            ...


class GoogleSheetsDataSource(ToucanDataSource):
    domain: str = Field(
        ...,
        title='dataset',
    )
    spreadsheet_id: str = Field(
        ...,
        title='ID of the spreadsheet',
        description='Can be found in your URL: '
        'https://docs.google.com/spreadsheets/d/<ID of the spreadsheet>/...',
    )
    sheet: Optional[str] = Field(
        None, title='Sheet title', description='Title of the desired sheet'
    )
    header_range: constr(regex=r'^[A-Z]*\d+:[A-Z]*\d+$') = Field(  # noqa: F722
        '1:1',
        title='Header range',
        description='Range of the header of the spreadsheet (e.g. B1:E1)',
    )

    @root_validator(pre=True)
    def handle_legacy_header_row(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if 'header_row' in values:
            # we used to have 'header_row' as parameter with index 0 by default
            header_row = values.pop('header_row')
            values['header_range'] = f'{header_row + 1}:{header_row + 1}'

        return values

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type['GoogleSheetsDataSource']) -> None:
            keys = schema['properties'].keys()
            prio_keys = ['domain', 'spreadsheet_id', 'sheet']
            new_keys = prio_keys + [k for k in keys if k not in prio_keys]
            schema['properties'] = {k: schema['properties'][k] for k in new_keys}

    @classmethod
    def get_form(cls, connector: 'GoogleSheetsConnector', current_config, **kwargs):
        """Retrieve a form filled with suggestions of available sheets."""
        # Always add the suggestions for the available sheets
        constraints = {}
        with suppress(Exception):
            available_sheets = connector.list_sheets(current_config['spreadsheet_id'])
            constraints['sheet'] = strlist_to_enum('sheet', available_sheets)

        return create_model('FormSchema', **constraints, __base__=cls).schema()


class GoogleSheetsConnector(ToucanConnector):
    """
    This is a connector for [GoogleSheets](https://developers.google.com/sheets/api/reference/rest)

    It needs to be provided a retrieve_token method which should provide a valid OAuth2 access token.
    Not to be confused with the OAuth2 connector, which handles all the OAuth2 process byt itself!
    """

    data_source_model: GoogleSheetsDataSource

    _auth_flow = 'managed_oauth2'
    _managed_oauth_service_id = 'google-sheets'
    _oauth_trigger = 'retrieve_token'
    _retrieve_token: Callable[[str, str], str] = PrivateAttr()

    auth_id: SecretStr = None

    def __init__(self, retrieve_token: Callable[[str, str], str], *args, **kwargs):
        super().__init__(**kwargs)
        self._retrieve_token = retrieve_token  # Could be async

    def _google_client_build_kwargs(self):  # pragma: no cover
        # Override it for testing purposes
        access_token = self._retrieve_token(
            self._managed_oauth_service_id, self.auth_id.get_secret_value()
        )
        return {'credentials': Credentials(token=access_token)}

    def _google_client_request_kwargs(self):  # pragma: no cover
        # Override it for testing purposes
        return {}

    def build_google_sheets_client(self) -> ContextManager['GoogleSheetsClient']:
        return build('sheets', 'v4', cache_discovery=False, **self._google_client_build_kwargs())

    def build_oauth2(self):
        return build('oauth2', 'v2', cache_discovery=False, **self._google_client_build_kwargs())

    def list_sheets(self, spreadsheet_id: str) -> List[str]:
        """
        List available sheets
        """
        with self.build_google_sheets_client() as client:
            spreadsheet_data = (
                client.spreadsheets()
                .get(
                    spreadsheetId=spreadsheet_id,
                    fields=','.join(
                        (
                            'sheets.properties.title',
                            'sheets.properties.sheetType',
                        )
                    ),
                )
                .execute(**self._google_client_request_kwargs())
            )

        return [
            sheet['properties']['title']
            for sheet in spreadsheet_data['sheets']
            if sheet['properties']['sheetType'] == 'GRID'
        ]

    def get_status(self) -> ConnectorStatus:
        """
        Test the Google Sheets connexion.

        If successful, returns a message with the email of the connected user account.
        """
        try:
            access_token = self._retrieve_token(
                self._managed_oauth_service_id, self.auth_id.get_secret_value()
            )
        except Exception:
            return ConnectorStatus(status=False, error='Credentials are missing')

        if not access_token:
            return ConnectorStatus(status=False, error='Credentials are missing')

        try:
            with self.build_oauth2() as oauth2_api:
                user_info = (
                    oauth2_api.userinfo().get().execute(**self._google_client_request_kwargs())
                )
                return ConnectorStatus(
                    status=True, message=f"Connected as {user_info.get('email')}"
                )
        except GoogleApiClientError:
            return ConnectorStatus(status=False, error="Couldn't retrieve user infos")

    def get_slice(
        self,
        data_source: GoogleSheetsDataSource,
        permissions: Optional[str] = None,
        offset: int = 0,
        limit: Optional[int] = None,
        get_row_count: Optional[bool] = False,
    ) -> DataSlice:
        if data_source.sheet is None:
            # Select the first sheet by default
            sheet_names = self.list_sheets(data_source.spreadsheet_id)
            data_source.sheet = sheet_names[0]

        with self.build_google_sheets_client() as client:
            spreadsheet_metadata = (
                client.spreadsheets()
                .get(
                    spreadsheetId=data_source.spreadsheet_id,
                    # use `repr()` to add single quotes around the name and escape correctly
                    ranges=[repr(data_source.sheet)],
                    fields=','.join(
                        (
                            'sheets.properties.gridProperties',
                            'sheets.data.rowData.values.effectiveFormat.numberFormat',
                        )
                    ),
                )
                .execute(**self._google_client_request_kwargs())
            )
            total_rows = spreadsheet_metadata['sheets'][0]['properties']['gridProperties'][
                'rowCount'
            ]

            header_data = (
                client.spreadsheets()
                .values()
                .get(
                    spreadsheetId=data_source.spreadsheet_id,
                    range=f'{repr(data_source.sheet)}!{data_source.header_range}',
                    dateTimeRenderOption='SERIAL_NUMBER',
                    majorDimension='ROWS',
                    valueRenderOption='UNFORMATTED_VALUE',
                )
                .execute(**self._google_client_request_kwargs())
            )

            try:
                import re

                range_regex = r'([A-Z]*)(\d+):([A-Z]*)(\d+)'
                header_col_1, header_row_1_str, header_col_2, header_row_2_str = re.search(
                    range_regex, data_source.header_range
                ).groups()
                assert header_row_1_str == header_row_2_str
                header_row = int(header_row_1_str)
            except (AssertionError, TypeError):
                raise Exception('header range is not valid')

            start_index = header_row + 1 + offset

            if limit is None:
                end_index = total_rows
            else:
                end_index = start_index + limit
                if end_index > total_rows:
                    end_index = total_rows

            slice_data = (
                client.spreadsheets()
                .values()
                .get(
                    spreadsheetId=data_source.spreadsheet_id,
                    range=f'{repr(data_source.sheet)}!{header_col_1}{start_index}:{header_col_2}{end_index}',
                    dateTimeRenderOption='SERIAL_NUMBER',
                    majorDimension='ROWS',
                    valueRenderOption='UNFORMATTED_VALUE',
                )
                .execute(**self._google_client_request_kwargs())
            )

        try:
            slice_values = slice_data['values']
        except KeyError:
            slice_values = []

        try:
            header_cols = header_data['values'][0]
        except KeyError:
            header_cols = []

        # header can have more columns than the slice if columns are empty
        len_header_cols = len(header_cols)
        for slice_row in slice_values:
            slice_row.extend([None] * (len_header_cols - len(slice_row)))

        # we only modify all the retrieved data if we have some formatting metadata on gsheet
        try:
            row_data = spreadsheet_metadata['sheets'][0]['data'][0]['rowData']
        except (KeyError, IndexError):
            pass
        else:
            for row_idx, row in enumerate(row_data):
                try:
                    row_cells = row['values']
                except KeyError:
                    continue
                else:
                    for col_idx, cell_data in enumerate(row_cells):
                        try:
                            cell_format = cell_data['effectiveFormat']
                        except KeyError:
                            continue
                        else:
                            try:
                                slice_values[row_idx][col_idx] = format_cell_value(
                                    slice_values[row_idx][col_idx], cell_format
                                )
                            except IndexError:
                                continue

        df = pd.DataFrame(columns=header_cols, data=slice_values)

        return DataSlice(
            df=df,
            stats=DataStats(
                total_returned_rows=len(df),
                total_rows=total_rows,
                df_memory_size=df.memory_usage().sum(),
            ),
        )

    def _retrieve_data(self, data_source: GoogleSheetsDataSource) -> pd.DataFrame:
        return self.get_slice(data_source).df


SERIAL_REFERENCE_DAY = datetime.fromisoformat('1899-12-30')


def serial_number_to_date(serial_number: Union[int, float]) -> datetime:
    """
    https://developers.google.com/sheets/api/reference/rest/v4/DateTimeRenderOption
    """
    # TODO implement the time part
    return SERIAL_REFERENCE_DAY + relativedelta(days=int(serial_number))


def format_cell_value(value: Any, format: Optional['CellFormat']) -> Any:
    """
    Use the format (if provided) to parse the value in its intended type
    """
    with suppress(KeyError):
        number_format_type: 'NumberFormatType' = format['numberFormat']['type']
        if number_format_type == 'DATE' and isinstance(value, (int, float)):
            return serial_number_to_date(value)

    return value


class GoogleSheetException(Exception):
    ...


class InvalidSheetException(GoogleSheetException):
    ...


class EmptySheetException(GoogleSheetException):
    ...
