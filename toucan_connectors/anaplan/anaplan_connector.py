import contextlib
from typing import Any, Dict, List, Optional

import pandas as pd
import pydantic
import requests
from pydantic import Field, constr, create_model
from pydantic.types import SecretStr

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource, strlist_to_enum

_ID_SEPARATOR = ' - '


def _sanitize_id(id_: str) -> str:
    return id_.split(_ID_SEPARATOR)[0]


def _format_name_and_id(obj: Dict[str, str]) -> str:
    return f"{obj['id']}{_ID_SEPARATOR}{obj['name']}"


class AnaplanDataSource(ToucanDataSource):
    model_id: constr(min_length=1) = Field(..., description='The model you want to query')
    view_id: constr(min_length=1) = Field(..., description='The view you want to query')
    workspace_id: str = Field(..., description='The ID of the workspace you want to query')

    @pydantic.validator('model_id', 'view_id', 'workspace_id')
    def _sanitize_id(cls, value: str) -> str:
        return _sanitize_id(value)

    @classmethod
    def get_form(
        cls,
        connector: 'AnaplanConnector',
        current_config: Dict[str, str],
    ) -> Dict[str, Any]:
        """
        Retrieves a form with suggestions of available Models and Views.

        Once the connector is configured, we can give suggestions for the `model` field.
        If `model` is set, we can give suggestions for the `view` field.
        """

        constraints = {}
        with contextlib.suppress(AnaplanError, KeyError):

            token = connector.fetch_token()
            available_workspaces = connector.get_available_workspaces(token=token)
            constraints['workspace_id'] = strlist_to_enum(
                'workspace_id', [_format_name_and_id(w) for w in available_workspaces]
            )

            if 'workspace_id' in current_config:
                workspace_id = _sanitize_id(current_config['workspace_id'])
                available_models = connector.get_available_models(workspace_id, token=token)
                constraints['model_id'] = strlist_to_enum(
                    'model_id',
                    [_format_name_and_id(m) for m in available_models],
                )

                if 'model_id' in current_config:
                    available_views = connector.get_available_views(
                        workspace_id, _sanitize_id(current_config['model_id']), token=token
                    )
                    constraints['view_id'] = strlist_to_enum(
                        'view_id', [_format_name_and_id(v) for v in available_views]
                    )

        return create_model('FormSchema', **constraints, __base__=cls).schema()


class AnaplanError(Exception):
    """Base exception for Anaplan connector errors"""


class AnaplanUnexpectedStatus(AnaplanError):
    """Exception raised when an unexpected HTTP status is returned by Anaplan"""


class AnaplanAuthError(AnaplanError):
    """Exception raised when auth fails"""


# refactor to fields when required
_ANAPLAN_AUTH_ROUTE = 'https://auth.anaplan.com/token/authenticate'
_ANAPLAN_API_BASEROUTE = 'https://api.anaplan.com/2/0'


class AnaplanConnector(ToucanConnector):
    data_source_model: AnaplanDataSource
    username: str
    password: Optional[SecretStr]

    def _extract_json(self, resp: requests.Response) -> dict:
        if resp.status_code in (401, 403):
            raise AnaplanAuthError(
                f'Invalid credentials for {self.username}: got HTTP status {resp.status_code}'
            )
        try:
            return resp.json()
        except requests.JSONDecodeError as exc:
            raise AnaplanError(
                f'Anaplan response appears to be invalid JSON: {resp.content} {exc!r}'
            ) from exc
        except requests.RequestException as exc:  # pragma: no cover
            raise AnaplanError(f'Encountered error while executing request: {exc}') from exc

    def _http_get(self, url: str, **kwargs) -> requests.Response:
        token = kwargs.pop('token', None) or self.fetch_token()
        headers = {
            **kwargs.pop('headers', {}),
            'Accept': 'application/json',
            'Authorization': f'AnaplanAuthToken {token}',
        }
        return requests.get(url, headers=headers, **kwargs)

    def _retrieve_data(self, data_source: AnaplanDataSource) -> pd.DataFrame:
        data = self._extract_json(
            self._http_get(
                f'{_ANAPLAN_API_BASEROUTE}/models/{data_source.model_id}/views/{data_source.view_id}/data?format=v1'
            )
        )

        try:
            # Columns can have several levels, we flatten them with the "/" separator
            df_columns = ['index'] + ['/'.join(col) for col in data['columnCoordinates']]
            # No MultiIndex for now
            data = (
                ['/'.join(row['rowCoordinates'])] + row['cells'] for row in data.get('rows', [])
            )
            return pd.DataFrame(columns=df_columns, data=data)
        except KeyError as exc:
            raise AnaplanError(f'Did not find expected key {exc} in response body')

    def fetch_token(self) -> str:
        try:
            # FIXME: use a session
            body = self._extract_json(
                requests.post(
                    _ANAPLAN_AUTH_ROUTE,
                    auth=(self.username, self.password.get_secret_value() if self.password else ''),
                )
            )
        except AnaplanError as exc:
            raise AnaplanAuthError(f'encountered error while retrieving auth token: {exc}') from exc
        try:
            return body['tokenInfo']['tokenValue']
        except KeyError as key:
            raise AnaplanAuthError(f'did not find expected key {key} in response body: {body}')

    def get_status(self) -> ConnectorStatus:
        try:
            self.fetch_token()
            return ConnectorStatus(status=True, message=f'connected as {self.username}')
        except AnaplanAuthError as exc:
            return ConnectorStatus(status=False, error=f'could not retrieve token: {exc}')

    def get_available_workspaces(self, token: str) -> List[Dict[str, str]]:
        body = self._extract_json(
            self._http_get(f'{_ANAPLAN_API_BASEROUTE}/workspaces', token=token)
        )
        return body.get('workspaces', [])

    def get_available_models(self, workspace_id: str, *, token: str) -> List[Dict[str, str]]:
        body = self._extract_json(
            self._http_get(
                f'{_ANAPLAN_API_BASEROUTE}/workspaces/{workspace_id}/models', token=token
            )
        )
        return body.get('models', [])

    def get_available_views(
        self, workspace_id: str, model_id: str, *, token: str
    ) -> List[Dict[str, str]]:
        body = self._extract_json(
            self._http_get(
                f'{_ANAPLAN_API_BASEROUTE}/workspaces/{workspace_id}/models/{model_id}/views',
                token=token,
            )
        )
        return body.get('views', [])
