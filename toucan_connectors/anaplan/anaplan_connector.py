import contextlib
import json
from typing import Any, Dict, List

import pandas as pd
import requests
from pydantic import Field, constr, create_model

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource, strlist_to_enum


class AnaplanDataSource(ToucanDataSource):
    model_id: constr(min_length=1) = Field(..., description="The model you want to query")
    view_id: constr(min_length=1) = Field(..., description="The view you want to query")

    @classmethod
    def get_form(
        cls,
        connector: 'AnaplanConnector',
        current_config: Dict[str, str],
    ) -> Dict[str, Any]:
        """
        Retrieve a form with suggestions of available Models and Views.

        Once the connector is configured, we can give suggestions for the `model` field.
        If `model` is set, we can give suggestions for the `view` field.
        """

        constraints = {}
        with contextlib.suppress(Exception):  # should we catch AnaplanErrors here instead ?
            available_models = connector.get_available_models()
            # TODO: Make it possible to pick models and views by name at some point
            constraints['model_id'] = strlist_to_enum(
                'model_id', [m['id'] for m in available_models]
            )

            if 'model_id' in current_config:
                available_views = connector.get_available_views(current_config['model_id'])
                constraints['view_id'] = strlist_to_enum(
                    'view_id', [v['id'] for v in available_views], default_value=None
                )

        return create_model('FormSchema', **constraints, __base__=cls).schema()


class AnaplanError(Exception):
    """Base exception for Anaplan connector errors"""


class AnaplanAuthError(AnaplanError):
    """Exception raised when auth fails"""


# refactor to fields when required
_ANAPLAN_AUTH_ROUTE = "https://auth.anaplan.com/token/authenticate"
_ANAPLAN_API_BASEROUTE = "https://api.anaplan.com/2/0"


class AnaplanConnector(ToucanConnector):
    data_source_model: AnaplanDataSource
    username: str
    password: str

    workspace_id: str = Field(..., description="The ID of the workspace you want to query")

    def _retrieve_data(self, data_source: AnaplanDataSource) -> pd.DataFrame:
        raise NotImplementedError

    def _fetch_token(self) -> str:
        try:
            # FIXME: use a session
            resp = requests.post(_ANAPLAN_AUTH_ROUTE, auth=(self.username, self.password))
            if resp.status_code in (401, 403):
                raise AnaplanAuthError(
                    f"Invalid credentials for {self.username}: got HTTP status {resp.status_code}"
                )
            body = resp.json()
            return body["tokenInfo"]["tokenValue"]
        except requests.RequestException as exc:  # pragma: no cover
            raise AnaplanAuthError(f"Encountered error while fetching token: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise AnaplanAuthError(f"could not parse response body as json: {resp.text}") from exc
        except KeyError as key:
            raise AnaplanAuthError(f"did not find expected key {key} in response body:  {body}")

    def get_status(self) -> ConnectorStatus:
        try:
            self._fetch_token()
            return ConnectorStatus(status=True, message=f"connected as {self.username}")
        except AnaplanAuthError as exc:
            return ConnectorStatus(status=False, error=f"could not retrieve token: {exc}")

    def get_available_models(self) -> List[Dict[str, str]]:
        token = self._fetch_token()
        resp = requests.get(
            f"{_ANAPLAN_API_BASEROUTE}/models",
            headers={"Accept": "application/json", "Authorization": f"AnaplanAuthToken {token}"},
        )
        # TODO: Add same checks as in _fetch_token()
        body = resp.json()
        return body['models']

    def get_available_views(self, model_id: str) -> List[Dict[str, str]]:
        token = self._fetch_token()
        resp = requests.get(
            f"{_ANAPLAN_API_BASEROUTE}/models/{model_id}/views",
            headers={"Accept": "application/json", "Authorization": f"AnaplanAuthToken {token}"},
        )
        # TODO: Add same checks as in _fetch_token()
        body = resp.json()
        return body['views']
