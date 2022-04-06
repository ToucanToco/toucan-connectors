import json

import pandas as pd
import requests

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class AnaplanDataSource(ToucanDataSource):
    query: str


class AnaplanError(Exception):
    """Base exception for Anaplan connector errors"""


class AnaplanAuthError(AnaplanError):
    """Exception raised when auth fails"""


# refactor to fields when required
_ANAPLAN_AUTH_ROUTE = "https://auth.anaplan.com/token/authenticate"
_ANAPLAN_API_BASEROUTE = "https://anaplan.com/"


class AnaplanConnector(ToucanConnector):
    data_source_model: AnaplanDataSource

    username: str
    password: str

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
