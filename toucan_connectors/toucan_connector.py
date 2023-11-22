import json
import logging
import operator
import os
import re
import socket
import uuid
from abc import ABC, ABCMeta, abstractmethod
from enum import Enum
from functools import reduce, wraps
from typing import Any, Generic, Iterable, NamedTuple, Type, TypeVar, Union

import pandas as pd
import tenacity as tny
from pydantic import BaseModel, Field, SecretBytes, SecretStr

from toucan_connectors.common import (
    ConnectorStatus,
    apply_query_parameters,
    nosql_apply_parameters_to_query,
)
from toucan_connectors.json_wrapper import JsonWrapper
from toucan_connectors.pagination import PaginationInfo, build_pagination_info
from toucan_connectors.pandas_translator import PandasConditionTranslator
from toucan_connectors.utils.datetime import sanitize_df_dates

try:
    from bearer import Bearer  # type: ignore[import-untyped]
except ImportError:
    pass

LOGGER = logging.getLogger(__name__)


class DataStats(BaseModel):
    query_generation_time: float | None = None
    data_extraction_time: float | None = None
    data_conversion_time: float | None = None
    compute_stats_time: float | None = None
    conversion_time: float | None = None
    data_filtered_from_permission_time: float | None = None
    df_memory_size: int | None = None
    others: dict[str, Any] | None = None


class QueryMetadata(NamedTuple):
    columns: dict[str, str] | None = None  # Stores column names and types


class Category(str, Enum):
    SNOWFLAKE: str = 'Snowflake'


class DataSlice(NamedTuple):
    """Extracted data enriched with additional metadata.

    See the documentation (DataSlice.md) for details.
    """

    df: pd.DataFrame  # the dataframe of the slice
    pagination_info: PaginationInfo  # Information about pagination
    input_parameters: dict | None = None
    stats: DataStats | None = None
    # TODO: name is kinda misleading. what others information than `columns` will it contain ?
    query_metadata: QueryMetadata | None = None


class StrEnum(str, Enum):
    """Class to easily make schemas with enum values and type string"""


def strlist_to_enum(field: str, strlist: list[str], default_value=...) -> tuple[type[StrEnum], Any]:
    """
    Convert a list of strings to a pydantic schema enum
    the value is either <default value> or a tuple ( <type>, <default value> )
    If the field is required, the <default value> has to be '...' (cf pydantic doc)
    By default, the field is considered required.
    """
    return StrEnum(field, {v: v for v in strlist}), default_value  # type: ignore[call-overload]


# Binding C to ToucanConnector causes mypy to crash (last checked with mypy 0.991)
C = TypeVar('C')


class ToucanDataSource(BaseModel, Generic[C]):
    domain: str
    name: str
    type: str | None = None
    load: bool = True
    live_data: bool = False
    validation: dict | None = None
    parameters: dict | None = None
    cache_ttl: int | None = Field(
        None,
        title="Slow Queries' Cache Expiration Time",
        description='In seconds. Will override the 5min instance default and/or the connector value',
    )

    class Config:
        extra = 'forbid'
        validate_assignment = True

    @classmethod
    def get_form(cls, connector: C, current_config):
        """
        Method to retrieve the form with a current config
        Once the connector is set, we are often able to enforce some values depending
        on what the current `ToucanDataSource` config looks like

        By default, we simply return the model schema.
        """
        return cls.schema()


class RetryPolicy(BaseModel):
    """Generic "retry" policy management.

    This is just a declarative wrapper around `tenacity.retry` that should
    ease retry policy definition for most classic use cases.

    It can be instantiated with the following parameters:

    - `retry_on`: the list of expected exception classes that should trigger a retry
    - `max_attempts`: the maximum number of retries before giving up
    - `max_delay`: delay, in seconds, above which we should give up
    - `wait_time`: time, in seconds, between each retry.

    The class also exposes the `retry_decorator` method that is responsible to convert
    the parameters aforementioned in a corresponding `tenacity.retry` decorator. If
    you need a really custom retry policy and want to use the full power of the
    `tenacity` library, you can simply override this method and return your own
    `tenacity.retry` decorator.
    """

    # retry_on: Iterable[BaseException] = ()
    max_attempts: int | None = 1
    max_delay: float | None = 0.0
    wait_time: float | None = 0.0

    def __init__(self, retry_on=(), logger=None, **data):
        super().__init__(**data)
        self.__dict__['retry_on'] = retry_on
        self.__dict__['logger'] = logger

    @property
    def tny_stop(self):
        """generate corresponding `stop` parameter for `tenacity.retry`"""
        stoppers = []
        if self.max_attempts > 1:
            stoppers.append(tny.stop_after_attempt(self.max_attempts))
        if self.max_delay:
            stoppers.append(tny.stop_after_delay(self.max_delay))
        if stoppers:
            return reduce(operator.or_, stoppers)
        return None

    @property
    def tny_retry(self):
        """generate corresponding `retry` parameter for `tenacity.retry`"""
        if self.retry_on:
            return tny.retry_if_exception_type(self.retry_on)
        return None

    @property
    def tny_wait(self):
        """generate corresponding `wait` parameter for `tenacity.retry`"""
        if self.wait_time:
            return tny.wait_fixed(self.wait_time)
        return None

    @property
    def tny_after(self):
        """generate corresponding `after` parameter for `tenacity.retry`"""
        if self.logger:
            return tny.after_log(self.logger, logging.DEBUG)
        return None

    def retry_decorator(self):
        """build the `tenaticy.retry` decorator corresponding to policy"""
        tny_kwargs = {}
        for attr in dir(self):
            # the "after" hook is handled separately later to plug it only if
            # there is an actual retry policy
            if attr.startswith('tny_') and attr != 'tny_after':
                paramvalue = getattr(self, attr)
                if paramvalue is not None:
                    tny_kwargs[attr[4:]] = paramvalue
        if tny_kwargs:
            # plug the "after" hook if there's one
            if self.tny_after:
                tny_kwargs['after'] = self.tny_after
            return tny.retry(reraise=True, **tny_kwargs)
        return None

    def __call__(self, f):
        """make retry_policy instances behave as `tenacity.retry` decorators"""
        decorator = self.retry_decorator()
        if decorator:
            return decorator(f)
        return f


def decorate_func_with_retry(func):
    """wrap `func` with the retry policy defined on the connector.
    If the retry policy is None, just leave the `get_df` implementation as is.
    """

    @wraps(func)
    def get_func_and_retry(self, *args, **kwargs):
        if self.retry_decorator:
            return self.retry_decorator(func)(self, *args, **kwargs)
        else:
            return func(self, *args, **kwargs)

    return get_func_and_retry


def get_oauth2_configuration(cls):
    """Return a tuple indicating if the connector is an oauth2 connector
    and in this case, where can the credentials be located
    """
    oauth2_enabled = hasattr(cls, '_auth_flow') and 'oauth2' in getattr(cls, '_auth_flow')
    oauth2_credentials_location = None
    if hasattr(cls, '_oauth_trigger'):
        oauth2_credentials_location = getattr(cls, '_oauth_trigger')
    return oauth2_enabled, oauth2_credentials_location


# Deprecated
def is_oauth2_connector(cls) -> bool:
    return get_oauth2_configuration(cls)[0]  # pragma: no cover


def needs_sso_credentials(cls) -> bool:
    return hasattr(cls, '_sso_credentials_access') and getattr(cls, '_sso_credentials_access')


class ConnectorSecretsForm(BaseModel):
    documentation_md: str = Field(description='This field contains documentation as a md string')
    secrets_schema: dict = Field(description='The schema for the configuration form')


def get_connector_secrets_form(cls) -> ConnectorSecretsForm | None:
    """
    Some connectors requires 2 steps of configuration.
    First one by an administrator
    Second one by an end user
    eg. a connector using an oauth2 flow will need to have some parameters, such as client_id and client_secret
    provided by an administrator, and not by an end user.
    To document this, ToucanConnector subclasses can implement as a @classmethod 'get_connector_config_form' that will
    return which fields SHOULD be provided by an administrator
    """
    if hasattr(cls, 'get_connector_secrets_form'):
        return getattr(cls, 'get_connector_secrets_form')()
    return None


_UI_HIDDEN: dict[str, Any] = {'ui.hidden': True}

DS = TypeVar('DS', bound=ToucanDataSource)


class ToucanConnector(BaseModel, Generic[DS], metaclass=ABCMeta):
    """Abstract base class for all toucan connectors.

    Each concrete connector should implement the `get_df` method that accepts a
    datasource definition and return the corresponding pandas dataframe. This
    base class allows to specify a retry policy on the `get_df` method. The
    default is not to retry on error but you can customize some of connector
    model parameters to define custom retry policy.

    Model parameters:


    - `max_attempts`: the maximum number of retries before giving up
    - `max_delay`: delay, in seconds, above which we should give up
    - `wait_time`: time, in seconds, between each retry.

    In order to retry only on some custom exception classes, you can override
    the `_retry_on` class attribute in your concrete connector class.
    """

    name: str = Field(...)
    retry_policy: RetryPolicy | None = RetryPolicy()
    _retry_on: Iterable[Type[BaseException]] = ()
    type: str | None
    secrets_storage_version: str = Field('1', **_UI_HIDDEN)  # type:ignore[pydantic-field]

    # Default ttl for all connector's queries (overridable at the data_source level)
    # /!\ cache ttl is used by the caching system which is not implemented in toucan_connectors.
    cache_ttl: int | None = Field(
        None,
        title="Slow Queries' Cache Expiration Time",
        description='In seconds. Will override the 5min instance default. Can also be overridden at the query level',
    )

    # Used to defined the connection
    identifier: str = Field(None, **_UI_HIDDEN)  # type:ignore[pydantic-field]

    class Config:
        extra = 'forbid'
        validate_assignment = True
        json_encoders = {
            SecretStr: lambda v: v.get_secret_value(),
            SecretBytes: lambda v: v.get_secret_value(),
        }

    @classmethod
    def __init_subclass__(cls):
        try:
            cls.data_source_model = cls.__fields__.pop('data_source_model').type_
            cls.logger = logging.getLogger(cls.__name__)
        except KeyError as e:
            raise TypeError(f'{cls.__name__} has no {e} attribute.')
        if 'bearer_integration' in cls.__fields__:
            cls.bearer_integration = cls.__fields__['bearer_integration'].default

    def bearer_oauth_get_endpoint(
        self,
        endpoint: str,
        query: dict | None = None,
    ):
        """Generic method to get an endpoint for an OAuth API integrated with Bearer"""
        return (
            Bearer(os.environ.get('BEARER_API_KEY'))
            .integration(self.bearer_integration)  # type: ignore[attr-defined]
            .auth(self.bearer_auth_id)  # type: ignore[attr-defined]
            .get(endpoint, query=query)
            .json()
        )

    @property
    def retry_decorator(self):
        kwargs = {**self.retry_policy.dict(), 'retry_on': self._retry_on, 'logger': self.logger}
        return RetryPolicy(**kwargs)

    @abstractmethod
    def _retrieve_data(self, data_source: DS):
        """Main method to retrieve a pandas dataframe"""

    @decorate_func_with_retry
    def get_df(self, data_source: DS, permissions: dict | None = None) -> pd.DataFrame:
        """
        Method to retrieve the data as a pandas dataframe
        filtered by permissions
        """
        res = self._retrieve_data(data_source)
        res.columns = res.columns.astype(str)
        res = sanitize_df_dates(res)

        if permissions is not None:
            permissions_query = PandasConditionTranslator.translate(permissions)
            permissions_query = apply_query_parameters(
                permissions_query, data_source.parameters or {}
            )
            res = res.query(permissions_query)
        return res

    def get_slice(
        self,
        data_source: DS,
        permissions: dict | None = None,
        offset: int = 0,
        limit: int | None = None,
        get_row_count: bool | None = False,
    ) -> DataSlice:
        """
        Method to retrieve a part of the data as a pandas dataframe
        and the total size filtered with permissions

        - offset is the index of the starting row
        - limit is the number of rows to retrieve
        Exemple: if offset = 5 and limit = 10 then 10 results are expected from 6th row

        Args:
          get_row_count: used in some connectors to optionally get the total number of
            rows from a request, before limit (Snowflake)
        """
        df = self.get_df(data_source, permissions)
        truncated_df = df[offset : offset + limit] if limit is not None else df[offset:]

        pagination_info = build_pagination_info(
            offset=offset, limit=limit, retrieved_rows=len(truncated_df), total_rows=len(df)
        )

        return DataSlice(
            truncated_df,
            pagination_info=pagination_info,
            stats=DataStats(df_memory_size=df.memory_usage().sum()),
        )

    def explain(self, data_source: DS, permissions: dict | None = None):
        """Method to give metrics about the query"""
        return None

    @staticmethod
    def check_hostname(hostname):
        """Check if a hostname is resolved"""
        socket.gethostbyname(hostname)

    @staticmethod
    def check_port(host, port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))

    def get_status(self) -> ConnectorStatus:
        """
        Check if connection can be made.
        Returns
        {
          'status': True/False/None  # the status of the connection (None if no check has been made)
          'details': [(< type of check >, True/False/None), (...), ...]
          'error': < error message >  # if a check raised an error, return it
          'message': < status message > # optionally provides some additional info, such as the user account connected
        }
        e.g.
        {
          'status': False,
          'details': [
            ('hostname resolved', True),
            ('port opened', False,),
            ('db validation', None),
            ...
          ],
          'error': 'port must be 0-65535'
        }
        """
        return ConnectorStatus()

    def get_unique_identifier(self) -> str:
        """
        Returns a serialized version of the connector's config.
        Override this method in connectors which have not-serializable properties.

        Used by `get_cache_key` method.
        """
        return self.json()

    def _get_unique_datasource_identifier(self, data_source: DS) -> dict:
        # By default we don't know which variable syntax is be supported by the inheriting connector,
        # so calling `nosql_apply_parameters_to_query` is wrong and will produce the same cache key
        # for different queries when using custom variable syntax !
        # Overwrite this method to improve the cache key at places where supported syntaxes are clear.
        return data_source.dict()

    def get_cache_key(
        self,
        data_source: DS | None = None,
        permissions: dict | None = None,
        offset: int = 0,
        limit: int | None = None,
    ) -> str:
        """
        Generate a unique identifier (str) for a given connector's configuration
        (if no parameters are supplied) or for a given couple connector/query
        configuration (if `data_source` parameter is supplied).
        This identifier will then be used as a cache key.
        """
        unique_identifier = {
            'connector': self.get_unique_identifier(),
            'permissions': nosql_apply_parameters_to_query(
                permissions or {}, data_source.parameters
            )
            if data_source
            else permissions,
            'offset': offset,
            'limit': limit,
        }

        if data_source is not None:
            unique_identifier['datasource'] = self._get_unique_datasource_identifier(data_source)
        json_uid = JsonWrapper.dumps(unique_identifier, sort_keys=True, default=hash)
        string_uid = str(uuid.uuid3(uuid.NAMESPACE_OID, json_uid))
        return string_uid

    def get_identifier(self):
        json_uid = JsonWrapper.dumps(self.get_unique_identifier(), sort_keys=True)
        string_uid = str(uuid.uuid3(uuid.NAMESPACE_OID, json_uid))
        return string_uid

    def describe(self, data_source: DS):
        """ """


TableInfo = dict[str, Union[str, list[dict[str, str]]]]


class DiscoverableConnector(ABC):
    @abstractmethod
    def get_model(self, db_name: str | None = None) -> list[TableInfo]:
        """Tries to get all tables that are available for the connector"""

    def get_model_with_info(self, db_name: str | None = None) -> tuple[list[TableInfo], dict]:
        """Tries to get all tables that are available for the connector
        and gives back some infos about what happened"""
        return (self.get_model(db_name=db_name), {})

    @staticmethod
    def format_db_model(
        # db, schema, type, name, columns as dict or json string
        unformatted_db_tree: list[tuple[str, str, str, str, list[dict[str, str]] | str]]
    ) -> list[TableInfo]:
        if not unformatted_db_tree:
            return []
        df = pd.DataFrame(unformatted_db_tree)
        df.columns = ['database', 'schema', 'type', 'name', 'columns']  # type:ignore[assignment]
        try:  # if columns is a string
            df['columns'] = df['columns'].apply(json.loads)
        except TypeError:  # else ignore
            pass
        return (
            df.groupby(by=['schema', 'database', 'type', 'name'])['columns']  # type: ignore[return-value]
            .apply(sum)
            .reset_index()
            .to_dict('records')
        )


class MalformedVersion(Exception):
    """raised when the given version of the engine is not well formated"""


class UnavailableVersion(Exception):
    """raised when the version of the engine is not available"""


class VersionableEngineConnector(ABC):
    """
    This class is responsible for formating and rendering the version engine
    from which the connector is fetching data from !

    We built a regex match for all common version structure that can be describe in 6 points :
    - Major version is required, minor and patch version, and the meta version are supported but optional.
    - Major, minor, and/or patch can be 0 but cannot be a non-zero value with a leading 0.
    - The meta version can contain any Unicode letter (not restricted to Latin characters), _, ., and - characters.
    - Minor version values must suffix to a major version.
    - Patch version values must suffix to a minor version.
    - Meta version values can suffix to a minor or patch version.

    Match Groups:

    1- Major
    2- Minor (optional - requires major to match)
    3- Patch (optional - requires minor to match)
    4- Meta/pre-release (optional - requires minor to match)
    You can add the ^ prefix and $suffix to do match enforcement.
    """

    # The output of these rules :
    semver_regex = re.compile(
        r'(0|(?:[1-9]\d*))(?:\.(0|(?:[1-9]\d*))(?:\.(0|(?:[1-9]\d*)))?(?:\-([\w][\w\.\-_]*))?)?'
    )

    def _validate(self, engine_version: str | float | None) -> re.Match | None:
        """
        A small validation function for incoming version format
        """
        if engine_version is None:
            raise UnavailableVersion

        return self.semver_regex.match(str(engine_version))

    @abstractmethod
    def get_engine_version(self) -> tuple:
        """This method will return the version of the target engine"""

    def _format_version(self, input_version: str | float | None = None) -> tuple:
        """
        We get the input version from the engine and convert it in tuple
        Either :
            - 1.45 -> (1, 45)
            - "0.9.3" -> (0, 9, 3)
            - "3.4.57 Debian+only" -> (3, 4, 57)
        """
        input_version_validated: re.Match | None = self._validate(input_version)
        if input_version_validated is not None:
            return tuple(map(int, input_version_validated.group(0).split('.')))

        raise MalformedVersion(f'"{input_version}" is not understood')
