import re
import urllib.parse
from contextlib import suppress
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Any

import boto3
import pandas as pd
from dateutil.tz import tzutc
from pandas.io.parsers.readers import TextFileReader
from peakina import DataSource
from pydantic import Field, validator

from toucan_connectors.common import ConnectorStatus
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class S3DataSource(ToucanDataSource):
    file: str = Field(..., description="Your File")
    reader_kwargs: dict[str, Any] = {}
    fetcher_kwargs: dict[str, Any] = {}


class S3Connector(ToucanConnector, data_source_model=S3DataSource):
    _sts_role: dict

    bucket_name: str = Field(..., description="Your Bucket Name")
    prefix: str | None = Field("", description="A Prefix for your objects like a path folder Ex: /marketing/revenues")
    role_arn: str = Field(..., description="The Role ARN")
    external_id: str = Field(
        ...,
        description="This is the external ID you need to use on your AWS policy configuration",
        ui={"readonly": True},
    )

    def __init__(self, **data):
        data["external_id"] = data.get("workspace_id", data.get("external_id"))
        super().__init__(**data)

    class Config:
        extra = "allow"
        validate_on_assignment = True

    @validator("external_id", pre=True, always=True)
    def _validate_external_id(cls, value: str, values: dict) -> str:  # noqa:N805
        return values.get("workspace_id", value)  # once set, external_id cannot be changed from the workspace id

    def get_status(self) -> ConnectorStatus:
        try:
            self._get_assumed_sts_role()
        except Exception as sts_exc:
            return ConnectorStatus(
                status=False,
                error=f"Cannot verify connection to S3 and/or AssumeRole failed : {sts_exc}",
            )
        return ConnectorStatus(status=True)

    def _forge_url(self, access_key: str, access_secret: str, session_token: str, file: str) -> str:
        # we encode the strings that may contain special characters
        access_key = urllib.parse.quote(access_key, safe="")
        access_secret = urllib.parse.quote(access_secret, safe="")
        session_token = urllib.parse.quote(session_token, safe="")

        # we control slashes ourselves
        prefix = (self.prefix or "").lstrip("/")
        file = file.lstrip("/")

        absolute_path = str(("/" / Path(self.bucket_name) / Path(prefix or "") / file).absolute()).removeprefix("/")

        forged_url = f"s3://{access_key}:{access_secret}@{absolute_path}"

        return forged_url

    def _retrieve_data(
        self,
        data_source: S3DataSource,
        offset: int = 0,
        limit: int | None = None,
    ) -> pd.DataFrame | TextFileReader:
        credentials = self._get_assumed_sts_role()["Credentials"]

        # See the documentation here for the schema :
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts/client/assume_role.html
        access_key = credentials["AccessKeyId"]
        access_secret = credentials["SecretAccessKey"]
        session_token = credentials["SessionToken"]

        reader_kwargs = {
            **data_source.reader_kwargs,
            "preview_nrows": limit,
            "preview_offset": offset,
        }
        fetcher_kwargs = data_source.fetcher_kwargs
        client_kwargs = fetcher_kwargs.get("client_kwargs", {})
        fetcher_kwargs = {
            **fetcher_kwargs,
            "client_kwargs": {**client_kwargs, "session_token": session_token},
        }

        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=access_secret,
            aws_session_token=session_token,
        )
        s3 = session.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.prefix)
        pattern = re.compile(rf"{data_source.file}")

        dfs = []
        # We loop over pages to fetch for available file's paths as object keys
        for page in pages:
            for obj_sum in page.get("Contents", []):
                file_path = obj_sum["Key"]
                # We check the regex match pattern for the given file path
                if pattern.match(file_path):
                    s3_uri = self._forge_url(
                        access_key=access_key,
                        access_secret=access_secret,
                        session_token=session_token,
                        file=file_path,
                    ).rstrip("$")
                    df = DataSource(
                        uri=s3_uri,
                        reader_kwargs=reader_kwargs,
                        fetcher_kwargs=fetcher_kwargs,
                    ).get_df()
                    df["__filename__"] = file_path
                    dfs.append(df)

        return pd.concat(dfs)

    @cached_property
    def _sts_assumed_role(self) -> dict:
        sts_client = boto3.client(
            "sts",
            aws_access_key_id=self.sts_access_key_id,
            aws_secret_access_key=self.sts_secret_access_key,
        )
        return sts_client.assume_role(
            RoleArn=self.role_arn,
            RoleSessionName="toucantoco",
            ExternalId=self.workspace_id,
        )

    def _get_assumed_sts_role(self) -> dict:
        with suppress(Exception):
            assumed_role = self._sts_assumed_role  # cached using @cached_property
            now = datetime.utcnow().replace(tzinfo=tzutc())
            if assumed_role["Credentials"]["Expiration"] > now:
                return assumed_role

        # If cache is expired, delete it and re-assume the role
        self.__dict__.pop("_sts_assumed_role", None)
        return self._sts_assumed_role
