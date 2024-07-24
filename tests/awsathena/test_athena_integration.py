import os
from typing import Any

import pandas as pd
import pytest
from pandas.core.dtypes.common import is_float_dtype, is_object_dtype
from pandas.testing import assert_frame_equal

from toucan_connectors.awsathena.awsathena_connector import AwsathenaConnector, AwsathenaDataSource


@pytest.fixture
def athena_connector() -> AwsathenaConnector:
    return AwsathenaConnector(
        name="test-athena",
        s3_output_bucket=os.environ.get("ATHENA_OUTPUT"),
        aws_access_key_id=os.environ.get("ATHENA_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("ATHENA_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("ATHENA_REGION"),
    )


def _athena_datasource(query: str, parameters: dict[str, Any] | None = None) -> AwsathenaDataSource:
    return AwsathenaDataSource(
        name="test-athena-datasource",
        domain="test-domain",
        database=os.environ.get("ATHENA_DATABASE"),
        use_ctas=False,
        query=query,
        parameters=parameters,
    )


@pytest.fixture
def expected_beers() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "price_per_l": [
                0.1599999964237213,
                0.05400000140070915,
                0.004999999888241291,
                0.0560000017285347,
                0.07000000029802322,
                0.004999999888241291,
                0.04500000178813934,
                0.08399999886751175,
                0.06499999761581421,
                0.05999999865889549,
            ],
            "alcohol_degree": [
                13.5,
                4.877377033233643,
                0.5699344873428345,
                6.994105339050293,
                8.930644989013672,
                1.2437106370925903,
                4.717409610748291,
                12.974271774291992,
                7.74746561050415,
                8.749608993530273,
            ],
            "name": [
                "Superstrong beer",
                "Ninkasi Ploploplop",
                "Brewdog Nanny State Alcoholvrij",
                "Ardwen Blonde",
                "Cuvée des Trolls",
                "Weihenstephan Hefe Weizen Alcoholarm",
                "Bellfield Lawless Village IPA",
                "Pauwel Kwak",
                "Brasserie De Sutter Brin de Folie",
                "Brugse Zot blonde",
            ],
            "cost": [
                2.890000104904175,
                2.890000104904175,
                2.2899999618530273,
                2.0899999141693115,
                1.5499999523162842,
                1.590000033378601,
                2.490000009536743,
                1.690000057220459,
                2.190000057220459,
                1.7899999618530273,
            ],
            "beer_kind": [
                "Triple",
                "India Pale Ale",
                "Sans alcool",
                "Best-sellers",
                "Blonde",
                "Blanche & Weizen",
                "India Pale Ale",
                "Belge blonde forte & Golden Ale",
                "Blonde",
                "Blonde",
            ],
            "volume_ml": [330.0, 330.0, 330.0, 330.0, 250.0, 500.0, 330.0, 330.0, 330.0, 330.0],
            "brewing_date": [
                pd.Timestamp("2022-01-01 00:00:00"),
                pd.Timestamp("2022-01-02 00:00:00"),
                pd.Timestamp("2022-01-03 00:00:00"),
                pd.Timestamp("2022-01-04 00:00:00"),
                pd.Timestamp("2022-01-05 00:00:00"),
                pd.Timestamp("2022-01-06 00:00:00"),
                pd.Timestamp("2022-01-07 00:00:00"),
                pd.Timestamp("2022-01-08 00:00:00"),
                pd.Timestamp("2022-01-09 00:00:00"),
                pd.Timestamp("2022-01-10 00:00:00"),
            ],
            "nullable_name": [
                None,
                "Ninkasi Ploploplop",
                "Brewdog Nanny State Alcoholvrij",
                "Ardwen Blonde",
                None,
                "Weihenstephan Hefe Weizen Alcoholarm",
                "Bellfield Lawless Village IPA",
                "Pauwel Kwak",
                None,
                "Brugse Zot blonde",
            ],
        }
    )
    for col_name, dtype in df.dtypes.items():
        if is_float_dtype(dtype):
            df[col_name] = df[col_name].astype("float32")
        elif is_object_dtype(dtype):
            df[col_name] = df[col_name].astype("string")

    return df


def test_simple_query(athena_connector: AwsathenaConnector, expected_beers: pd.DataFrame) -> None:
    result = athena_connector.get_df(data_source=_athena_datasource("SELECT * FROM beers_tiny"))
    assert_frame_equal(expected_beers, result)


def test_simple_query_with_parameters(athena_connector: AwsathenaConnector, expected_beers: pd.DataFrame) -> None:
    result = athena_connector.get_df(
        data_source=_athena_datasource(
            "SELECT * FROM beers_tiny WHERE beer_kind = {{ beer_kind }} AND COST > {{ cost }}",
            {"beer_kind": "Blonde", "cost": 2},
        )
    )
    expected = expected_beers[(expected_beers["beer_kind"] == "Blonde") & (expected_beers["cost"] > 2)].reset_index(
        drop=True
    )
    assert_frame_equal(expected, result)


def test_simple_query_with_missing_parameters(
    athena_connector: AwsathenaConnector, expected_beers: pd.DataFrame
) -> None:
    result = athena_connector.get_df(
        data_source=_athena_datasource(
            "SELECT * FROM beers_tiny WHERE beer_kind = {{ beer_kind }} AND (COST > {{ cost }} OR name = {{ nope }})",
            {"beer_kind": "Blonde", "cost": 2},
        )
    )
    expected = expected_beers[(expected_beers["beer_kind"] == "Blonde") & (expected_beers["cost"] > 2)].reset_index(
        drop=True
    )
    assert_frame_equal(expected, result)
