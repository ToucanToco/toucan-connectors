import copy
import tempfile

import pytest

from tests.tools import (
    default_zip_file,
    DF,
    DF2)
from toucan_data_sdk.sdk import extract_zip, extract, DataSdkError


@pytest.fixture(name='zip_content', scope='function')
def gen_zipfile():
    return copy.deepcopy(default_zip_file(DF, DF2))


@pytest.fixture(name='df', scope='function')
def gen_df():
    return DF.copy(deep=True)


@pytest.fixture(name='df2', scope='function')
def gen_df2():
    return DF2.copy(deep=True)


def test_extract_zip(zip_content):
    with tempfile.NamedTemporaryFile() as tmp_file:
        tmp_file.write(zip_content)
        tmp_file.flush()
        tmp_file.seek(0)

        dfs = extract_zip(tmp_file.name)
        assert 'df' in dfs
        assert 'df2' in dfs
        assert DF.equals(dfs['df'])
        assert DF2.equals(dfs['df2'])


def test_extract(df, df2, mocker):
    mock_extract_zip = mocker.patch('toucan_data_sdk.sdk.extract_zip')
    mock_extract_zip.return_value = 1

    # 1. Is a (valid) zip file
    zip_content = default_zip_file(df, df2)
    res = extract(zip_content)

    assert mock_extract_zip.call_count == 1
    assert res == 1

    # 2. Is not a zip file
    mock_is_zip_file = mocker.patch('zipfile.is_zipfile')
    mock_is_zip_file.return_value = False

    with pytest.raises(DataSdkError):
        extract(zip_content)

    # 3. Unknown input data
    with pytest.raises(DataSdkError):
        extract(b'string is not a valid input')
