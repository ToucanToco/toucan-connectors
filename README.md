[![Pypi-v](https://img.shields.io/pypi/v/toucan-connectors.svg)](https://pypi.python.org/pypi/toucan-connectors)
[![Pypi-pyversions](https://img.shields.io/pypi/pyversions/toucan-connectors.svg)](https://pypi.python.org/pypi/toucan-connectors)
[![Pypi-l](https://img.shields.io/pypi/l/toucan-connectors.svg)](https://pypi.python.org/pypi/toucan-connectors)
[![Pypi-wheel](https://img.shields.io/pypi/wheel/toucan-connectors.svg)](https://pypi.python.org/pypi/toucan-connectors)
[![GitHub Actions](https://github.com/ToucanToco/toucan-connectors/workflows/CI/badge.svg)](https://github.com/ToucanToco/toucan-connectors/actions?query=workflow%3ACI)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=ToucanToco_toucan-connectors&metric=coverage)](https://sonarcloud.io/dashboard?id=ToucanToco_toucan-connectors)

# Toucan Connectors
[Toucan Toco](https://toucantoco.com/fr/) data connectors are plugins to the Toucan Toco platform,
configured with dictionaries (cf. `DataSource` class) and returning
[Pandas DataFrames](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html).

## Setup
In order to work you need `make` and `Python 3.8` (consider
running `pip install -U pip setuptools` if needed)
You can then install:
- the main dependencies by typing `pip install -e .`
- the test requirements by typing `pip install -r requirements-testing.txt`

You should be able to run basic tests `pytest tests/test_connector.py`

Consider installing [pre-commit](https://pre-commit.com) to profit form linting hooks:
```
$ pip install pre-commit
$ pre-commit install
```

:warning: To test and use `mssql` (and `azure_mssql`) you need to install the Microsoft ODBC driver for SQL Server for
[Linux](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15)
or [MacOS](https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/install-microsoft-odbc-driver-sql-server-macos?view=sql-server-ver15)

:warning: On macOS, to test the `postgres` connector, you need to install `postgresql` by running for instance `brew install postgres`.
You can then install the library with `env LDFLAGS='-L/usr/local/lib -L/usr/local/opt/openssl/lib -L/usr/local/opt/readline/lib' pip install psycopg2`

## Testing a connector
If you want to run the tests for another connector, you can install the extra dependencies
(e.g to test MySQL just type `pip install -e ".[mysql]"`)
Now `pytest tests/mysql` should run all the mysql tests properly.

If you want to run the tests for all the connectors you can add all the dependencies by typing
`pip install -e ".[all]"` and `make test`.

## Adding a connector

To generate the connector and test modules from boilerplate, run:

```
$ make new_connector type=mytype
```

`mytype` should be the name of a system we would like to build a connector for,
such as `MySQL` or `Hive` or `Magento`.

#### Step 1 : Tests
Open the folder in `tests` for the new connector. You can start writing your tests
before implementing it.

Some connectors are tested with calls to the actual data systems that they target,
for example `elasticsearch`, `mongo`, `mssql`. Other are tested with mocks of the
classes or functions returning data that you are wrapping (see : `HttpAPI`, or
`microstrategy`).

If you have a container for your target system, please do not hesitate to add a docker image in
the `docker-compose.yml`. You can then use the fixture `service_container` to automatically
start the docker and shut it down for you!

:warning: _The test runner assumes you have all the docker images locally,
if not please run pytest with `--pull` to retrieve them_

#### Step 2 : New connector
Open the folder `mytype` in `toucan_connectors` for your new connector and
create your classes

```python
import pandas as pd

# Careful here you need to import ToucanConnector from the deep path, not the __init__ path.
from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class MyTypeDataSource(ToucanDataSource):
    """Model of my datasource"""
    query: str


class MyTypeConnector(ToucanConnector):
    """Model of my connector"""
    data_source_model: MyTypeDataSource

    host: str
    port: int
    database: str

    def _retrieve_data(self, data_source: MyTypeDataSource) -> pd.DataFrame:
        """how to retrieve a dataframe"""
```

Please add your connector in `toucan_connectors/__init__.py`.
The key is what we call the `type` of the connector, which
is basically like an id used to retrieve it.
```python
CONNECTORS_CATALOGUE = {
  ...,
  'MyType': 'mytype.mytype_connector.MyTypeConnector',
  ...
}
```

You can now generate and edit the documentation page for your connector:

```shell
PYTHONPATH=. python doc/generate.py MyTypeConnector > doc/mytypeconnector.md
```

#### Step 3 : Register your connector
Add the main requirements to the `setup.py` in the `extras_require` dictionary:
```ini
extras_require = {
    ...
    'mytype': ['my_dependency_pkg1==x.x.x', 'my_dependency_pkg2>=x.x.x']
}
```
If you need to add testing dependencies, add them to the `requirements-testing.txt` file.

### Step 4 : Create a pull request
Make sure your new code is properly formatted by typing `make lint`.
If it's not, please use `make format`!
You can now create a pull request!
