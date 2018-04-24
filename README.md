[![Pypi-v](https://img.shields.io/pypi/v/toucan-connectors.svg)](https://pypi.python.org/pypi/toucan-connectors)
[![Pypi-pyversions](https://img.shields.io/pypi/pyversions/toucan-connectors.svg)](https://pypi.python.org/pypi/toucan-connectors)
[![Pypi-l](https://img.shields.io/pypi/l/toucan-connectors.svg)](https://pypi.python.org/pypi/toucan-connectors)
[![Pypi-wheel](https://img.shields.io/pypi/wheel/toucan-connectors.svg)](https://pypi.python.org/pypi/toucan-connectors)
[![CircleCI](https://img.shields.io/circleci/project/github/ToucanToco/toucan-connectors.svg)](https://circleci.com/gh/ToucanToco/toucan-connectors)
[![codecov](https://codecov.io/gh/ToucanToco/toucan-connectors/branch/master/graph/badge.svg)](https://codecov.io/gh/ToucanToco/toucan-connectors)

# Toucan Connectors
[Toucan Toco](https://toucantoco.com/fr/) data connectors.

## Setup
In order to work you need `Python 3.6` (consider running `pip install -U pip setuptools` if needed)
You can then install:
- the main dependencies by typing `pip install -e .`
- the test requirements by typing `pip install -r requirements-testing.txt`

You should be able to run basic tests `pytest tests/test_connector.py`

## Testing a connector
If you want to run the tests for another connector, you can install the extra dependencies  
(e.g to test MySQL just type `pip install -e ".[mysql]"`)  
Now `pytest tests/mysql` should run all the mysql tests properly.

If you want to run the tests for all the connectors you can add all the dependencies by typing  
`pip install -e ".[all]"` and `make test`

## Adding a connector
#### Step 1
Create a new folder in `tests` for the new connector. You can start writing your tests
before implementing it. Please do not hesitate to add a docker image in
the `docker-compose.yml`. You can then use the fixture `service_container` to automatically
start the docker and shut it down for you!

:warning: _If you don't have the docker images in local please run pytest with `--pull` to retrieve them_

#### Step 2
Create a new folder `mytype` in `toucan_connectors` for your new connector and
create your classes

You can generate the basic layout of the connector class using
this [repl](https://repl.it/@piotch/ToucanConnectorBoilerplate).

```python
import pandas as pd

# Careful here you need to import ToucanConnector from the deep path, not the __init__ path.
from toucan_connectors.toucan_connector import ToucanDataSource, ToucanConnector


class MyTypeDataSource(ToucanDataSource):
    """Model of my datasource"""
    query: str
    

class MyTypeConnector(ToucanConnector):
    """Model of my connector"""
    type = 'MyType'
    data_source_model: MyTypeDataSource

    host: str
    port: int
    database: str
    
    def get_df(self, data_source: MyTypeDataSource) -> pd.DataFrame:
        """how to retrieve a dataframe"""
```

Please add your connector in `toucan_connectors/__init__.py` :
```python
with suppress(ImportError):
    from .mytype.my_connector import MyConnector
```

You can now generate and edit the documentation page for your connector:

```shell
PYTHONPATH=. python doc/generate.py MyTypeConnector > doc/mytypeconnector.md
```

#### Step 3
Add the main requirements to the `setup.py` in the `extras_require` dictionary:
```ini
extras_require = {
    ...
    'mytype': ['my_dependency_pkg1==x.x.x', 'my_dependency_pkg2>=x.x.x']
}
```
If you need to add testing dependencies, add them to the `requirements-testing.txt` file.
