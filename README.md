[![Pypi-v](https://img.shields.io/pypi/v/toucan-connectors.svg)](https://pypi.python.org/pypi/toucan-connectors)
[![Pypi-pyversions](https://img.shields.io/pypi/pyversions/toucan-connectors.svg)](https://pypi.python.org/pypi/toucan-connectors)
[![Pypi-l](https://img.shields.io/pypi/l/toucan-connectors.svg)](https://pypi.python.org/pypi/toucan-connectors)
[![Pypi-wheel](https://img.shields.io/pypi/wheel/toucan-connectors.svg)](https://pypi.python.org/pypi/toucan-connectors)
[![CircleCI](https://img.shields.io/circleci/project/github/ToucanToco/toucan-connectors.svg)](https://circleci.com/gh/ToucanToco/toucan-connectors)
[![codecov](https://codecov.io/gh/ToucanToco/toucan-connectors/branch/master/graph/badge.svg)](https://codecov.io/gh/ToucanToco/toucan-connectors)

# Toucan Connectors
All connectors available

## Adding a connector
In order to work you need `Python 3.6` and `pip install -U setuptools`.
You can then install:
- the main requirements by typing `pip install .`
- the test requirements by typing `pip install .[test]`

#### Step 1
Create a new folder in `tests` for the new connector. You can start writing your tests
before implementing it. Please do not hesitate to add a docker image in
the `docker-compose.yml`. You can then use the fixture `service_container` to automatically
start the docker and shut it down for you!

#### Step 2
Create a new folder in `toucan_connectors` for the new connector and create your classes
```python
class MyDataSource(ToucanDataSource):
    """Model of my datasource"""
    query: str
    

class MyConnector(ToucanConnector):
    """Model of my connector"""
    type = 'MyType'
    data_source_model: MyDataSource

    host: str
    port: int
    database: str
    
    def get_df(self):
        """how to retrieve a dataframe"""
```


#### Step 3
Add the requirements to the `setup.cfg` in the `[options.extras_require]` section:
```ini
new_connector =
    dependency_package1=x.x.x
    dependency_package2==x.x.x
    
test =
    ...
    ...
    dependency_package1=x.x.x
    dependency_package2=x.x.x
```
and don't forget to upgrade the version !
