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
Create a new folder in `connectors` for the new connector. Please expose your connector in
the `__init__.py` of your folder! It is important to be able to import the new connector
directly from the directory!

#### Step 3
Add the requirements to the `setup.cfg` in the `[options.extras_require]` section:
```
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
