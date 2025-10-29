# Changelog (Pypi package)

## Unreleased

### Added

- Redshift: `get_model` now supports filtering on the database, schema and table name.

### Fixed

- Redshift: also return tables outside of the "public" schema in `get_model` methods.
- Lower constraint on `pydantic` has been bumped to `>=2.12`, following the support of `union_format` introduced in #2058

## [10.1.0] 2025-10-15

### Changed

- Upper and lower constraints on various dependencies have been raised

## [10.0.0] 2025-08-22

### Changed

- **breaking**: Support for Python<3.13 has been dropped.

## [9.2.2] 2025-08-05

### Fixed

- Postgres: Transform `__VOID__` parameters into `NULL` values following upgrade of `psycopg` from v2 to v3

## [9.2.1] 2025-07-30

### Fixed

- Snowflake: use the default warehouse when retrieving the DB model
- Snowflake: add `private_key` to the model schema
- Snowflake: sanitize the private key before connecting to the DB

## [9.2.0] 2025-07-30

### Added

- Snowflake: The connector now supports keypair-based authentication.

## [9.1.0] 2025-07-22

### Changed

- Elasticsearch: we now depend on `elasticsearch-py>=9`. It is now possible to specify the target elasticsearch version
  via the `es_version` parameter.

### Fixed

- HTTPApi: the DataFrame returned by `_retrieve_data` is now properly reindexed

## [9.0.0] 2025-07-15

### Changed

- **breaking** The Postgres connector's driver switched from `psycopg2` to `psycopg3`. As a consequence, array parameters are no longer interpolated as tuples in strings, meaning that in order to use query parameters, the `ANY()` function should be used instead of the `IN` operator: https://www.psycopg.org/psycopg3/docs/basic/from_pg2.html#you-cannot-use-in-s-with-a-tuple
* The Postgres connector now uses `psycopg` via `sqlalchemy` rather than directly

## [8.0.3] 2025-05-28

### Fixed

- Dependencies: loosened upper constraints

### Changed

- Loosened constraints on `elasticsearch` to allow using v9

## [8.0.2] 2025-04-14

### Fixed

- MSSQL & Azure MSSQL: The connect timeout when retrieving the connector form now defaults to 5 seconds

## [8.0.1] 2025-04-14

### Fixed

- Azure MSSQL: The connector properly exports `CONNECTOR_OK` again
- MSSQL: get form even if database or table search is unsuccessful
- MSSQL: only wait 5 seconds for the database and table list in connector form

## [8.0.0] 2025-04-09

### Added

- MSSQL: it is now possible to trust self-signed certificates via the new `trust_server_certificate` parameter

### Changed

- **Breaking**: MSSQL: user attributes are now interpreted as regular SQL variables

## [7.8.0] 2025-03-25

### Fixed

- The ODBC install script now relies on the distro's install script rather than Toucan's.

## [7.7.8] 2025-02-13

### Fixed

- Datetime sanitization does not fail on out-of-bounds dates anymore. Instead, they get coerced to pandas `NaT` objects

## [7.7.7] 2025-02-13

### Fixed

- GoogleSheets: the `retrieve_token` field is now excluded when serializing an instance of the connector

## [7.7.6] 2025-02-11

### Fixed

- Github & Snowflake & OneDrive & Salesforce connectors can now be instantiated without backend fields or a secrets keeper

## [7.7.5] 2025-02-07

### Fixed

- GoogleSheets: can generate connector's pydantic schema again.

## [7.7.4] 2025-02-06

### Fixed

- Oauth2 & GoogleSheets: can now instantiate connectors without providing secret keeper or callback functions.

## [7.7.3] 2025-01-29

### Changed

- HTTP API: Oauth2 secret tokens now accepts null refresh-token. Some oauth2 token providers can return un-expirable access_tokens.

## [7.7.2] 2025-01-27

### Fixed

- Fixed HTTP API connector import error

## [7.7.1] 2025-01-27

### Fixed

- Wheels include `png`, `md` and `sh` files again

## [7.7.0] 2025-01-27

### Changed

- Switched from `poetry` to `uv` for package management
- HTTP API: Added support for OAuth2 authentication

### Fixed

- Fixed google credentials import for Google Big Query and Google Translator connectors

## [7.6.0] 2025-01-16

### Changed

- Added support for Python 3.13

## [7.5.0] 2025-01-15

### Changed

- MSSQL and Azure MSQQL connectors now use the ODBC 18 driver
- The Oracle connector install script is now compatible with Ubuntu 24.04

## [7.4.1] 2024-12-12

### Fixed

- Prevent injection in Jinja templates

## [7.4.0] 2024-12-02

### Changed

- The Azure MSSQL connector now uses `sqlalchemy` to connect to MSSQL.

## [7.3.3] 2024-11-21

### Fixed

- The lib is now compatible with pydantic 2.10

## [7.3.2] 2024-11-19

### Fixed

- Mongo: when reading data by chunk, ignore each individual chunk index when concatenating them.

## [7.3.1] 2024-11-14

### Fixed

- Mongo: correctly type the aggregation pipeline, expected when `query` is a `list`

## [7.3.0] 2024-11-13

### Added

- Mongo: Added an optional `chunk_size` param to get_df, to create the dataframe chunk by chunk (saves memory)

## [7.2.0] 2024-11-06

### Fix

- HTTP API: API results are now correctly merged even if they need to be filtered or flattened.

### Added

- HTTP API: Add `data_filter` offset pagination config field to determine which part of data must be used to compute the data length.

## [7.1.1] 2024-10-28

### Fix

- HTTP API: Missing dependencies for the HTTP API connector do not prevent the import of the lib anymore

## [7.1.0] 2024-10-28

### Changed

- HTTP API: Add a `PaginationConfig` to `HttpAPIDataSource` in order to handle API pagination and fetch all data. It supports the following kinds of pagination: page-based, cursor-based, offset-limit and hypermedia.

## [7.0.3] 2024-10-04

### Fix

- Google BigQuery: If the dtype of a column in the `DataFrame` returned by `_retrive_data` is `object`,
  it gets converted to `Int64` or `float64` when it is defined as a numeric dtype by Big Query.
- When testing connection, set timeout to 10s when checking if port is opened.

## [7.0.2] 2024-09-13

### Changed

- jinja templates :: expressions containing parentheses or curly braces are not limited to output
  strings anymore.
- Google BigQuery :: increase limits when fetching db tree structure

## [7.0.1] 2024-09-10

### Fix

- MySQL / Redshift / Snowflake oAuth2 :: `get_model` now supports extra kwargs so it doesn't crash

## [7.0.0] 2024-09-06

### Changed

- It is now possible to import all connectors models without having any extra installed
- The deprecated `GoogleSpreadsheet` connector has been removed.
- The deprecated `GoogleSheets2` connector has been removed.

## [6.7.0] 2024-09-02

### Changed

- Postgres: Rather than being silently caught, exceptions happenning in `get_form` and `get_model` are now logged
- HTTP: The `custom_token_server` authentication type now accepts a `token_header_name` kwarg. It allows to override the name of the
  authorization header, which defaults to `Authorization`.

## [6.6.0] 2024-08-23

### Changed

- `get_model` now alphabetically sorts the columns before returning them, in order to ensure result consistency.
- `get_model` now supports an `exclude_columns` argument defaulting to `False`. It allows to not retrieve columns in the model.
  This is only implemented in the Postgres connector for now.
- `get_model` now supports `schema_name` and `table_name` arguments, allowing to filter on a specific table and/or schema.
  This is only implemented in the Postgres connector for now.
- DiscoverableConnector: `format_db_model` is now roughly 3x faster, resulting in performance gains in `get_model`.

## [6.5.0] 2024-07-31

### Changed

- Hubspot: added support for listing and selection of custom objects

## [6.4.0] 2024-07-25

### Changed

- Hubspot: added support for custom attributes

### Fix

- Athena: params are now correctly interpolated

## [6.3.2] 2024-07-17

### Fix

- Dependencies: removed the upper bound on peakina

## [6.3.1] 2024-07-08

### Fix

- BigQuery: the JWT token auth method is now supported in the status check.
- HTTP: allow connector to be instantiated without passing positional arguments to auth.

## [6.3.0] 2024-06-21

### Fix

- OracleSQL: Fix jinja templates and test string fixtures

### Changed

- Datetime series returned by our connectors don't have timezones anymore

## [6.2.0] 2024-06-12

### Changed

- OracleSQL: Add variables templating support

### Fix

- MySQL: An unknown exception during the status check now makes the check fail

## [6.1.3] 2024-05-24

### Changed

- MySQL: Add an optional `charset_collation` to the connector, as PyMySQL >=1.1.0 always runs a `SET NAMES` on connection,
  which breaks on servers using a non-default collation

### Fix

- MySQL: Allow dict parameters to be used with PyMySQL 1.1.1
- MySQL: Use a regular PyMySQL Cursor rather than a DictCursor when pandas 2.x is used

## [6.1.2] 2024-04-18

### Fixed

- Google Big Query: the query generated by `get_model` now correctly quotes the dataset name, which allows to build a
  DB models for datasets starting with a number

## [6.1.1] 2024-03-18

### Fixed

- Elasticsearch: force `widget="json"` on `body` so the form is properly filled when updating a data source

## [6.1.0] 2024-03-13

### Changed

- Added support for Python 3.12

### Fixed

- Restored the `HubspotPrivateApp` connector, which was deleted by error in v6.0.0

## [6.0.0] 2024-03-12

### Changed

- **Breaking**: Support for Python 3.10 has been dropped.
- **Breaking**: The following connectors have been removed:
    * Wootric
    * Trello
    * Toucan Toco
    * Net Explorer
    * Linkedin Ads
    * Microstrategy
    * Hubspot
    * Google My Business
    * Google Adwords
    * Facebook Insights
    * Facebook Ads
    * Anaplan
    * Adobe Analytics

### Fixed

- Google Big Query: do not exclude partitioning columns when listing table structure

## [5.3.0] 2024-02-14

### Changed

- Mongo: maximal connection pool size is now configurable via the `max_pool_size` parameter. It defaults to 1

## [5.2.0] 2024-02-08

### Changed

- Google Big Query: an actual connection check is now done in `get_status`, rather than just a private key validation.
- SQL connectors: duplicate columns are now renamed with a suffix indicating their position. A duplicate `my_column` column
  now becomes `my_column_0`, `my_column_1`...

## [5.1.0] 2024-01-23

### Changed

- Google Big Query: A simple status check that validates the private key's format has been implemented
- Elasticsearch: Host verification has been disabled to tolerate strict network configurations

### Fixed

- Install scripts: fix oracle install script by replacing gdown.pl with wget

## [5.0.0] 2023-12-15

### Changed

- Postgres: Materialized views are now returned as well via `get_model`. Their type is `'view'`.

- **Breaking:** The version requirement for pydantic has been increased to `>=2.4.2,<3`

## [4.9.6] 2023-11-23

### Fixed

- Removed the upper constraint on `pyarrow<14`

## [4.9.5] 2023-10-27

### Fixed

- Revert a change (from 4.9.3) that prevented the publication of the package on pypi

## [4.9.4] 2023-10-27

### Fixed

- Update DataBricks connector

### [4.9.2] 2023-10-04

## Fixed

- Google Big Query: get project_id from connector config whatever auth mode (JWT/GoogleCreds).

### [4.9.1] 2023-09-22

## Fixed

- Goole Big Query:
    - Better UX (Switch between GoogleCreds auth or GoogleJWT  auth).
    - Explicit errors information when no data is returned.
    - Fallback on GoogleCredentials auth when JWTCredentials fails (or when jwt-token is not valid aymore).

### [4.9.0] 2023-09-20

## Changed

- Goole Big Query: Now support signed JWT connection on the GBQ connector.

### [4.8.1] 2023-09-18

### Fixed

- Postgres: In case two tables in different schemas have the same name, `get_model`
  and `get_model_with_info` now return the correct information.

### [4.8.0] 2023-09-13

### Changed

- S3: Add a new AWS S3 connector using the Security Token Service (STS) API Assume Role.

### Fixed

- Install scripts: fix mssql install scripts by forcing debian/11 deb repo

### [4.7.3] 2023-08-22

### Fixed

- GoogleSheets: Replace empty values by numpy `NaN`.

### [4.7.2] 2023-07-19

### Fixed

- Redshift: Ignore Programming Error when table_infos is empty for a database.

### [4.7.1] 2023-07-19

### Fixed

- PyYaml: Fix broken dependency and bump it from 5.4.1 to >=6,<7

### [4.7.0] 2023-07-07

## Changed

- Feat[Goole Big Query] : We can now get the database model(list of tables) based on a given schema name to speed up the project tree structure.
- Fix: on mysql, avoid duplicated columns when retrieving table informations

### [4.6.0] 2023-06-02

### Changed

- The exception raised by `nosql_apply_parameters_to_query` when `handle_errors` is true and an undefined variable is encountered has changed from  `NonValidVariable`  to `UndefinedVariableError`.
- `__VOID__` values are no longer removed from queries.

### [4.5.1] 2023-04-27

### Fixed

- Added a missing dependency on `aiohttp`

### [4.5.0] 2023-04-24

### Changed

- This release officially adds support for Python 3.11
- The `awswrangler` dependency has been bumped to `^3.0.0`
- For SQL connectors, `get_model()` 's output is now filtered on the passed db name, if it is specified

### Removed

- The `Hive` connector has been deleted
- The `Indexima` connector has been deleted
- The `Rok` connector has been deleted
- The `Lightspeed` connector has been deleted
- The `Revinate` connector has been deleted

### [4.4.1] 2023-03-30

### Changed

- Bump Peakina from 0.9.x to 0.10.x

### [4.4.0] 2023-03-07

### Changed

- The upper constraint on python < 3.11 has been lifted. **This does not mean that Python 3.11 is officially supported yet**.

### [4.3.3] 2023-03-03

### Fixed

- MySQL: It is now possible to use the MySQL connector with a CA bundle in VERIFY_IDENTITY mode

### [4.3.2] 2023-03-01

### Fixed

- HubSpot: root-level properties are now also returned along with proeprties in the "properties" object

### [4.3.1] 2023-02-27

### Fixed

- HubSpot: it is now possible to retrieve a data slice for owners

### [4.3.0] 2023-02-23

### Changed

- HubSpot: Added a new connector based on HubSpot private apps
- MySQL: Allow Optional parameters on ssl_mode

### [4.2.2] 2023-01-24

### Fixed

- MongoConnector: Now handle "__VOID__" in $and match conditions.
### [4.2.1] 2023-01-04

### Fixed

- Export of the peakina Connector through `CONNECTOR_REGISTRY`.

### [4.2.0] 2023-01-01

### Changed

- Added a new Connector: [Peakina](https://github.com/ToucanToco/peakina) for files.

### [4.1.1] 2022-12-27

### Fixed

- Google Big Query no longer crashes when trying to retrieve the table list for datasets in different locations.

### Changed

- `Dates as float` is now selected by default in Google Sheets data sources.

### [4.1.0] 2022-12-02

### Changed

- Feat: The connector `GoogleSheets` datasource now has an option called `Dates as Floats`, to see date time columns as strings or float when reading the sheet.

### [4.0.0] 2022-11-23

### Breaking changes

Pagination information has been refactored. The `DataSlice` and `DataStats` interfaces have been changed:

* `DataStats` no longer has `total_rows` and `total_returned_rows` fields.
* `DataSlice` now has a `pagination_info` field in its root. This field is required and contains a `PaginationInfo` model.

For information about the `PaginationInfo` model and how to interpret its contents, see the [documentation](doc/PaginationInfo.md).

### [3.25.0] 2022-11-23

### Changed

- Deps: Upper constraint on cryptography has been loosened from <37 -> <39
- Snowflake: The snowflake connector has been refactored in order to prevent spawning threads
  and connection pooling.

### [3.24.0] 2022-11-07

### Changed

- Fix: drop `date_as_object` argument since we moved on to for google bigquery 3.

### [3.23.4] 2022-10-28

### Changed

- Fix: Ensure Postgres always uses the default database for connection, rather than 'postgres'.

### [3.23.3] 2022-10-26

### Changed

- Fix regression introduced in the mongo connector in 3.23.2 where `$match` statements containing only matches on
  nulls were considered empty.

### [3.23.2] 2022-10-20

### Changed

- Fix: Add support for `__VOID__` syntax to `nosql_apply_parameters_to_query`

### [3.23.1] 2022-10-07

### Changed

- Fix: Fixed the % character replacement on edges cases for `pandas_read_sql`.

### [3.23.0] 2022-10-04

### Changed

- MySQL: Added support for REQUIRED ssl_mode

### [3.22.3] 2022-10-04

### Changed

- Fix: Replace % character by %% in `pandas_read_sql` to prevent pandas from interpreting `%` as the interpolation of an SQL parameter

### [3.22.2] 2022-09-29

### Changed

- Fix: Ensure timezone-aware timestamp column are converted to UTC

### [3.22.1] 2022-09-28

### Changed

- The contraint of the `lxml` dependency has been loosened from `4.9.1` to `^4.6.5`.

### [3.22.0] 2022-09-28

### Changed

- The package now exposes a `__version__` attribute.
- The contraint of the `pyarrow` dependency has been loosened from `<7` to `<9`.

### [3.21.1] 2022-09-27

### Changed

- Automate PyPI artifact publication

### [3.21.0] 2022-09-20

### Changed

- MySQL: Add support for SSL-based authentication

### [3.20.6] 2022-09-14

### Changed

- Google Big Query: fix variables interpolation.

### [3.20.5] 2022-09-09

### Changed

- Athena: fix order of OFFSET and LIMIT query parameters

### [3.20.4] 2022-09-07

### Changed

- Athena: fix the parameter injection

### [3.20.3] 2022-09-07

### Changed

- Base connector: Fixed pagination values (`total_rows` and `total_returned_rows`)
- Athena: Hacked pagination values in case not all results were fetched

### [3.20.2] 2022-09-05

### Changed

- Mongo: removed `_id` column in response DataFrame.

### [3.20.1] 2022-09-02

### Changed

- All connectors: removed werkzeug dependency.

### [3.19.0] 2022-08-26

### Changed

- All connectors: Add support for an optional `db_name` parameter in the `get_model` method.
- MySQL: Use the provided `db_name` for discoverability when possible in `get_model`.
- MySQL: Simplify query for schema construction in order to be compatible with older versions
- Redshift: Add an option to disable TCP keep-alive (enabled by default).

### [3.18.4] 2022-08-26

### Changed

- MySQL: Do not specify a database on discoverability-related functions (listing databases and describing table schemas).

### [3.18.3] 2022-08-24

### Changed

- Conditions: The unquoting logic is now only applied when the passed parameter is a string

### [3.18.2] 2022-08-23

### Changed

- Athena: Parameters are now passed as SQL parameters rather than interpolated by us in order to prevent SQL injection.
- Conditions: Strings are now unquoted for conditions applying only to numbers (`lt`, `lte`, `gt`, `gte`).

### [3.18.1] 2022-08-12

### Changed

- MySQL: Return a more explicit error message in case no query is specified

### [3.18.0] 2022-08-12

### Changed

- Mysql: Revert the `following_relations` attribute as deprecated
- Athena: Add an option allowing to toggle CTAS (disabled by default)

## [3.17.2] 2022-07-27

### Changed

- Fix: Mysql, Athena add hidden table attribute to avoid old datasources configs to break

## [3.17.1] 2022-07-27

### Changed

- Fix: Mysql replace quoting character

## [3.17.0] 2022-07-27

### Changed

- Feat: Mysql & Athena graphical selection interface

## [3.16.0] 2022-07-18

### Changed

- Feat: Mongo connector's `get_slice_with_regex` method now supports a dict of lists of regex patterns to match for
  in the different columns

## [3.15.3] 2022-06-30

### Changed

- Fix redshift connector: Removing pooling due to table locks
- Feature nosql_apply_parameters_to_query: add tuple render capabilities

## [3.15.2] 2022-06-30

### Changed

- Ignore extra attributes in BigQueryDataSource for graphical selection


## [3.15.1] 2022-06-30

### Changed

- Add attributes & methods to big query connector for graphical selection

## [3.15.0] 2022-06-29

### Changed

- Implement exploration in google big query connector

## [3.14.1] 2022-06-28

### Changed

- Make exploration faster and add form for redshift connector

## [3.14.0] 2022-06-25

### Changed

- Improve order and default values of fields of the redshift connector

### Fixed

- Get table information from redshift connector

## [3.13.0] 2022-06-24

### Changed

- Added default database field for redshift and postgres connectors
- Added a new status check for request on default databases

## [3.12.0] 2022-06-23

### Changed

Remove the table attribute from RedshiftDataBaseConnector

## [3.11.0] 2022-06-17

### Changed

Add support for elasticsearch >= 8 on the ElasticsearchConnector.

## [3.0.0] 2022-02-03

### Changed

The connector `GoogleSheets` based on bearer.sh (discontinued service) has been replaced by a new one, agnostic of the
OAuth manager used. This new connector needs a `retrieve_token` function to get valid authentication tokens.

It also features automatic dates parsing and uses the official google API python client.

## [2.0.0] 2022-01-19

### Changed

Some DataStats properties changed in the naming and some of them was added, see [HERE](https://github.com/ToucanToco/toucan-connectors/commit/9d74efb6a6e37c6fbcd951743ac418aa84911704) for more informations.

### Fixed

- Fixes on sql/snowflake (don't run count for DESCRIBE or SHOW queries + don't use -1 as default rows count)
- Fixes on sharepoint and onedrive connectors.

## [1.3.43] 2022-01-17

### Added

- Added filenames_to_match param to extract multiple files on connectors sharepoint and onedrive.
- Added a dev container for developping safely on connectors.
