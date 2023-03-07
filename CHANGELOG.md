# Changelog (Pypi package)

## Unreleased

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


[3.14.1]: https://github.com/ToucanToco/toucan-connectors/compare/v3.14.0...v3.14.1
[3.14.0]: https://github.com/ToucanToco/toucan-connectors/compare/v3.13.0...v3.14.0
[3.13.0]: https://github.com/ToucanToco/toucan-connectors/compare/v3.12.0...v3.13.0
[3.12.0]: https://github.com/ToucanToco/toucan-connectors/compare/v3.11.0...v3.12.0
[3.11.0]: https://github.com/ToucanToco/toucan-connectors/compare/v3.0.0...v3.11.0
[3.0.0]: https://github.com/ToucanToco/toucan-connectors/compare/v2.0.0...v3.0.0
[2.0.0]: https://github.com/ToucanToco/toucan-connectors/compare/v1.3.40...v2.0.0
[1.3.43]: https://github.com/ToucanToco/toucan-connectors/compare/v1.3.40...v1.3.44
