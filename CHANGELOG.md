# Changelog (Pypi package)

## Unreleased

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
