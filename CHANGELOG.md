# Changelog (Pypi package)

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


[2.0.0]: https://github.com/ToucanToco/toucan-connectors/compare/v1.3.40...v2.0.0
[1.3.43]: https://github.com/ToucanToco/toucan-connectors/compare/v1.3.40...v1.3.44
