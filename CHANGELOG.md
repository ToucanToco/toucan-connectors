# Changelog (Pypi package)

## [1.3.44] 2022-01-19

### Added

- Add filenames_to_match param to extract multiple files on connectors sharepoint and onedrive.
- Added a dev container for developping safely on connectors.

### Changed

Some DataStats properties changed in the naming and some of them was added, see [HERE](https://github.com/ToucanToco/toucan-connectors/commit/9d74efb6a6e37c6fbcd951743ac418aa84911704) for more informations.

### Fixed

- Fixes on sql/snowflake (don't run count for DESCRIBE or SHOW queries + don't use -1 as default rows count)
- Fixes on sharepoint and onedrive connectors.


[1.3.44]: https://github.com/ToucanToco/toucan-connectors/compare/v1.3.40...v1.3.44
