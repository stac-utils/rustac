# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.7](https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.3.6...stac-duckdb-v0.3.7) - 2026-02-19

### Other

- update Cargo.toml dependencies

## [0.3.6](https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.3.5...stac-duckdb-v0.3.6) - 2026-02-18

### Other

- create traits for clients ([#949](https://github.com/stac-utils/rustac/pull/949))

## [0.3.5](https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.3.4...stac-duckdb-v0.3.5) - 2026-02-12

### Other

- updated the following local packages: stac

## [0.3.4](https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.3.3...stac-duckdb-v0.3.4) - 2026-02-03

### Other

- bump msrv version ([#944](https://github.com/stac-utils/rustac/pull/944))

## [0.3.3](https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.3.2...stac-duckdb-v0.3.3) - 2026-01-20

### Other

- updated the following local packages: stac

## [0.3.2](https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.3.1...stac-duckdb-v0.3.2) - 2026-01-05

### Other

- updated the following local packages: stac

## [0.3.1](https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.3.0...stac-duckdb-v0.3.1) - 2025-12-15

### Other

- update releasing to be much simpler ([#899](https://github.com/stac-utils/rustac/pull/899))

## [0.3.0](https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.2.2...stac-duckdb-v0.3.0) (2025-12-01)


### ⚠ BREAKING CHANGES

* move stac_api crate into stac crate ([#869](https://github.com/stac-utils/rustac/issues/869))

### Bug Fixes

* remove filename by default ([#855](https://github.com/stac-utils/rustac/issues/855)) ([8bba676](https://github.com/stac-utils/rustac/commit/8bba67652da65f9423fd9fabdeed20d3fab668b1))


### Code Refactoring

* move stac_api crate into stac crate ([#869](https://github.com/stac-utils/rustac/issues/869)) ([d0f7405](https://github.com/stac-utils/rustac/commit/d0f7405a811dd2c3b044404b4a6a48cf07926a89))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * stac bumped from 0.14.0 to 0.15.0
  * dev-dependencies
    * stac-validate bumped from 0.5.0 to 0.6.0

## [0.2.2] - 2025-11-14

Update **stac** dependency.

### Added

- `build_query` ([#832](https://github.com/stac-utils/rustac/pull/832))

## [0.2.1] - 2025-09-23

Update dependencies.

## [0.2.0] - 2025-07-11

### Added

- Read hive partitioned datasets ([#624](https://github.com/stac-utils/rustac/pull/624))
- Conditionally disable parsing the WKB ([#635](https://github.com/stac-utils/rustac/pull/635))
- `Client.extensions` ([#665](https://github.com/stac-utils/rustac/pull/665))
- Filtering ([#699](https://github.com/stac-utils/rustac/pull/699))
- `union_by_name`, on by default ([#773](https://github.com/stac-utils/rustac/pull/773))

### Removed

- geoarrow record batch converters (moved to **stac**) ([#652](https://github.com/stac-utils/rustac/pull/652))

## [0.1.1] - 2025-01-31

### Added

- Offset ([#592](https://github.com/stac-utils/rustac/pull/592))
- `search` function ([#607](https://github.com/stac-utils/rustac/pull/607))

## [0.1.0] - 2025-01-02

### Changed

- Updated to **DuckDB** v1.1 and **geoarrow-rs** v0.4.0-beta.3 ([#562](https://github.com/stac-utils/rustac/pull/562))
- Only allow searching one **stac-geoparquet** file at a time ([#562](https://github.com/stac-utils/rustac/pull/562))

## [0.0.3] - 2024-11-21

### Added

- `version` ([#476](https://github.com/stac-utils/rustac/pull/476))

## [0.0.2] - 2024-09-19

### Changed

- Update **geoarrow** to v0.3.0 ([#367](https://github.com/stac-utils/rustac/pull/367))
- Bump **stac** to v0.6.0, **stac-api** to v0.6.0

## [0.0.1] - 2024-09-05

Initial release of **stac-duckdb**.

[Unreleased]: https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.2.2...main
[0.2.2]: https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.2.1...stac-duckdb-v0.2.2
[0.2.1]: https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.2.0...stac-duckdb-v0.2.1
[0.2.0]: https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.1.1...stac-duckdb-v0.2.0
[0.1.1]: https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.1.0...stac-duckdb-v0.1.1
[0.1.0]: https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.0.3...stac-duckdb-v0.1.0
[0.0.3]: https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.0.2...stac-duckdb-v0.0.3
[0.0.2]: https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.0.1...stac-duckdb-v0.0.2
[0.0.1]: https://github.com/stac-utils/rustac/releases/tag/stac-duckdb-v0.0.1

<!-- markdownlint-disable-file MD024 -->
