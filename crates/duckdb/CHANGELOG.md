# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Configure s3 credential chain ([#621](https://github.com/stac-utils/rustac/pull/621))
- Read hive partitioned datasets, `Config` structure ([#624](https://github.com/stac-utils/rustac/pull/624))
- `Client.search_to_arrow_table` ([#634](https://github.com/stac-utils/rustac/pull/634))
- Conditionally disable parsing the WKB ([#635](https://github.com/stac-utils/rustac/pull/635))
- `Client.extensions` ([#665](https://github.com/stac-utils/rustac/pull/665))
- `Config.install_extensions` ([#681](https://github.com/stac-utils/rustac/pull/681))
- `Config.from_href` ([#684](https://github.com/stac-utils/rustac/pull/684))

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

[Unreleased]: https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.1.1...main
[0.1.1]: https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.1.0...stac-duckdb-v0.1.1
[0.1.0]: https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.0.3...stac-duckdb-v0.1.0
[0.0.3]: https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.0.2...stac-duckdb-v0.0.3
[0.0.2]: https://github.com/stac-utils/rustac/compare/stac-duckdb-v0.0.1...stac-duckdb-v0.0.2
[0.0.1]: https://github.com/stac-utils/rustac/releases/tag/stac-duckdb-v0.0.1

<!-- markdownlint-disable-file MD024 -->
