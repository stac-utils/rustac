
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Outfile and stream arguments to `items` ([#363](https://github.com/stac-utils/stac-rs/pull/363))
- `object_store` ([#371](https://github.com/stac-utils/stac-rs/pull/371))

### Fixed

- ndjson reads ([363](https://github.com/stac-utils/stac-rs/pull/363))

### Changed

- Update **geoarrow** to v0.3.0 ([#367](https://github.com/stac-utils/stac-rs/pull/367))

## [0.3.1] - 2024-09-06

### Added

- `stacrs items` ([#360](https://github.com/stac-utils/stac-rs/pull/360))

### Fixed

- **tokio** panic when validating ([#358](https://github.com/stac-utils/stac-rs/pull/358))

## [0.3.0] - 2024-09-05

### Added

- Geoparquet support ([#300](https://github.com/stac-utils/stac-rs/pull/300))
- Auto-create collections when serving ([#304](https://github.com/stac-utils/stac-rs/pull/304))
- Auto-add items when serving ([#312](https://github.com/stac-utils/stac-rs/pull/312))
- Searching geoparquet files with DuckDB ([#331](https://github.com/stac-utils/stac-rs/pull/331))
- Python package ([#335](https://github.com/stac-utils/stac-rs/pull/335))

## [0.2.0] - 2024-08-12

### Added

- `migrate` subcommand ([#294](https://github.com/stac-utils/stac-rs/pull/294))

### Changed

- Switch to using structures for command arguments, and move the `execute` methods to those structures ([#285](https://github.com/stac-utils/stac-rs/pull/285))

## [0.1.0] - 2024-04-29

### Added

- `stac serve` ([#244](https://github.com/stac-utils/stac-rs/pull/244))

## [0.0.8] - 2024-04-22

### Added

- `stac sort` can take stdin ([#241](https://github.com/stac-utils/stac-rs/pull/241))

### Changed

- Re-organized the CLI code architecture ([#243](https://github.com/stac-utils/stac-rs/pull/243))

## [0.0.7] - 2024-04-11

### Added

- `stac validate` can take from stdin ([#236](https://github.com/stac-utils/stac-rs/pull/236))
- `stac item` to create items ([#237](https://github.com/stac-utils/stac-rs/pull/237))
- The `gdal` feature ([#232](https://github.com/stac-utils/stac-rs/pull/232))

## [0.0.6] - 2023-10-18

### Added

- Validation for the collections endpoint ([#208](https://github.com/stac-utils/stac-rs/pull/208))

## [0.0.5] - 2023-10-11

### Added

- Sort ([#197](https://github.com/stac-utils/stac-rs/pull/197))
- Search ([#200](https://github.com/stac-utils/stac-rs/pull/200))

### Removed

- Downloading (use [stac-asset](https://github.com/stac-utils/stac-asset) instead) ([#194](https://github.com/stac-utils/stac-rs/pull/194))

## [0.0.4] - 2023-10-09

### Changed

- Better error messages for `stac validate` ([#190](https://github.com/stac-utils/stac-rs/pull/190))

## [0.0.3] - 2023-04-04

Moved over from [stac-incubator-rs](https://github.com/gadomski/stac-incubator-rs) ([#142](https://github.com/stac-utils/stac-rs/pull/142))

### Added

- Downloading ([#142](https://github.com/stac-utils/stac-rs/pull/142), [#152](https://github.com/stac-utils/stac-rs/pull/152))
- Validation ([#155](https://github.com/stac-utils/stac-rs/pull/155))

[Unreleased]: https://github.com/stac-utils/stac-rs/compare/stac-cli-v0.3.1..main
[0.3.1]: https://github.com/stac-utils/stac-rs/compare/stac-cli-v0.3.0..stac-cli-v0.3.1
[0.3.0]: https://github.com/stac-utils/stac-rs/compare/stac-cli-v0.2.0..stac-cli-v0.3.0
[0.2.0]: https://github.com/stac-utils/stac-rs/compare/stac-cli-v0.1.0..stac-cli-v0.2.0
[0.1.0]: https://github.com/stac-utils/stac-rs/compare/stac-cli-v0.0.8..stac-cli-v0.1.0
[0.0.8]: https://github.com/stac-utils/stac-rs/compare/stac-cli-v0.0.7..stac-cli-v0.0.8
[0.0.7]: https://github.com/stac-utils/stac-rs/compare/stac-cli-v0.0.6..stac-cli-v0.0.7
[0.0.6]: https://github.com/stac-utils/stac-rs/compare/stac-cli-v0.0.5..stac-cli-v0.0.6
[0.0.5]: https://github.com/stac-utils/stac-rs/compare/stac-cli-v0.0.4..stac-cli-v0.0.5
[0.0.4]: https://github.com/stac-utils/stac-rs/compare/stac-cli-v0.0.3..stac-cli-v0.0.4
[0.0.3]: https://github.com/stac-utils/stac-rs/tree/stac-cli-v0.0.3

<!-- markdownlint-disable-file MD024 -->
