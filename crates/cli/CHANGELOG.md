# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.9](https://github.com/stac-utils/rustac/compare/rustac-v0.2.8...rustac-v0.2.9) - 2026-02-19

### Other

- updated the following local packages: stac, stac-duckdb, stac-io, pgstac, stac-server, stac-validate

## [0.2.8](https://github.com/stac-utils/rustac/compare/rustac-v0.2.7...rustac-v0.2.8) - 2026-02-18

### Added

- get and put streams ([#958](https://github.com/stac-utils/rustac/pull/958))

## [0.2.7](https://github.com/stac-utils/rustac/compare/rustac-v0.2.6...rustac-v0.2.7) - 2026-02-12

### Other

- updated the following local packages: stac, stac-io, stac-validate, pgstac, stac-duckdb, stac-server

## [0.2.6](https://github.com/stac-utils/rustac/compare/rustac-v0.2.5...rustac-v0.2.6) - 2026-02-03

### Added

- add search_with_headers ([#948](https://github.com/stac-utils/rustac/pull/948))
- add custom headers to search api requests ([#943](https://github.com/stac-utils/rustac/pull/943))

### Other

- bump msrv version ([#944](https://github.com/stac-utils/rustac/pull/944))

## [0.2.5](https://github.com/stac-utils/rustac/compare/rustac-v0.2.4...rustac-v0.2.5) - 2026-01-21

### Added

- add a collection command ([#940](https://github.com/stac-utils/rustac/pull/940))

## [0.2.4](https://github.com/stac-utils/rustac/compare/rustac-v0.2.3...rustac-v0.2.4) - 2026-01-20

### Other

- updated the following local packages: stac, stac-io, stac-validate, pgstac, stac-duckdb, stac-server

## [0.2.3](https://github.com/stac-utils/rustac/compare/rustac-v0.2.2...rustac-v0.2.3) - 2026-01-14

### Added

- search directly from pgstac ([#933](https://github.com/stac-utils/rustac/pull/933))

## [0.2.2](https://github.com/stac-utils/rustac/compare/rustac-v0.2.1...rustac-v0.2.2) - 2026-01-05

### Added

- datetime expansion ([#917](https://github.com/stac-utils/rustac/pull/917))

## [0.2.1](https://github.com/stac-utils/rustac/compare/rustac-v0.2.0...rustac-v0.2.1) - 2025-12-15

### Other

- update releasing to be much simpler ([#899](https://github.com/stac-utils/rustac/pull/899))

## [0.2.0](https://github.com/stac-utils/rustac/compare/rustac-v0.1.2...rustac-v0.2.0) (2025-12-01)


### ⚠ BREAKING CHANGES

* move stac_api crate into stac crate ([#869](https://github.com/stac-utils/rustac/issues/869))
* move api client to stac-io crate ([#864](https://github.com/stac-utils/rustac/issues/864))

### Features

* add bind argument when serving ([#871](https://github.com/stac-utils/rustac/issues/871)) ([f3a3517](https://github.com/stac-utils/rustac/commit/f3a35179cf5026cc100313faa2010e6a1af4efb7))
* shell completions ([#874](https://github.com/stac-utils/rustac/issues/874)) ([717c4ee](https://github.com/stac-utils/rustac/commit/717c4ee62b993730d54dcf91534b39d69242db0e)), closes [#650](https://github.com/stac-utils/rustac/issues/650)
* specify max_row_group_size in geoparquet WriterBuilder ([#846](https://github.com/stac-utils/rustac/issues/846)) ([2bde538](https://github.com/stac-utils/rustac/commit/2bde538b41e5900b5be2d75587b1f8904520b3a1))


### Code Refactoring

* move api client to stac-io crate ([#864](https://github.com/stac-utils/rustac/issues/864)) ([e06de28](https://github.com/stac-utils/rustac/commit/e06de28787f9868f000ccc884979dcede1984f01)), closes [#764](https://github.com/stac-utils/rustac/issues/764)
* move stac_api crate into stac crate ([#869](https://github.com/stac-utils/rustac/issues/869)) ([d0f7405](https://github.com/stac-utils/rustac/commit/d0f7405a811dd2c3b044404b4a6a48cf07926a89))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * stac bumped from 0.14.0 to 0.15.0
    * stac-duckdb bumped from 0.2.0 to 0.3.0
    * stac-io bumped from 0.1.0 to 0.2.0
    * stac-server bumped from 0.3.2 to 0.4.0
    * stac-validate bumped from 0.5.0 to 0.6.0

## [0.1.2] - 2025-11-14

Update **stac** dependency.

## [0.1.1] - 2025-10-22

### Changed

- Don't write a newline when outputting ndjson ([#824](https://github.com/stac-utils/rustac/pull/824))

## [0.1.0] - 2025-07-11

First release as **rustac**.

### Added

- DuckDB server backend ([#651](https://github.com/stac-utils/rustac/pull/651))
- Crawl ([#756](https://github.com/stac-utils/rustac/pull/756))

## [stac-cli 0.5.3] - 2025-02-20

### Added

- Tracing subscriber to get verbosity

## [stac-cli 0.5.2] - 2025-02-20

### Removed

- A lot of unused dependencies

## [stac-cli 0.5.1] - 2025-02-19

### Changed

- Always bundle DuckDB (again) ([#631](https://github.com/stac-utils/rustac/pull/631))

### Removed

- **stacrs-cli** (moved to <https://github.com/stac-utils/stacrs>) ([#633](https://github.com/stac-utils/rustac/pull/633))

## [stac-cli 0.5.0] - 2025-01-31

### Changed

- Pretty big code refactor and a lot of options ([#607](https://github.com/stac-utils/rustac/pull/607))

## [stac-cli 0.4.1] - 2024-10-22

### Changed

- Bump **stac-api** version

## [stac-cli 0.4.0] - 2024-09-19

### Added

- Outfile and stream arguments to `items` ([#363](https://github.com/stac-utils/rustac/pull/363))

## [stac-cli 0.3.1] - 2024-09-06

### Added

- `stacrs items` ([#360](https://github.com/stac-utils/rustac/pull/360))

### Fixed

- **tokio** panic when validating ([#358](https://github.com/stac-utils/rustac/pull/358))

## [stac-cli 0.3.0] - 2024-09-05

### Added

- Geoparquet support ([#300](https://github.com/stac-utils/rustac/pull/300))
- Auto-create collections when serving ([#304](https://github.com/stac-utils/rustac/pull/304))
- Auto-add items when serving ([#312](https://github.com/stac-utils/rustac/pull/312))
- Searching geoparquet files with DuckDB ([#331](https://github.com/stac-utils/rustac/pull/331))
- Python package ([#335](https://github.com/stac-utils/rustac/pull/335))

## [stac-cli 0.2.0] - 2024-08-12

### Added

- `migrate` subcommand ([#294](https://github.com/stac-utils/rustac/pull/294))

### Changed

- Switch to using structures for command arguments, and move the `execute` methods to those structures ([#285](https://github.com/stac-utils/rustac/pull/285))

## [stac-cli 0.1.0] - 2024-04-29

### Added

- `stac serve` ([#244](https://github.com/stac-utils/rustac/pull/244))

## [stac-cli 0.0.8] - 2024-04-22

### Added

- `stac sort` can take stdin ([#241](https://github.com/stac-utils/rustac/pull/241))

### Changed

- Re-organized the CLI code architecture ([#243](https://github.com/stac-utils/rustac/pull/243))

## [stac-cli 0.0.7] - 2024-04-11

### Added

- `stac validate` can take from stdin ([#236](https://github.com/stac-utils/rustac/pull/236))
- `stac item` to create items ([#237](https://github.com/stac-utils/rustac/pull/237))
- The `gdal` feature ([#232](https://github.com/stac-utils/rustac/pull/232))

## [stac-cli 0.0.6] - 2023-10-18

### Added

- Validation for the collections endpoint ([#208](https://github.com/stac-utils/rustac/pull/208))

## [stac-cli 0.0.5] - 2023-10-11

### Added

- Sort ([#197](https://github.com/stac-utils/rustac/pull/197))
- Search ([#200](https://github.com/stac-utils/rustac/pull/200))

### Removed

- Downloading (use [stac-asset](https://github.com/stac-utils/stac-asset) instead) ([#194](https://github.com/stac-utils/rustac/pull/194))

## [stac-cli 0.0.4] - 2023-10-09

### Changed

- Better error messages for `stac validate` ([#190](https://github.com/stac-utils/rustac/pull/190))

## [stac-cli 0.0.3] - 2023-04-04

Moved over from [stac-incubator-rs](https://github.com/gadomski/stac-incubator-rs) ([#142](https://github.com/stac-utils/rustac/pull/142))

### Added

- Downloading ([#142](https://github.com/stac-utils/rustac/pull/142), [#152](https://github.com/stac-utils/rustac/pull/152))
- Validation ([#155](https://github.com/stac-utils/rustac/pull/155))

[Unreleased]: https://github.com/stac-utils/rustac/compare/rustac-v0.1.2..main
[0.1.2]: https://github.com/stac-utils/rustac/compare/rustac-v0.1.1..rustac-v0.1.2
[0.1.1]: https://github.com/stac-utils/rustac/compare/rustac-v0.1.0..rustac-v0.1.1
[0.1.0]: https://github.com/stac-utils/rustac/tree/rustac-v0.1.0
[stac-cli 0.5.3]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.5.2..stac-cli-v0.5.3
[stac-cli 0.5.2]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.5.1..stac-cli-v0.5.2
[stac-cli 0.5.1]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.5.0..stac-cli-v0.5.1
[stac-cli 0.5.0]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.4.1..stac-cli-v0.5.0
[stac-cli 0.4.1]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.4.0..stac-cli-v0.4.1
[stac-cli 0.4.0]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.3.1..stac-cli-v0.4.0
[stac-cli 0.3.1]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.3.0..stac-cli-v0.3.1
[stac-cli 0.3.0]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.2.0..stac-cli-v0.3.0
[stac-cli 0.2.0]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.1.0..stac-cli-v0.2.0
[stac-cli 0.1.0]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.0.8..stac-cli-v0.1.0
[stac-cli 0.0.8]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.0.7..stac-cli-v0.0.8
[stac-cli 0.0.7]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.0.6..stac-cli-v0.0.7
[stac-cli 0.0.6]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.0.5..stac-cli-v0.0.6
[stac-cli 0.0.5]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.0.4..stac-cli-v0.0.5
[stac-cli 0.0.4]: https://github.com/stac-utils/rustac/compare/stac-cli-v0.0.3..stac-cli-v0.0.4
[stac-cli 0.0.3]: https://github.com/stac-utils/rustac/tree/stac-cli-v0.0.3

<!-- markdownlint-disable-file MD024 -->
