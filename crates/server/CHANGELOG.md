# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.2](https://github.com/stac-utils/rustac/compare/stac-server-v0.4.1...stac-server-v0.4.2) - 2026-01-05

### Other

- *(deps)* update reqwest requirement from 0.12.8 to 0.13.1 ([#926](https://github.com/stac-utils/rustac/pull/926))

## [0.4.1](https://github.com/stac-utils/rustac/compare/stac-server-v0.4.0...stac-server-v0.4.1) - 2025-12-15

### Other

- switch to release-plz ([#911](https://github.com/stac-utils/rustac/pull/911))
- update releasing to be much simpler ([#899](https://github.com/stac-utils/rustac/pull/899))

## [0.4.0](https://github.com/stac-utils/rustac/compare/stac-server-v0.3.6...stac-server-v0.4.0) (2025-12-01)


### ⚠ BREAKING CHANGES

* move stac_api crate into stac crate ([#869](https://github.com/stac-utils/rustac/issues/869))
* remove unused error enums ([#868](https://github.com/stac-utils/rustac/issues/868))

### Bug Fixes

* remove unused error enums ([#868](https://github.com/stac-utils/rustac/issues/868)) ([cf0e815](https://github.com/stac-utils/rustac/commit/cf0e815e03433e8ef219a79a67161174f3e99e84))


### Code Refactoring

* move stac_api crate into stac crate ([#869](https://github.com/stac-utils/rustac/issues/869)) ([d0f7405](https://github.com/stac-utils/rustac/commit/d0f7405a811dd2c3b044404b4a6a48cf07926a89))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * pgstac bumped from 0.3.0 to 0.4.0
    * stac bumped from 0.14.0 to 0.15.0
    * stac-duckdb bumped from 0.2.0 to 0.3.0

## [0.3.6] - 2025-11-14

Update **stac** dependency.

## [0.3.5] - 2025-07-11

### Added

- DuckDB backend ([#651](https://github.com/stac-utils/rustac/pull/651))

## [0.3.4] - 2025-01-31

Bump axum dependency.

## [0.3.3] - 2025-01-14

### Removed

- **stac-types** dependency ([#561](https://github.com/stac-utils/rustac/pull/561))

## [0.3.2] - 2024-11-12

### Added

- Filter extension for **pgstac** backend ([#519](https://github.com/stac-utils/rustac/pull/519))

## [0.3.1] - 2024-09-19

### Changed

- Bump **stac** to v0.10.0, **stac-api** to v0.6.0

## [0.3.0] - 2024-09-16

### Added

- Parameterize `PgstacBackend` on tls provider ([#383](https://github.com/stac-utils/rustac/pull/383))

### Removed

- **stac-async** dependency ([#369](https://github.com/stac-utils/rustac/pull/369))

## [0.2.0] - 2024-09-05

### Added

- Auto-create collections on ingest ([#304](https://github.com/stac-utils/rustac/pull/304))
- Auto-add items on ingest ([#312](https://github.com/stac-utils/rustac/pull/312))
- Permissive CORS layer
- Public `router::{Error, GeoJson}` types ([#326](https://github.com/stac-utils/rustac/pull/326))

### Changed

- `axum` is no longer a default feature ([#322](https://github.com/stac-utils/rustac/pull/322))

### Removed

- `memory-item-search` feature ([#322](https://github.com/stac-utils/rustac/pull/322))
- `APPLICATION_GEO_JSON` and `APPLICATION_OPENAPI_3_0` constants (they're now in `stac::mime`) ([#327](https://github.com/stac-utils/rustac/pull/327))
- `async_trait` ([#347](https://github.com/stac-utils/rustac/pull/347))

## [0.1.1] - 2024-08-12

### Added

- `impl Default for MemoryBackend` ([#252](https://github.com/stac-utils/rustac/pull/252))

## [0.1.0] - 2024-04-29

Initial release.

[Unreleased]: https://github.com/stac-utils/rustac/compare/stac-server-v0.3.6..main
[0.3.6]: https://github.com/stac-utils/rustac/compare/stac-server-v0.3.5..stac-server-v0.3.6
[0.3.5]: https://github.com/stac-utils/rustac/compare/stac-server-v0.3.4..stac-server-v0.3.5
[0.3.4]: https://github.com/stac-utils/rustac/compare/stac-server-v0.3.3..stac-server-v0.3.4
[0.3.3]: https://github.com/stac-utils/rustac/compare/stac-server-v0.3.2..stac-server-v0.3.3
[0.3.2]: https://github.com/stac-utils/rustac/compare/stac-server-v0.3.1..stac-server-v0.3.2
[0.3.1]: https://github.com/stac-utils/rustac/compare/stac-server-v0.3.0..stac-server-v0.3.1
[0.3.0]: https://github.com/stac-utils/rustac/compare/stac-server-v0.2.0..stac-server-v0.3.0
[0.2.0]: https://github.com/stac-utils/rustac/compare/stac-server-v0.1.1..stac-server-v0.2.0
[0.1.1]: https://github.com/stac-utils/rustac/compare/stac-server-v0.1.0..stac-server-v0.1.1
[0.1.0]: https://github.com/stac-utils/rustac/releases/tag/stac-server-v0.1.0

<!-- markdownlint-disable-file MD024 -->
