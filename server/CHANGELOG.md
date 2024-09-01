# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Auto-create collections on ingest ([#304](https://github.com/stac-utils/stac-rs/pull/304))
- Auto-add items on ingest ([#312](https://github.com/stac-utils/stac-rs/pull/312))
- Permissive CORS layer
- Public `router::{Error, GeoJson}` types ([#326](https://github.com/stac-utils/stac-rs/pull/326))

### Changed

- `axum` is no longer a default feature ([#322](https://github.com/stac-utils/stac-rs/pull/322))

### Removed

- `memory-item-search` feature ([#322](https://github.com/stac-utils/stac-rs/pull/322))
- `APPLICATION_GEO_JSON` and `APPLICATION_OPENAPI_3_0` constants (they're now in `stac::mime`) ([#327](https://github.com/stac-utils/stac-rs/pull/327))

## [0.1.1] - 2024-08-12

### Added

- `impl Default for MemoryBackend` ([#252](https://github.com/stac-utils/stac-rs/pull/252))

## [0.1.0] - 2024-04-29

Initial release.

[Unreleased]: https://github.com/stac-utils/stac-rs/compare/stac-server-v0.1.1..main
[0.1.1]: https://github.com/stac-utils/stac-rs/compare/stac-server-v0.1.0..stac-server-v0.1.1
[0.1.0]: https://github.com/stac-utils/stac-rs/releases/tag/stac-server-v0.1.0

<!-- markdownlint-disable-file MD024 -->