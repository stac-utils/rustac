# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Return JSON, not STAC ([#550](https://github.com/stac-utils/stac-rs/pull/550))

## [0.2.2] - 2024-11-12

Bump dependencies.

## [0.2.1] - 2024-09-19

### Changed

- Bump **stac** to v0.10.0, **stac-api** to v0.6.0

## [0.2.0] - 2024-09-16

### Added

- Unverified tls provider ([#383](https://github.com/stac-utils/stac-rs/pull/383))

## [0.1.2] - 2024-09-05

### Changed

- Bump **stac** version to v0.9
- Bump **stac-api** version to v0.5

## [0.1.1] - 2024-08-12

### Changed

- Bump **pgstac** version to v0.8.6

## [0.1.0] - 2024-04-29

### Changed

- Moved from <https://github.com/stac-utils/pgstac-rs> to the <https://github.com/stac-utils/stac-rs> monorepo ([#246](https://github.com/stac-utils/stac-rs/pull/246))

## [0.0.6] - 2024-04-20

- Bump **stac** version to v0.6
- Bump **pgstac** version to v0.8.5

## [0.0.5] - 2023-09-25

- Bump **stac-api** version to v0.3.0

## [0.0.4] - 2023-07-07

- Bump **stac** version to v0.5
- Bump **pgstac** version to v0.6.13 ([#2](https://github.com/stac-utils/pgstac-rs/pull/2))

## [0.0.3] - 2023-01-08

### Changed

- `Client` now takes a reference to a generic client, instead of owning it

### Removed

- `Client::into_inner`

## [0.0.2] - 2023-01-08

### Changed

- Make `Error`, `Result`, and `Context` publicly visible

## [0.0.1] - 2023-01-07

Initial release

[unreleased]: https://github.com/stac-utils/stac-rs/compare/pgstac-v0.2.2...HEAD
[0.2.2]: https://github.com/stac-utils/stac-rs/compare/pgstac-v0.2.1..pgstac-v0.2.2
[0.2.1]: https://github.com/stac-utils/stac-rs/compare/pgstac-v0.2.0..pgstac-v0.2.1
[0.2.0]: https://github.com/stac-utils/stac-rs/compare/pgstac-v0.1.2..pgstac-v0.2.0
[0.1.2]: https://github.com/stac-utils/stac-rs/compare/pgstac-v0.1.1..pgstac-v0.1.2
[0.1.1]: https://github.com/stac-utils/stac-rs/compare/pgstac-v0.1.0..pgstac-v0.1.1
[0.1.0]: https://github.com/stac-utils/stac-rs/releases/tag/pgstac-v0.1.0
[0.0.6]: https://github.com/stac-utils/pgstac-rs/compare/v0.0.5...v0.0.6
[0.0.5]: https://github.com/stac-utils/pgstac-rs/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/stac-utils/pgstac-rs/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/stac-utils/pgstac-rs/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/stac-utils/pgstac-rs/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/stac-utils/pgstac-rs/tree/v0.0.1

<!-- markdownlint-disable-file MD024 -->
