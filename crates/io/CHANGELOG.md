# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.5](https://github.com/stac-utils/rustac/compare/stac-io-v0.2.4...stac-io-v0.2.5) - 2026-02-12

### Fixed

- windows paths ([#955](https://github.com/stac-utils/rustac/pull/955))

## [0.2.4](https://github.com/stac-utils/rustac/compare/stac-io-v0.2.3...stac-io-v0.2.4) - 2026-02-03

### Added

- add search_with_headers ([#948](https://github.com/stac-utils/rustac/pull/948))
- add custom headers to search api requests ([#943](https://github.com/stac-utils/rustac/pull/943))

### Other

- bump msrv version ([#944](https://github.com/stac-utils/rustac/pull/944))

## [0.2.3](https://github.com/stac-utils/rustac/compare/stac-io-v0.2.2...stac-io-v0.2.3) - 2026-01-20

### Other

- update Cargo.toml dependencies

## [0.2.2](https://github.com/stac-utils/rustac/compare/stac-io-v0.2.1...stac-io-v0.2.2) - 2026-01-05

### Fixed

- references, not ownership, in geoparquet store writer ([#929](https://github.com/stac-utils/rustac/pull/929))
- properly write metadata in async geoparquet writer ([#928](https://github.com/stac-utils/rustac/pull/928))

## [0.2.1](https://github.com/stac-utils/rustac/compare/stac-io-v0.2.0...stac-io-v0.2.1) - 2025-12-15

### Other

- switch to release-plz ([#911](https://github.com/stac-utils/rustac/pull/911))
- update releasing to be much simpler ([#899](https://github.com/stac-utils/rustac/pull/899))

## [0.2.0](https://github.com/stac-utils/rustac/compare/stac-io-v0.1.2...stac-io-v0.2.0) (2025-12-01)


### ⚠ BREAKING CHANGES

* move stac_api crate into stac crate ([#869](https://github.com/stac-utils/rustac/issues/869))
* remove unused error enums ([#868](https://github.com/stac-utils/rustac/issues/868))
* move api client to stac-io crate ([#864](https://github.com/stac-utils/rustac/issues/864))

### Features

* add geoparquet writer encoder and object writing ([#863](https://github.com/stac-utils/rustac/issues/863)) ([ec6e7de](https://github.com/stac-utils/rustac/commit/ec6e7de6bf7c43cff11ba5d7dfd9f7c0654b2db1))
* specify max_row_group_size in geoparquet WriterBuilder ([#846](https://github.com/stac-utils/rustac/issues/846)) ([2bde538](https://github.com/stac-utils/rustac/commit/2bde538b41e5900b5be2d75587b1f8904520b3a1))


### Bug Fixes

* remove unused error enums ([#868](https://github.com/stac-utils/rustac/issues/868)) ([cf0e815](https://github.com/stac-utils/rustac/commit/cf0e815e03433e8ef219a79a67161174f3e99e84))


### Code Refactoring

* move api client to stac-io crate ([#864](https://github.com/stac-utils/rustac/issues/864)) ([e06de28](https://github.com/stac-utils/rustac/commit/e06de28787f9868f000ccc884979dcede1984f01)), closes [#764](https://github.com/stac-utils/rustac/issues/764)
* move stac_api crate into stac crate ([#869](https://github.com/stac-utils/rustac/issues/869)) ([d0f7405](https://github.com/stac-utils/rustac/commit/d0f7405a811dd2c3b044404b4a6a48cf07926a89))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * stac bumped from 0.14.0 to 0.15.0

## [0.1.2] - 2025-11-14

Update **stac** dependency.

## [0.1.1] - 2025-09-23

Bump dependencies.

## [0.1.0] - 2025-07-10

Initial release

[unreleased]: https://github.com/stac-utils/rustac/compare/stac-io-v0.1.2...main
[0.1.2]: https://github.com/stac-utils/rustac/compare/stac-io-v0.1.1...stac-io-v0.1.2
[0.1.1]: https://github.com/stac-utils/rustac/compare/stac-io-v0.1.0...stac-io-v0.1.1
[0.1.0]: https://github.com/stac-utils/rustac/releases/tag/stac-io-v0.1.0

<!-- markdownlint-disable-file MD024 -->
