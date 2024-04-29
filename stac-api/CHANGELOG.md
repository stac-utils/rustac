# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2024-04-29

### Added

- `Conformance` builder functions ([#247](https://github.com/stac-utils/stac-rs/pull/247))
- Un-serialized pagination members to `ItemCollection` ([#247](https://github.com/stac-utils/stac-rs/pull/247))
- `stac::Fields` for `Search` and `Items` ([#247](https://github.com/stac-utils/stac-rs/pull/247))
- `Items::valid` and `Search::valid` ([#244](https://github.com/stac-utils/stac-rs/pull/244))

### Changed

- Consolidate duplicated fields between `Items` and `Search` ([#247](https://github.com/stac-utils/stac-rs/pull/247))

### Removed

- `schemars` feature ([#245](https://github.com/stac-utils/stac-rs/pull/245))
- `Search::validate` ([#244](https://github.com/stac-utils/stac-rs/pull/244))

## [0.3.3] - 2024-04-07

### Added

- `Search::validate` ([#206](https://github.com/stac-utils/stac-rs/pull/206))
- `geo` feature, `Search::matches` and sub-methods, `Search::new`, `Search::ids`, `Default` for `Filter`, `Error::Stac`, and `Error::Unimplemented` ([#209](https://github.com/stac-utils/stac-rs/pull/209))

## [0.3.2] - 2023-10-11

### Added

- `GetSearch` ([#198](https://github.com/stac-utils/stac-rs/pull/198))

## [0.3.1] - 2023-10-03

### Added

- Item search conformance URI ([#193](https://github.com/stac-utils/stac-rs/pull/193))

## [0.3.0] - 2023-09-25

### Added

- Conformance URIs ([#170](https://github.com/stac-utils/stac-rs/pull/170))
- `schemars` feature ([#177](https://github.com/stac-utils/stac-rs/pull/177))
- `PartialEq` to `Filter` ([#179](https://github.com/stac-utils/stac-rs/pull/179))
- `TryFrom` to go between `Items` and `GetItems` ([#179](https://github.com/stac-utils/stac-rs/pull/179))
- `Default` for `ItemCollection` ([#183](https://github.com/stac-utils/stac-rs/pull/183))

### Changed

- Don't serialize a missing context in an `ItemCollection` ([#170](https://github.com/stac-utils/stac-rs/pull/170))

### Fixed

- Strip plus sign from fields ([#179](https://github.com/stac-utils/stac-rs/pull/179))

### Removed

- `LinkBuilder` ([#170](https://github.com/stac-utils/stac-rs/pull/170))
- `Items.into_get_items` ([#179](https://github.com/stac-utils/stac-rs/pull/179))

## [0.2.0] - 2023-04-03

### Added

- `From<Vec<Collection>>` for `Collections` ([#124](https://github.com/stac-utils/stac-rs/pull/124))
- `UrlBuilder` ([#129](https://github.com/stac-utils/stac-rs/pull/129), [#130](https://github.com/stac-utils/stac-rs/pull/130))
- New `LinkBuilder` methods, including some renames ([#126](https://github.com/stac-utils/stac-rs/pull/126))
- `Links` for `Collections`, `ItemCollection` ([#126](https://github.com/stac-utils/stac-rs/pull/126))
- `Conformance` structure ([#126](https://github.com/stac-utils/stac-rs/pull/126))
- `Default` for `Search` ([#126](https://github.com/stac-utils/stac-rs/pull/126))
- `Clone` for `Search` and its sub structs ([#130](https://github.com/stac-utils/stac-rs/pull/130))
- `Display` for `Fields` and `Sortby` ([#133](https://github.com/stac-utils/stac-rs/pull/133))
- `Filter` as an externally-tagged enum ([#133](https://github.com/stac-utils/stac-rs/pull/133))
- `Items` and `GetItems` for paging items ([#133](https://github.com/stac-utils/stac-rs/pull/133))

### Changed

- `ItemCollection` now has a `items` attribute, instead of `features` ([#126](https://github.com/stac-utils/stac-rs/pull/126))
- `Item` is now just a type alias ([#130](https://github.com/stac-utils/stac-rs/pull/130))
- All `Search` fields are now optional ([#130](https://github.com/stac-utils/stac-rs/pull/130))

### Removed

- `Link` was removed, STAC API link attributes were added to `stac::Link` ([#126](https://github.com/stac-utils/stac-rs/pull/126))
- `Sortby::from_query_param` ([#133](https://github.com/stac-utils/stac-rs/pull/133))

## [0.1.0] - 2023-01-14

Initial release

[unreleased]: https://github.com/stac-utils/stac-rs/compare/stac-api-v0.4.0...main
[0.4.0]: https://github.com/stac-utils/stac-rs/compare/stac-api-v0.3.3...stac-api-v0.4.0
[0.3.3]: https://github.com/stac-utils/stac-rs/compare/stac-api-v0.3.2...stac-api-v0.3.3
[0.3.2]: https://github.com/stac-utils/stac-rs/compare/stac-api-v0.3.1...stac-api-v0.3.2
[0.3.1]: https://github.com/stac-utils/stac-rs/compare/stac-api-v0.3.0...stac-api-v0.3.1
[0.3.0]: https://github.com/stac-utils/stac-rs/compare/stac-api-v0.2.0...stac-api-v0.3.0
[0.2.0]: https://github.com/stac-utils/stac-rs/compare/stac-api-v0.1.0...stac-api-v0.2.0
[0.1.0]: https://github.com/stac-utils/stac-rs/releases/tag/stac-api-v0.1.0

<!-- markdownlint-disable-file MD024 -->
