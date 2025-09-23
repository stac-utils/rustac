# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.1] - 2025-09-23

Bump dependencies.

## [0.8.0] - 2025-07-10

### Fixed

- Empty `type` field for `ItemCollection` ([#734](https://github.com/stac-utils/rustac/pull/734))

### Removed

- Python ([#679](https://github.com/stac-utils/rustac/pull/679))

## [0.7.1] - 2025-01-31

### Added

- kwargs to Python search build ([#591](https://github.com/stac-utils/rustac/pull/591))
- `client::search` function ([#607](https://github.com/stac-utils/rustac/pull/607))
- Better error reporting from `get` ([#670](https://github.com/stac-utils/rustac/pull/670))

## [0.7.0] - 2025-01-02

### Added

- Convenience methods on `Search` ([#562](https://github.com/stac-utils/rustac/pull/562), [#584](https://github.com/stac-utils/rustac/pull/584))

### Changed

- Many vectors on `Search` and `Items` are no longer `Options` ([#562](https://github.com/stac-utils/rustac/pull/562))

## [0.6.2] - 2024-11-12

### Added

- `cql2` conversions
- STAC API filter extension

## [0.6.1] - 2024-10-22

### Added

- `version` ([#476](https://github.com/stac-utils/rustac/pull/476))
- Set user agent when searching ([#487](https://github.com/stac-utils/rustac/pull/487))

## [0.6.0] - 2024-09-19

### Added

- `Client` (from now-defunkt **stac-async**) ([#372](https://github.com/stac-utils/rustac/pull/372))
- `BlockingClient` ([#387](https://github.com/stac-utils/rustac/pull/387))

### Changed

- `Collections` and `Items` now have private attributes ([#407](https://github.com/stac-utils/rustac/pull/407))

## [0.5.0] - 2024-09-05

### Added

- `From<Vec<Item>>` for `ItemCollection`, `From<Items>` for `Search`, `Direction` to public API ([#328](https://github.com/stac-utils/rustac/pull/328))

### Changed

- Use `stac::Bbox` for `Items`, renamed `Items::into_search` to `Items::search_collection` ([#328](https://github.com/stac-utils/rustac/pull/328))

## [0.4.1] - 2024-08-12

### Added

- `impl Default for Conformance` ([#252](https://github.com/stac-utils/rustac/pull/252))

## [0.4.0] - 2024-04-29

### Added

- `Conformance` builder functions ([#247](https://github.com/stac-utils/rustac/pull/247))
- Un-serialized pagination members to `ItemCollection` ([#247](https://github.com/stac-utils/rustac/pull/247))
- `stac::Fields` for `Search` and `Items` ([#247](https://github.com/stac-utils/rustac/pull/247))
- `Items::valid` and `Search::valid` ([#244](https://github.com/stac-utils/rustac/pull/244))

### Changed

- Consolidate duplicated fields between `Items` and `Search` ([#247](https://github.com/stac-utils/rustac/pull/247))

### Removed

- `schemars` feature ([#245](https://github.com/stac-utils/rustac/pull/245))
- `Search::validate` ([#244](https://github.com/stac-utils/rustac/pull/244))

## [0.3.3] - 2024-04-07

### Added

- `Search::validate` ([#206](https://github.com/stac-utils/rustac/pull/206))
- `geo` feature, `Search::matches` and sub-methods, `Search::new`, `Search::ids`, `Default` for `Filter`, `Error::Stac`, and `Error::Unimplemented` ([#209](https://github.com/stac-utils/rustac/pull/209))

## [0.3.2] - 2023-10-11

### Added

- `GetSearch` ([#198](https://github.com/stac-utils/rustac/pull/198))

## [0.3.1] - 2023-10-03

### Added

- Item search conformance URI ([#193](https://github.com/stac-utils/rustac/pull/193))

## [0.3.0] - 2023-09-25

### Added

- Conformance URIs ([#170](https://github.com/stac-utils/rustac/pull/170))
- `schemars` feature ([#177](https://github.com/stac-utils/rustac/pull/177))
- `PartialEq` to `Filter` ([#179](https://github.com/stac-utils/rustac/pull/179))
- `TryFrom` to go between `Items` and `GetItems` ([#179](https://github.com/stac-utils/rustac/pull/179))
- `Default` for `ItemCollection` ([#183](https://github.com/stac-utils/rustac/pull/183))

### Changed

- Don't serialize a missing context in an `ItemCollection` ([#170](https://github.com/stac-utils/rustac/pull/170))

### Fixed

- Strip plus sign from fields ([#179](https://github.com/stac-utils/rustac/pull/179))

### Removed

- `LinkBuilder` ([#170](https://github.com/stac-utils/rustac/pull/170))
- `Items.into_get_items` ([#179](https://github.com/stac-utils/rustac/pull/179))

## [0.2.0] - 2023-04-03

### Added

- `From<Vec<Collection>>` for `Collections` ([#124](https://github.com/stac-utils/rustac/pull/124))
- `UrlBuilder` ([#129](https://github.com/stac-utils/rustac/pull/129), [#130](https://github.com/stac-utils/rustac/pull/130))
- New `LinkBuilder` methods, including some renames ([#126](https://github.com/stac-utils/rustac/pull/126))
- `Links` for `Collections`, `ItemCollection` ([#126](https://github.com/stac-utils/rustac/pull/126))
- `Conformance` structure ([#126](https://github.com/stac-utils/rustac/pull/126))
- `Default` for `Search` ([#126](https://github.com/stac-utils/rustac/pull/126))
- `Clone` for `Search` and its sub structs ([#130](https://github.com/stac-utils/rustac/pull/130))
- `Display` for `Fields` and `Sortby` ([#133](https://github.com/stac-utils/rustac/pull/133))
- `Filter` as an externally-tagged enum ([#133](https://github.com/stac-utils/rustac/pull/133))
- `Items` and `GetItems` for paging items ([#133](https://github.com/stac-utils/rustac/pull/133))

### Changed

- `ItemCollection` now has a `items` attribute, instead of `features` ([#126](https://github.com/stac-utils/rustac/pull/126))
- `Item` is now just a type alias ([#130](https://github.com/stac-utils/rustac/pull/130))
- All `Search` fields are now optional ([#130](https://github.com/stac-utils/rustac/pull/130))

### Removed

- `Link` was removed, STAC API link attributes were added to `stac::Link` ([#126](https://github.com/stac-utils/rustac/pull/126))
- `Sortby::from_query_param` ([#133](https://github.com/stac-utils/rustac/pull/133))

## [0.1.0] - 2023-01-14

Initial release

[unreleased]: https://github.com/stac-utils/rustac/compare/stac-api-v0.8.1...main
[0.8.1]: https://github.com/stac-utils/rustac/compare/stac-api-v0.8.0...stac-api-v0.8.1
[0.8.0]: https://github.com/stac-utils/rustac/compare/stac-api-v0.7.1...stac-api-v0.8.0
[0.7.1]: https://github.com/stac-utils/rustac/compare/stac-api-v0.7.0...stac-api-v0.7.1
[0.7.0]: https://github.com/stac-utils/rustac/compare/stac-api-v0.6.2...stac-api-v0.7.0
[0.6.2]: https://github.com/stac-utils/rustac/compare/stac-api-v0.6.1...stac-api-v0.6.2
[0.6.1]: https://github.com/stac-utils/rustac/compare/stac-api-v0.6.0...stac-api-v0.6.1
[0.6.0]: https://github.com/stac-utils/rustac/compare/stac-api-v0.5.0...stac-api-v0.6.0
[0.5.0]: https://github.com/stac-utils/rustac/compare/stac-api-v0.4.1...stac-api-v0.5.0
[0.4.1]: https://github.com/stac-utils/rustac/compare/stac-api-v0.4.0...stac-api-v0.4.1
[0.4.0]: https://github.com/stac-utils/rustac/compare/stac-api-v0.3.3...stac-api-v0.4.0
[0.3.3]: https://github.com/stac-utils/rustac/compare/stac-api-v0.3.2...stac-api-v0.3.3
[0.3.2]: https://github.com/stac-utils/rustac/compare/stac-api-v0.3.1...stac-api-v0.3.2
[0.3.1]: https://github.com/stac-utils/rustac/compare/stac-api-v0.3.0...stac-api-v0.3.1
[0.3.0]: https://github.com/stac-utils/rustac/compare/stac-api-v0.2.0...stac-api-v0.3.0
[0.2.0]: https://github.com/stac-utils/rustac/compare/stac-api-v0.1.0...stac-api-v0.2.0
[0.1.0]: https://github.com/stac-utils/rustac/releases/tag/stac-api-v0.1.0

<!-- markdownlint-disable-file MD024 -->
