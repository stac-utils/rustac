# stac

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/stac-utils/rustac/ci.yml?branch=main&style=for-the-badge)](https://github.com/stac-utils/rustac/actions/workflows/ci.yml)
[![docs.rs](https://img.shields.io/docsrs/stac?style=for-the-badge)](https://docs.rs/stac/latest/stac/)
[![Crates.io](https://img.shields.io/crates/v/stac?style=for-the-badge)](https://crates.io/crates/stac)
![Crates.io](https://img.shields.io/crates/l/stac?style=for-the-badge)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg?style=for-the-badge)](./CODE_OF_CONDUCT)

Rust implementation of the [SpatioTemporal Asset Catalog (STAC)](https://stacspec.org/) specification.

## Usage

To use the library in your project:

```toml
[dependencies]
stac = "0.13"
```

## Examples

```rust
use stac::Item;

// Creates an item from scratch.
let item = Item::new("an-id");

// Reads an item from the filesystem.
let item: Item = stac::read("examples/simple-item.json").unwrap();
```

Please see the [documentation](https://docs.rs/stac) for more usage examples.

## Other info

This crate is part of the [rustac](https://github.com/stac-utils/rustac) monorepo, see its README for contributing and license information.
