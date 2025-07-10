# stac-api

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/stac-utils/rustac/ci.yml?branch=main&style=for-the-badge)](https://github.com/stac-utils/rustac/actions/workflows/ci.yml)
[![docs.rs](https://img.shields.io/docsrs/stac-api?style=for-the-badge)](https://docs.rs/stac-api/latest/stac_api/)
[![Crates.io](https://img.shields.io/crates/v/stac-api?style=for-the-badge)](https://crates.io/crates/stac-api)
![Crates.io](https://img.shields.io/crates/l/stac-api?style=for-the-badge)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg?style=for-the-badge)](./CODE_OF_CONDUCT)

Rust implementation of the data structures that make up the [STAC API specification](https://github.com/radiantearth/stac-api-spec).
This is **not** a server implementation.
For a STAC API server written in Rust, check out our [stac-server](../server/).

## Usage

To use the library in your project:

```toml
[dependencies]
stac-api = "0.8"
```

**stac-api** has one optional feature.
`geo` enables `Search::match`:

```toml
[dependencies]
stac-api = { version = "0.8", features = ["geo"] }
```

## Examples

```rust
use stac_api::{Root, Conformance, CORE_URI};
use stac::Catalog;

// Build the root (landing page) endpoint.
let root = Root {
    catalog: Catalog::new("an-id", "a description"),
    conformance: Conformance {
        conforms_to: vec![CORE_URI.to_string()],
    }
};
```

Please see the [documentation](https://docs.rs/stac-api) for more usage examples.

## Other info

This crate is part of the [rustac](https://github.com/stac-utils/rustac) monorepo, see its README for contributing and license information.
