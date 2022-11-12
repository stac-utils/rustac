# stac-rs

[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/gadomski/stac-rs/CI?style=for-the-badge)](https://github.com/gadomski/stac-rs/actions/workflows/ci.yml)
[![docs.rs](https://img.shields.io/docsrs/stac?style=for-the-badge)](https://docs.rs/stac/latest/stac/)
[![Crates.io](https://img.shields.io/crates/v/stac?style=for-the-badge)](https://crates.io/crates/stac)
[![Codecov](https://img.shields.io/codecov/c/github/gadomski/stac-rs?style=for-the-badge)](https://app.codecov.io/gh/gadomski/stac-rs/)
![Crates.io](https://img.shields.io/crates/l/stac?style=for-the-badge)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg?style=for-the-badge)](./CODE_OF_CONDUCT)

Rust implementation of the [SpatioTemporal Asset Catalog (STAC)](https://stacspec.org/) specification.

## Using the library

We are [**stac**](https://crates.io/crates/stac) on crates.io.
To use the library in your project:

```toml
[dependencies]
stac = "0.0.5"
```

### Features

There is one opt-in feature:  `reqwest`.
If you'd like to use the library with `reqwest` for blocking remote reads:

```toml
[dependencies]
stac = { version = "0.0.5", features = ["reqwest"]}
```

If `reqwest` is not enabled, `Reader::read` will throw an error if you try to read from a url.

## API

Please see the [documentation](https://docs.rs/stac/latest/stac/) for usage examples, and the [architecture diagram](./ARCHITECTURE.md) for a visual diagram of the key structures and traits.

## Examples

There is one example at [examples/copy.rs](./examples/copy.rs) that demonstrates a simple read-write operation.
To run it from the command line:

```shell
cargo run --example copy data/catalog.json tmp
```

## Incubator

We have an [incubator repository](https://github.com/gadomski/stac-rs-incubator) that holds related projects that aren't ready to be released as their own repositories.
These include (or are planned to include):

- async support
- command line interface
- STAC-API client
- STAC-API server

## Development

See [CONTRIBUTING.md](./CONTRIBUTING.md) for information about contributing to this project.
Use [RELEASING.md](./RELEASING.md) as an alternate pull request template when releasing a new version.

## License

**stac-rs** is dual-licensed under both the MIT license and the Apache license (Version 2.0).
See [LICENSE-APACHE](./LICENSE-APACHE) and [LICENSE-MIT](./LICENSE-MIT) for details.
