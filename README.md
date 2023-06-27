# stac-rs

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/gadomski/stac-rs/ci.yml?branch=main&style=for-the-badge)](https://github.com/gadomski/stac-rs/actions/workflows/ci.yml)
![Crates.io](https://img.shields.io/crates/l/stac?style=for-the-badge)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg?style=for-the-badge)](./CODE_OF_CONDUCT)

Rust implementation of the [SpatioTemporal Asset Catalog (STAC)](https://stacspec.org/) specification, spread over several crates.

<p align="center">
<img src="https://github.com/radiantearth/stac-site/raw/main/assets/images/STAC-01.png" height="100">
<img src="https://rustacean.net/assets/rustacean-orig-noshadow.svg" height=100>
</p>

| Crate | Description | |
| ----- | ---- | --------- |
| **stac** | Core data structures and synchronous I/O | [![README](https://img.shields.io/static/v1?label=README&message=stac&color=informational&style=flat-square)](./stac/README.md) <br> [![docs.rs](https://img.shields.io/docsrs/stac?style=flat-square)](https://docs.rs/stac/latest/stac/) <br> [![Crates.io](https://img.shields.io/crates/v/stac?style=flat-square)](https://crates.io/crates/stac) |
| **stac-validate** | Validate STAC data structures with [jsonschema](https://json-schema.org/) | [![README](https://img.shields.io/static/v1?label=README&message=stac-validate&color=informational&style=flat-square)](./stac-validate/README.md) <br> [![docs.rs](https://img.shields.io/docsrs/stac-validate?style=flat-square)](https://docs.rs/stac-validate/latest/stac-validate/) <br> [![Crates.io](https://img.shields.io/crates/v/stac-validate?style=flat-square)](https://crates.io/crates/stac-validate) |
| **stac-api** | Data structures for the [STAC API](https://github.com/radiantearth/stac-api-spec) specification | [![README](https://img.shields.io/static/v1?label=README&message=stac-api&color=informational&style=flat-square)](./stac-api/README.md) <br> [![docs.rs](https://img.shields.io/docsrs/stac-api?style=flat-square)](https://docs.rs/stac-api/latest/stac_api/) <br> [![Crates.io](https://img.shields.io/crates/v/stac-api?style=flat-square)](https://crates.io/crates/stac-api)
| **stac-async** | Asynchronous I/O with [tokio](https://tokio.rs/) | [![README](https://img.shields.io/static/v1?label=README&message=stac-async&color=informational&style=flat-square)](./stac-async/README.md) <br> [![docs.rs](https://img.shields.io/docsrs/stac-async?style=flat-square)](https://docs.rs/stac-async/latest/stac_async/) <br> [![Crates.io](https://img.shields.io/crates/v/stac-async?style=flat-square)](https://crates.io/crates/stac-async)
| **stac-cli** | Command line interface | [![README](https://img.shields.io/static/v1?label=README&message=stac-cli&color=informational&style=flat-square)](./stac-cli/README.md) <br> [![docs.rs](https://img.shields.io/docsrs/stac-cli?style=flat-square)](https://docs.rs/stac-cli/latest/stac_cli/) <br> [![Crates.io](https://img.shields.io/crates/v/stac-cli?style=flat-square)](https://crates.io/crates/stac-cli)

## Usage

To use our [command-line interface (CLI)](./stac-cli/README.md), first install Rust, e.g. with [rustup](https://rustup.rs/).
Then:

```shell
cargo install stac-cli
```

You can download assets from a STAC item:

```shell
stac download https://raw.githubusercontent.com/radiantearth/stac-spec/master/examples/simple-item.json .
```

To see a full list of available commands:

```shell
stac --help
```

The other crates in this repository are libraries — see their respective READMEs and documentation for details on their usage.

## Development

See [CONTRIBUTING.md](./CONTRIBUTING.md) for information about contributing to this project.
See [RELEASING.md](./RELEASING.md) for a checklist to use when releasing a new version.

## Ecosystem

Here's some related projects that use this repo:

- [pgstac-rs](https://github.com/gadomski/pgstac-rs): Rust interface for [pgstac](https://github.com/stac-utils/pgstac), PostgreSQL schema and functions for STAC
- [stac-server-rs](https://github.com/gadomski/stac-server-rs): A STAC API server implementation

## License

**stac-rs** is dual-licensed under both the MIT license and the Apache license (Version 2.0).
See [LICENSE-APACHE](./LICENSE-APACHE) and [LICENSE-MIT](./LICENSE-MIT) for details.

<!-- markdownlint-disable-file MD033 -->
