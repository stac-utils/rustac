# rustac

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/stac-utils/rustac/ci.yml?branch=main&style=for-the-badge)](https://github.com/stac-utils/rustac/actions/workflows/ci.yml)
![Crates.io](https://img.shields.io/crates/l/stac?style=for-the-badge)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg?style=for-the-badge)](./CODE_OF_CONDUCT)

![rustac logo](./img/rustac-small.png)

The power of Rust for the STAC ecosystem

**rustac** is a monorepo of Rust crates for the [SpatioTemporal Asset Catalog (STAC)](https://stacspec.org/) specification.
We have a Python package derived from **rustac** in its own repository, [rustac-py](https://github.com/stac-utils/rustac-py).
For more on the difference between **rustac** and **rustac-py**, see [our docs](https://stac-utils.github.io/rustac/).

<!-- markdownlint-disable MD028 -->
> [!TIP]
> We pronounce **rustac** "ruh-stac".

> [!NOTE]
> Until 2025-04-17, this repository was named **stac-rs**.
> See [this RFC](https://github.com/stac-utils/rustac/issues/641) for context on the name change.
<!-- markdownlint-enable MD028 -->

## Formats

**rustac** "speaks" three forms of STAC:

- **JSON**: STAC is derived from [GeoJSON](https://geojson.org/)
- **Newline-delimited JSON (ndjson)**: One JSON [item](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md) per line, often used for bulk item loading and storage
- **stac-geoparquet**: A newer [specification](https://github.com/stac-utils/stac-geoparquet) for storing STAC items, and optionally collections

We also have interfaces to other storage backends, e.g. Postgres via [pgstac](https://github.com/stac-utils/pgstac).

## Command line interface

Our command line interface (CLI) can query STAC APIs, validate STAC, and more.
Install:

```shell
python -m pip install rustac
# or
cargo install rustac
```

Then:

```shell
# Search
$ rustac search https://landsatlook.usgs.gov/stac-server \
    --collections landsat-c2l2-sr \
    --intersects '{"type": "Point", "coordinates": [-105.119, 40.173]}' \
    --sortby='-properties.datetime' \
    --max-items 1000 \
    items.parquet

# Translate formats
$ rustac translate items.parquet items.ndjson
$ rustac translate items.ndjson items.json

# Migrate STAC versions
$ rustac translate item-v1.0.json item-v1.1.json --migrate

# Search stac-geoparquet (no API server required)
$ rustac search items.parquet

# Server
$ rustac serve items.parquet  # Opens a STAC API server on http://localhost:7822

# Validate
$ rustac validate item.json
```

## Crates

This monorepo contains several crates:

| Crate                                            | Description                                                                                     | Badges                                                                                                                                                                                                                                                                  |
| ------------------------------------------------ | ----------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [stac](./crates/core/README.md)                  | Core data structures                                                                  | [![docs.rs](https://img.shields.io/docsrs/stac?style=flat-square)](https://docs.rs/stac/latest/stac/) <br> [![Crates.io](https://img.shields.io/crates/v/stac?style=flat-square)](https://crates.io/crates/stac)                                                        |
| [stac-io](./crates/io/README.md)                  | Input and output                                                                    | [![docs.rs](https://img.shields.io/docsrs/stac-io?style=flat-square)](https://docs.rs/stac-io/latest/stac_io/) <br> [![Crates.io](https://img.shields.io/crates/v/stac-io?style=flat-square)](https://crates.io/crates/stac-io)                                                        |
| [stac-api](./crates/api/README.md)               | Data structures for the [STAC API](https://github.com/radiantearth/stac-api-spec) specification | [![docs.rs](https://img.shields.io/docsrs/stac-api?style=flat-square)](https://docs.rs/stac-api/latest/stac_api/) <br> [![Crates.io](https://img.shields.io/crates/v/stac-api?style=flat-square)](https://crates.io/crates/stac-api)                                    |
| [stac-api](./crates/api/README.md)               | Data structures for the [STAC API](https://github.com/radiantearth/stac-api-spec) specification | [![docs.rs](https://img.shields.io/docsrs/stac-api?style=flat-square)](https://docs.rs/stac-api/latest/stac_api/) <br> [![Crates.io](https://img.shields.io/crates/v/stac-api?style=flat-square)](https://crates.io/crates/stac-api)                                    |
| [stac-extensions](./crates/extensions/README.md) | Basic support for [STAC extensions](https://stac-extensions.github.io/)                         | [![docs.rs](https://img.shields.io/docsrs/stac-extensions?style=flat-square)](https://docs.rs/stac-extensions/latest/stac_extensions/) <br> [![Crates.io](https://img.shields.io/crates/v/stac-extensions?style=flat-square)](https://crates.io/crates/stac-extensions) |
| [rustac](./crates/cli/README.md)               | Command line interface                                                                          | [![docs.rs](https://img.shields.io/docsrs/rustac?style=flat-square)](https://docs.rs/rustac/latest/rustac/) <br> [![Crates.io](https://img.shields.io/crates/v/rustac?style=flat-square)](https://crates.io/crates/rustac)                                    |
| [stac-server](./crates/server/README.md)         | STAC API server with multiple backends                                                          | [![docs.rs](https://img.shields.io/docsrs/stac-server?style=flat-square)](https://docs.rs/stac-server/latest/stac_server/) <br> [![Crates.io](https://img.shields.io/crates/v/stac-server?style=flat-square)](https://crates.io/crates/stac-server)                     |
| [pgstac](./crates/pgstac/README.md)              | Bindings for [pgstac](https://github.com/stac-utils/pgstac)                                     | [![docs.rs](https://img.shields.io/docsrs/pgstac?style=flat-square)](https://docs.rs/pgstac/latest/pgstac/) <br> [![Crates.io](https://img.shields.io/crates/v/pgstac?style=flat-square)](https://crates.io/crates/pgstac)                                              |
| [stac-duckdb](./crates/duckdb/README.md)         | Experimental client for [duckdb](https://duckdb.org/)                                           | [![docs.rs](https://img.shields.io/docsrs/stac-duckdb?style=flat-square)](https://docs.rs/stac-duckdb/latest/stac_duckdb/) <br> [![Crates.io](https://img.shields.io/crates/v/stac-duckdb?style=flat-square)](https://crates.io/crates/stac-duckdb)                     |

### Dependency relationships

```mermaid
graph TD
    stac --> stac-api
    stac --> stac-io
    stac --> stac-extensions
    stac --> rustac
    stac --> pgstac
    stac --> stac-duckdb
    stac --> stac-server
    stac-io --> rustac
    stac-api --> pgstac
    stac-api --> stac-duckdb
    stac-api --> stac-server
    stac-api --> rustac
    pgstac --> stac-server
    stac-duckdb --> stac-server
    stac-server --> rustac
```

> [!NOTE]
> There's one additional crate in [crates](./crates/), [stac-derive](./crates/derive/).
> This is not published and is for internal use only.

## Development

See [CONTRIBUTING.md](./CONTRIBUTING.md) for information about contributing to this project.
See [RELEASING.md](./RELEASING.md) for a checklist to use when releasing a new version.

## Acknowledgements

We'd like to thank [@jkeifer](https://github.com/jkeifer), [@parksjr](https://github.com/parksjr), and [@Xenocide122](https://github.com/Xenocide122) (all from [@Element84](https://github.com/Element84)) for creating the [rustac logo](./img/rustac.svg) from an AI-generated image from this prompt:

> There is a library for working with STAC metadata that is written in rust called rustac: <https://github.com/stac-utils/rustac>. That name sounds like the word "rustic", and is meant to envoke (sic) an image of "a cabin and a glass of neat whisky".

## License

**rustac** is dual-licensed under both the MIT license and the Apache license (Version 2.0).
See [LICENSE-APACHE](./LICENSE-APACHE) and [LICENSE-MIT](./LICENSE-MIT) for details.

<!-- markdownlint-disable-file MD033 -->
