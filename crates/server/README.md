# stac-server

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/stac-utils/rustac/ci.yml?branch=main&style=for-the-badge)](https://github.com/stac-utils/rustac/actions/workflows/ci.yml)
[![docs.rs](https://img.shields.io/docsrs/stac-server?style=for-the-badge)](https://docs.rs/stac-server/latest/stac_server/)
[![Crates.io](https://img.shields.io/crates/v/stac-server?style=for-the-badge)](https://crates.io/crates/stac-server)
![Crates.io](https://img.shields.io/crates/l/stac-server?style=for-the-badge)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg?style=for-the-badge)](./CODE_OF_CONDUCT)

A [STAC API](https://github.com/radiantearth/stac-api-spec) server with multiple backends.

## Usage

To run a server from the command-line, use [rustac](../cli/README.md).
Any arguments will be interpreted as hrefs to STAC collections, items, and item collections, and will be loaded into the server on startup.

```shell
rustac serve collection.json items.json
```

To use the [pgstac](https://github.com/stac-utils/pgstac) backend:

```shell
rustac serve --pgstac postgresql://username:password@localhost:5432/postgis
```

If you'd like to serve your own **pgstac** backend with some sample items:

```shell
docker compose up -d pgstac
scripts/load-pgstac-fixtures  # This might take a while, e.g. 30 seconds or so
```

### Library

To use this library in another application:

```toml
[dependencies]
stac-server = "*"
```

### Deploying

There is currently no infrastructure-as-code for deploying **stac-server**.
We hope to provide this support in the future.

### Features

**stac-server** has two optional features.

#### axum

The `axum` feature enables routing and serving using [axum](https://github.com/tokio-rs/axum).

#### pgstac

In order to use the [pgstac](https://github.com/stac-utils/pgstac), you need to enable the `pgstac` feature.

## Backends

This table lists the provided backends and their supported conformance classes and extensions:

| Capability | Memory backend | Pgstac backend |
| -- | -- | -- |
| [STAC API - Core](https://github.com/radiantearth/stac-api-spec/blob/release/v1.0.0/core) | ✅ | ✅ |
| [STAC API - Features](https://github.com/radiantearth/stac-api-spec/blob/release/v1.0.0/ogcapi-features) | ✅ | ✅ |
| [STAC API - Item Search](https://github.com/radiantearth/stac-api-spec/blob/release/v1.0.0/item-search) | ✅ | ✅ |
| [Aggregation extension](https://github.com/stac-api-extensions/aggregation) | ✖️ | ✖️ |
| [Browseable extension](https://github.com/stac-api-extensions/browseable) | ✖️ | ✖️ |
| [Children extension](https://github.com/stac-api-extensions/children) | ✖️ | ✖️ |
| [Collection search extension](https://github.com/stac-api-extensions/collection-search) | ✖️ | ✖️ |
| [Collection transaction extension](https://github.com/stac-api-extensions/collection-transaction) | ✖️ | ✖️ |
| [Fields extension](https://github.com/stac-api-extensions/fields) | ✖️ | ✖️ |
| [Filter extension](https://github.com/stac-api-extensions/filter) | ✖️ | ✅️ |
| [Free-text search extension](https://github.com/stac-api-extensions/freetext-search) | ✖️ | ✖️ |
| [Language (I18N) extension](https://github.com/stac-api-extensions/language) | ✖️ | ✖️ |
| [Query extension](https://github.com/stac-api-extensions/query) | ✖️ | ✖️ |
| [Sort extension](https://github.com/stac-api-extensions/sort) | ✖️ | ✖️ |
| [Transaction extension](https://github.com/stac-api-extensions/transaction) | ✖️ | ✖️ |

## Other info

This crate is part of the [rustac](https://github.com/stac-utils/rustac) monorepo, see its README for contributing and license information.
