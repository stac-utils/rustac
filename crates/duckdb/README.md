# stac-duckdb

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/stac-utils/rustac/ci.yml?branch=main&style=for-the-badge)](https://github.com/stac-utils/rustac/actions/workflows/ci.yml)
[![docs.rs](https://img.shields.io/docsrs/stac-duckdb?style=for-the-badge)](https://docs.rs/stac-duckdb/latest/stac_duckdb/)
[![Crates.io](https://img.shields.io/crates/v/stac-duckdb?style=for-the-badge)](https://crates.io/crates/stac-duckdb)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg?style=for-the-badge)](./CODE_OF_CONDUCT)

Use [DuckDB](https://duckdb.org/) to search [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet).

## Usage

```shell
cargo add stac-duckdb
```

See the [documentation](https://docs.rs/stac-duckdb) for more.

## Bundling

By default, DuckDB looks for its shared library on your system.
Use `DUCKDB_LIB_DIR` and `DUCKDB_INCLUDE_DIR` to help it find those resources.
If you want to build the DuckDB library as a part of this (or a downstream's) crate's build process, use the `bundled` feature.
E.g. to test this crate if you don't have DuckDB locally:

```shell
cargo test -p stac-duckdb -F bundled
```

See [the duckdb-rs docs](https://github.com/duckdb/duckdb-rs?tab=readme-ov-file#notes-on-building-duckdb-and-libduckdb-sys) for more.

## Other info

This crate is part of the [rustac](https://github.com/stac-utils/rustac) monorepo, see its README for contributing and license information.
