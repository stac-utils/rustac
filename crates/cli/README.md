# rustac

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/stac-utils/rustac/ci.yml?branch=main&style=for-the-badge)](https://github.com/stac-utils/rustac/actions/workflows/ci.yml)
[![docs.rs](https://img.shields.io/docsrs/rustac?style=for-the-badge)](https://docs.rs/rustac/latest/rustac/)
[![Crates.io](https://img.shields.io/crates/v/rustac?style=for-the-badge)](https://crates.io/crates/rustac)
![Crates.io](https://img.shields.io/crates/l/rustac?style=for-the-badge)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg?style=for-the-badge)](./CODE_OF_CONDUCT)

Command Line Interface (CLI) for [STAC](https://stacspec.org/), named `rustac`.

## Installation

If you have DuckDB on your system:

```sh
cargo install rustac
```

> [!TIP]
> Set `DUCKDB_LIB_DIR` to the directory containing your **libduckdb**.
> If you're on macos and using [Homebrew](https://brew.sh/), this might be `export DUCKDB_LIB_DIR=/opt/homebrew/lib`

Otherwise:

```sh
cargo install rustac -F duckdb-bundled  # (slow)
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
$ stac search items.parquet

# Server
$ rustac serve items.parquet  # Opens a STAC API server on http://localhost:7822

# Validate
$ rustac validate item.json
```

## Usage

**rustac** provides the following subcommands:

- `rustac search`: searches STAC APIs and, if the `duckdb` feature is enabled, geoparquet files
- `rustac serve`: serves a STAC API
- `rustac translate`: converts STAC from one format to another
- `rustac validate`: validates a STAC value

Use the `--help` flag to see all available options for the CLI and the subcommands:

## Other info

This crate is part of the [rustac](https://github.com/stac-utils/rustac) monorepo, see its README for contributing and license information.
