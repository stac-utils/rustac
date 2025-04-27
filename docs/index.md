# rustac

![rustac logo](./img/rustac-small.png)

Welcome to the home of STAC and Rust.
We're happy you're here.

## What is rustac?

**rustac** is a [Github repository](https://github.com/stac-utils/rustac) that holds the code for several Rust [crates](https://doc.rust-lang.org/book/ch07-01-packages-and-crates.html) for creating, searching, and otherwise working with [STAC](https://stacspec.org).

!!! tip

    We pronounce **rustac** "ruh-stac"

!!! note

    Until 2025-04-17, this repository was named **stac-rs**.
    See [this RFC](https://github.com/stac-utils/rustac/issues/641) for context on the name change.

## What is rustac-py?

**rustac-py** is a Python [package](https://pypi.org/project/rustac/) that provides a simple API for interacting with STAC.
**rustac-py** uses the Rust code in **rustac** under the hood.

<!-- markdownlint-disable MD046 -->
```python
import rustac

items = rustac.search("s3://bucket/items.parquet", ...)
```
<!-- markdownlint-enable MD046 -->

Check out the [rustac-py docs](https://stac-utils.github.io/rustac-py) for more.

## Why are rustac and rustac-py in two separate repos?

Couple of reasons:

1. **rustac** is intended to be useful on its own.
   It's not just the engine for some Python bindings.
2. Care-and-feeding for Python wheels built from Rust is a bit finicky.
   By moving **rustac-py** to its own repo, we're able to separate the concerns of keeping a good, clean Rust core, and building Python wheels.
   Not everyone agrees with this strategy, but here we are.

## Rust documentation on docs.rs

- [stac](https://docs.rs/stac): The core Rust crate
- [stac-api](https://docs.rs/stac-api): Data structures for a STAC API, and a client for searching one
- [stac-server](https://docs.rs/stac-server): A STAC API server with multiple backends
- [pgstac](https://docs.rs/pgstac): Rust bindings for [pgstac](https://github.com/stac-utils/pgstac)

## Acknowledgements

We'd like to thank [@jkeifer](https://github.com/jkeifer), [@parksjr](https://github.com/parksjr), and Rob Gomez (all from @Element84) for creating the [rustac logo](./img/rustac.svg) from an AI-generated image from the prompt "a crab, a cabin, and a glass of whisky".
