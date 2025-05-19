---
description: The rustac command-line interface (CLI)
---

# Command-line interface (CLI)

The **rustac** command-line interface can be installed two ways.
If you have Rust, use `cargo`:

```sh
cargo install rustac -F duckdb  # to use libduckdb on your system
# or
cargo install rustac -F duckdb-bundled  # to build libduckdb on install (slow)
```

The CLI is called **rustac**:

```shell
rustac --help
```

If you don't have DuckDB on your system, you can also use the Python wheel, which includes **libduckdb**:

```shell
python -m pip install rustac
```

For examples of using the CLI, check out the slides from [@gadomski's](https://github.com/gadomski/) [2024 FOSS4G-NA presentation](https://www.gadom.ski/2024-09-FOSS4G-NA-rustac/).
