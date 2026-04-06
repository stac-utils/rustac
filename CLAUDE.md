# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**rustac** is a Rust monorepo providing a SpatioTemporal Asset Catalog (STAC) ecosystem: core data structures, I/O, validation, a CLI tool, a STAC API server, DuckDB-backed search, PostgreSQL (pgstac) support, and WebAssembly bindings.

## Build & Test Commands

### Building
```bash
cargo build                        # Default workspace members
cargo build --workspace            # All crates including pgstac, wasm
cargo build -F duckdb-bundled      # Bundle DuckDB from source (slower, no system lib needed)
```

### Testing
```bash
cargo test                         # All default workspace members
cargo test -p stac                 # Single crate
cargo test -p stac --all-features  # With all features enabled
cargo test -p stac -- test_name    # Single test by name
cargo test -p stac --lib           # Only lib tests (not integration tests)
```

### Linting & Formatting
```bash
prek run --all-files               # Run all formatters/linters (cargo fmt + clippy + markdownlint)
cargo clippy --workspace           # Clippy only
cargo fmt --all                    # Format only
```

### Documentation
```bash
cargo doc --workspace --no-deps    # Generate rustdoc
```

## Development Setup

**DuckDB** is required as a system library unless using `duckdb-bundled`:
- macOS (Homebrew): `export DUCKDB_LIB_DIR=/opt/homebrew/lib`
- Linux: Download from GitHub releases and set `DUCKDB_LIB_DIR` and `LD_LIBRARY_PATH`

**Python tools** (validation scripts use `uv`): `uv run scripts/validate-stac-server`

## Workspace Crates

| Crate | Path | Role |
|-------|------|------|
| `stac` | `crates/core` | Core STAC data structures (Item, Collection, Catalog) |
| `stac-io` | `crates/io` | Reading/writing JSON, NDJSON, geoparquet |
| `stac-extensions` | `crates/extensions` | STAC extension support |
| `stac-validate` | `crates/validate` | JSON schema validation |
| `stac-duckdb` | `crates/duckdb` | DuckDB-backed search over stac-geoparquet |
| `pgstac` | `crates/pgstac` | PostgreSQL pgstac bindings |
| `stac-server` | `crates/server` | Axum-based STAC API server |
| `rustac` | `crates/cli` | CLI (`rustac search/serve/translate/validate`) |
| `stac-wasm` | `crates/wasm` | WebAssembly bindings (Arrow ↔ STAC) |
| `stac-derive` | `crates/derive` | Internal derive macros (not published) |

Rust edition: 2024, MSRV: 1.88.

## Architecture

**Dependency flow**: `stac` (core) → `stac-io` → `stac-duckdb` / `pgstac` → `stac-server` → `rustac` (CLI)

**Server backends**: `stac-server` is backend-agnostic via the `Backend` trait. Concrete backends are in-memory (default), DuckDB (`stac-duckdb`), or PostgreSQL (`pgstac`). The CLI wires these together based on input type.

**Search client traits** (`bc98b7e`): `crates/core/src/client/` defines generic search client traits and adapters so multiple search backends can be used interchangeably.

**Format support**: The IO crate handles JSON, NDJSON, and geoparquet (via Arrow/geoarrow). The CLI's `translate` subcommand converts between these.

**Feature flags**: Most optional backends are gated behind feature flags (`duckdb`, `pgstac`, `geoparquet`, `geoarrow`, `async`, `geo`). The CLI enables most features by default.

## Testing Conventions

- Unit tests: inline in source files using `#[test]` / `#[rstest]`
- Integration tests: `crates/<name>/tests/` directories
- Test data: `crates/<name>/data/` and `crates/<name>/examples/`
- CLI tests use `assert_cmd` against the built binary
- pgstac tests require a live PostgreSQL+pgstac instance (CI spins one up)
- WASM tests are TypeScript in `crates/wasm/tests/`

## Conventional Commits

Follow [Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, etc. Subject line ≤50 chars, body wrapped at 72.
