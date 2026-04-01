# rustac - Agent Instructions

## Workspace map

- `crates/core` (`stac`): canonical types, API traits, shared logic.
- `crates/io`: HTTP and object-store I/O.
- `crates/duckdb`: DuckDB-backed querying.
- `crates/server`: API server backends and wiring.
- `crates/pgstac`: pgstac integration.
- `crates/extensions`, `crates/validate`, `crates/wasm`, `crates/derive`, `crates/cli`: supporting crates.

## Build and validation

```sh
cargo test
cargo test -p stac
prek run --all-files
```

DuckDB tests may require `DUCKDB_LIB_DIR`; alternatively use `--features duckdb-bundled` where supported.

## High-level rules

- Keep changes minimal and crate-local.
- Keep default builds lightweight; gate optional capabilities behind features.
- Preserve cross-crate consistency in naming, error shape, and docs style.

## Error handling

- One public error enum per crate in `src/error.rs` (typically `crate::Error`).
- Use `thiserror`, `#[non_exhaustive]`, and `#[error(transparent)]`/`#[from]` for wrappers.
- Document each error variant with a short doc comment.
- Do not introduce parallel error enums when a new variant on crate `Error` is sufficient.

## API and naming

- Use names that describe behavior (for traits/functions), not implementation detail.
- Avoid redundant suffixes/prefixes (`_generic`, duplicated context words).
- Keep trait families coherent (`Items*`, `Collections*`, streaming variants).
- Prefer explicit conversion adapters where coherence or ownership prevents a blanket impl.

## Features and dependencies

- Heavy or optional functionality must be feature-gated.
- Keep Arrow/GeoArrow/GeoParquet dependencies scoped to relevant features.
- Avoid widening default feature surfaces without strong need.
- Prefer workspace dependency versions and existing crate patterns.

## Async and memory behavior

- Treat `Stream` as the async equivalent of `Iterator`.
- Prefer streaming paths for large result sets.
- If buffering is required, document the reason and bound memory when practical.
- Be explicit about sync/async boundaries (for example, borrowed readers and blocking APIs).

## Documentation

- Library/API behavior belongs in Rust doc comments (`//!` and `///`).
- Use module-level docs for design overviews and trait relationships.
- Keep `docs/` focused on user-facing MkDocs content (CLI/history/site pages), not deep library internals.
- Keep examples realistic and compile-aware (`no_run` when needed).

## Testing

- Favor real implementations for behavior tests.
- Reserve mocks mainly for network/HTTP boundary simulation.
- Unit tests: colocated in `#[cfg(test)] mod tests`.
- Integration tests: crate `tests/` directories.
- Use `#[tokio::test]` for async code paths.

## Code style

- Remove stray/placeholder comments and dead code.
- Keep function docs and type docs short, factual, and behavior-focused.
- Match existing formatting and module organization.

## PR and git hygiene

- Use conventional commit style for PR titles.
- Run `prek run --all-files` before finalizing.
- Keep history clean; squash fixups when requested.
- Include tests and docs updates for behavior changes.
