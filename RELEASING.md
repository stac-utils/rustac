# Releasing

## Rust crates

We use [release-plz](https://release-plz.dev/) to manage versioning, changelogs, and publishing.
On every push to `main`, release-plz opens (or updates) a `chore: release` [pull request](https://github.com/stac-utils/rustac/pulls) with the pending version bumps and changelog entries.
To release, merge that pull request.
Release-plz then publishes the changed crates to crates.io, creates a git tag for each one, and creates the corresponding Github releases.

Per-crate configuration lives in [release-plz.toml](./release-plz.toml).

## stac-wasm

`stac-wasm` is **not** managed by release-plz (`release = false` in [release-plz.toml](./release-plz.toml)) and is not published to crates.io.
It is published to npm as [stac-wasm](https://www.npmjs.com/package/stac-wasm).

Release-plz can't manage it because it determines the previous version by looking the package up in the cargo registry, and `stac-wasm` isn't there.
Its `git_only` mode is the documented fix for unpublished packages, but it runs `cargo package --workspace`, which fails on `stac-wasm`'s git dependency on [arrow-wasm](https://github.com/kylebarron/arrow-wasm) (a git dependency needs a version requirement, and `arrow-wasm` isn't on crates.io).
See [release-plz#2651](https://github.com/release-plz/release-plz/issues/2651); once that's fixed we can move `stac-wasm` back under release-plz.

Until then, release it by hand:

1. Bump `version` in [crates/wasm/Cargo.toml](./crates/wasm/Cargo.toml) and add an entry to [crates/wasm/CHANGELOG.md](./crates/wasm/CHANGELOG.md), then merge that to `main`.
2. Create the tag and Github release, which triggers the [Release npm](./.github/workflows/release-npm.yml) workflow:

   ```sh
   gh release create stac-wasm-v$VERSION --title stac-wasm-v$VERSION --generate-notes
   ```

   The [Release npm](./.github/workflows/release-npm.yml) workflow also has a `workflow_dispatch` trigger, in case you need to re-run a publish without cutting a new release.
