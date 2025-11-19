# Contributing to **rustac**

First off, thanks for contributing!
We appreciates you.

## Testing

We aim for comprehensive unit testing of this library.
Please provide tests for any new features, or to demonstrate bugs.
Draft pull requests with a failing test to demonstrate a bug are much appreciated.

To run the tests (for the [default crates](./Cargo.toml#L16)):
```bash
cargo test
```

Some of the crates (e.g. `cli`) are not run by default. To run tests for a non-default crate, specify it with the `--package` flag:
```bash
cargo test --package rustac
```

To run the `rustac` CLI using your local changes:
```bash
cargo run --package rustac --help
```

## Development

### `libduckdb` configuration

> [!TIP]
> Set `DUCKDB_LIB_DIR` to the directory containing your **libduckdb**.
> If you're on macos and using [Homebrew](https://brew.sh/), this might be `export DUCKDB_LIB_DIR=/opt/homebrew/lib`
> On linux, you can download the `libduckdb-linux-{platform}.zip` file from the [latest release](https://github.com/duckdb/duckdb/releases/latest) and unzip the contents into a directory on your machine (you will also need to set `LD_LIBRARY_PATH` to include this directory).


## Submitting changes

Please open a [pull request](https://docs.github.com/en/pull-requests) with your changes -- make sure to include unit tests.
Please follow standard git commit formatting (subject line 50 characters max, wrap the body at 72 characters).
Run `scripts/lint` to make sure your changes are nice, and use `scripts/format` to fix things that can be fixed.

We use [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/).
Your commits do not have to but if you'd like to format them this way, we would be grateful.

If you can, use `git rebase -i` to create a clean, well-formatted history before opening your pull request.
If you need to make changes after opening your pull request (e.g. to fix CI breakages) we will be grateful if you squash those fixes into their relevant commits.

Thanks so much! \
-Pete Gadomski
