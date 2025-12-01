# Releasing

We use [release-please](https://github.com/googleapis/release-please) to manage versioning and creating Github releases.
Look for a release [pull request](https://github.com/stac-utils/rustac/pulls) to see what's queued up.
To release, simply merge that pull request, then:

```sh
cargo publish --workspace
```

You may need to `--exclude` or `--include` certain packages, depending on what's changed.
