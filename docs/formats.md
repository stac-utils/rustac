# Formats

**rustac** "speaks" three forms of STAC:

- **JSON**: STAC is derived from [GeoJSON](https://geojson.org/)
- **Newline-delimited JSON (ndjson)**: One JSON [item](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md) per line, often used for bulk item loading and storage
- **stac-geoparquet**: A newer [specification](https://github.com/radiantearth/stac-geoparquet-spec) for storing STAC items, and optionally collections

We also have interfaces to other storage backends, e.g. Postgres via [pgstac](https://github.com/stac-utils/pgstac).
