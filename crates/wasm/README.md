# stac-wasm

Converts [Arrow](https://arrow.apache.org/) arrays to [SpatioTemporal Asset Catalog (STAC)](https://stacspec.org/) items, via [WebAssembly (WASM)](https://webassembly.org/).

> [!WARNING]
> This package is in an "alpha" state and will likely break and change a lot.

## Usage

```shell
npm i stac-wasm
```

We give you one function:

```javascript
import * as stac_wasm from "stac-wasm";

const table = loadArrowTable();  // e.g. from DuckDB
const items = stac_wasm.arrowToStacJson(table);
```

## Tests

We don't have automated tests.
If you want to play with the function, modify `www/index.js` and then:

```shell
cd www
npm run start
```

This should open a page at <http://localhost:8080/> that you can use to test out the WASM library.

## Contributing

**stac-wasm** is part of [rustac](https://github.com/stac-utils/rustac), a monorepo that includes the Rust code used to build the WASM module.
See [CONTRIBUTING.md](https://github.com/stac-utils/rustac/blob/main/CONTRIBUTING.md) for instructions on contributing to the monorepo.
