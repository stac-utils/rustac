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
