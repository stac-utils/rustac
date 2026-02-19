import * as duckdb from "@duckdb/duckdb-wasm";
import * as stac_wasm from "stac-wasm";

const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

// Select a bundle based on browser checks
const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

const worker_url = URL.createObjectURL(
  new Blob([`importScripts("${bundle.mainWorker}");`], {
    type: "text/javascript",
  }),
);

// Instantiate the asynchronous version of DuckDB-wasm
const worker = new Worker(worker_url);
const logger = new duckdb.ConsoleLogger();
const db = new duckdb.AsyncDuckDB(logger, worker);
await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
URL.revokeObjectURL(worker_url);

const connection = await db.connect();
const table = await connection.query("select 'an-id' as id");
console.log(stac_wasm.arrowToStacJson(table));

const bytes = stac_wasm.stacJsonToParquet([
  {
    stac_version: "1.1.0",
    stac_extensions: [],
    type: "Feature",
    id: "20201211_223832_CS2",
    bbox: [
      172.91173669923782, 1.3438851951615003, 172.95469614953714,
      1.3690476620161975,
    ],
    geometry: {
      type: "Polygon",
      coordinates: [
        [
          [172.91173669923782, 1.3438851951615003],
          [172.95469614953714, 1.3438851951615003],
          [172.95469614953714, 1.3690476620161975],
          [172.91173669923782, 1.3690476620161975],
          [172.91173669923782, 1.3438851951615003],
        ],
      ],
    },
    properties: {
      datetime: "2020-12-11T22:38:32.125000Z",
    },
    collection: "simple-collection",
    links: [
      {
        rel: "collection",
        href: "./collection.json",
        type: "application/json",
        title: "Simple Example Collection",
      },
      {
        rel: "root",
        href: "./collection.json",
        type: "application/json",
        title: "Simple Example Collection",
      },
      {
        rel: "parent",
        href: "./collection.json",
        type: "application/json",
        title: "Simple Example Collection",
      },
    ],
    assets: {
      visual: {
        href: "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2.tif",
        type: "image/tiff; application=geotiff; profile=cloud-optimized",
        title: "3-Band Visual",
        roles: ["visual"],
      },
      thumbnail: {
        href: "https://storage.googleapis.com/open-cogs/stac-examples/20201211_223832_CS2.jpg",
        title: "Thumbnail",
        type: "image/jpeg",
        roles: ["thumbnail"],
      },
    },
  },
]);
console.log(bytes);
const url = URL.createObjectURL(
  new Blob([bytes], { type: "application/vnd.apache.parquet" }),
);
const a = document.createElement("a");
a.href = url;
a.download = "items.parquet";
a.textContent = "download";
document.body.appendChild(a);

const response = await fetch("opr-one.parquet");
const parquetBuffer = new Uint8Array(await response.arrayBuffer());
await db.registerFileBuffer("opr-one.parquet", parquetBuffer);

await connection.query("INSTALL spatial");
await connection.query("LOAD spatial");

const oprTable = await connection.query(
  "SELECT * REPLACE ST_AsGeoJSON(geometry) as geometry FROM read_parquet('opr-one.parquet')",
);

try {
  const items = stac_wasm.arrowToStacJson(oprTable);
  console.log("opr items:", items);
} catch (e) {
  console.error("arrowToStacJson failed:", e);
}
