import { expect, test } from "vitest";
import { arrowToStacJson, stacJsonToParquet } from "stac-wasm";
import * as duckdb from "@duckdb/duckdb-wasm";

test("stacJsonToParquet converts a STAC item to parquet bytes", () => {
  const items = [
    {
      type: "Feature",
      stac_version: "1.1.0",
      id: "test-item",
      geometry: {
        type: "Point",
        coordinates: [0, 0],
      },
      bbox: [0, 0, 0, 0],
      properties: {
        datetime: "2024-01-01T00:00:00Z",
      },
      links: [],
      assets: {},
    },
  ];

  const bytes = stacJsonToParquet(items);
  expect(bytes).toBeInstanceOf(Uint8Array);
  expect(bytes.length).toBeGreaterThan(0);

  // Parquet magic bytes: "PAR1"
  expect(bytes[0]).toBe(0x50); // P
  expect(bytes[1]).toBe(0x41); // A
  expect(bytes[2]).toBe(0x52); // R
  expect(bytes[3]).toBe(0x31); // 1
});

async function createDuckDB(): Promise<duckdb.AsyncDuckDB> {
  const bundles = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(bundles);
  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], {
      type: "text/javascript",
    }),
  );
  const worker = new Worker(worker_url);
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.INFO);
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(worker_url);
  return db;
}

test("arrowToStacJson with empty links from DuckDB (issue #959)", async () => {
  const db = await createDuckDB();
  const connection = await db.connect();

  const response = await fetch("/opr-one.parquet");
  const buffer = new Uint8Array(await response.arrayBuffer());
  await db.registerFileBuffer("opr-one.parquet", buffer);

  await connection.query("INSTALL spatial");
  await connection.query("LOAD spatial");
  const table = await connection.query(
    "SELECT links FROM read_parquet('opr-one.parquet')",
  );
  const items = arrowToStacJson(table);

  expect(items).toHaveLength(1);
  expect(items[0].id).toBe("Data_20160517_04_001");

  await connection.close();
  await db.terminate();
}, 30000);
