use arrow_array::RecordBatchReader;
use arrow_ipc::writer::StreamWriter;
use extendr_api::prelude::*;
use geoparquet::reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{fs::File, io::Cursor};

/// Reads a STAC value from an href and returns a JSON string.
///
/// The format is inferred from the file extension (e.g. `.json`, `.ndjson`).
/// URLs are also supported.
/// @export
#[extendr]
fn stac_read_json(path: &str) -> String {
    let value: stac::Value =
        stac_io::read(path).unwrap_or_else(|e| panic!("failed to read '{}': {}", path, e));
    serde_json::to_string(&value).unwrap_or_else(|e| panic!("failed to serialize to JSON: {}", e))
}

/// Writes a STAC value (as a JSON string) to a file path.
///
/// The output format is inferred from the file extension (e.g. `.json`, `.ndjson`).
/// @export
#[extendr]
fn stac_write_json(json: &str, path: &str) {
    let value: stac::Value =
        serde_json::from_str(json).unwrap_or_else(|e| panic!("failed to parse JSON: {}", e));
    stac_io::write(path, value).unwrap_or_else(|e| panic!("failed to write '{}': {}", path, e));
}

/// Reads a stac-geoparquet file and returns Arrow IPC stream bytes.
/// @export
#[extendr]
fn stac_read_geoparquet(path: &str) -> Raw {
    let file = File::open(path).unwrap_or_else(|e| panic!("failed to open '{}': {}", path, e));
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap_or_else(|e| panic!("failed to read parquet '{}': {}", path, e));
    let geoparquet_metadata = builder
        .geoparquet_metadata()
        .transpose()
        .unwrap_or_else(|e| panic!("failed to read geoparquet metadata: {}", e))
        .unwrap_or_else(|| panic!("missing geoparquet metadata in '{}'", path));
    let geoarrow_schema = builder
        .geoarrow_schema(&geoparquet_metadata, true, Default::default())
        .unwrap_or_else(|e| panic!("failed to build geoarrow schema: {}", e));
    let reader = builder
        .build()
        .unwrap_or_else(|e| panic!("failed to build parquet reader: {}", e));
    let reader = GeoParquetRecordBatchReader::try_new(reader, geoarrow_schema)
        .unwrap_or_else(|e| panic!("failed to create geoparquet reader: {}", e));

    let mut batches = Vec::new();
    let mut schema = reader.schema();
    for batch in reader {
        let batch = batch.unwrap_or_else(|e| panic!("failed to read record batch: {}", e));
        let batch = stac::geoarrow::with_wkb_geometry(batch, "geometry")
            .unwrap_or_else(|e| panic!("failed to convert geometry to WKB: {}", e));
        schema = batch.schema();
        batches.push(batch);
    }

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)
            .unwrap_or_else(|e| panic!("failed to create IPC writer: {}", e));
        for batch in &batches {
            writer
                .write(batch)
                .unwrap_or_else(|e| panic!("failed to write IPC batch: {}", e));
        }
        writer
            .finish()
            .unwrap_or_else(|e| panic!("failed to finish IPC stream: {}", e));
    }
    Raw::from_bytes(&buf)
}

/// Writes Arrow IPC stream bytes to a stac-geoparquet file.
/// @export
#[extendr]
fn stac_write_geoparquet(ipc_bytes: Raw, path: &str) {
    let cursor = Cursor::new(ipc_bytes.as_slice());
    let reader = arrow_ipc::reader::StreamReader::try_new(cursor, None)
        .unwrap_or_else(|e| panic!("failed to read IPC stream: {}", e));
    let item_collection = stac::geoarrow::from_record_batch_reader(reader)
        .unwrap_or_else(|e| panic!("failed to convert arrow to STAC: {}", e));
    stac::geoparquet::into_writer(
        File::create(path).unwrap_or_else(|e| panic!("failed to create '{}': {}", path, e)),
        item_collection,
    )
    .unwrap_or_else(|e| panic!("failed to write geoparquet '{}': {}", path, e));
}

extendr_module! {
    mod rustac;
    fn stac_read_json;
    fn stac_write_json;
    fn stac_read_geoparquet;
    fn stac_write_geoparquet;
}
