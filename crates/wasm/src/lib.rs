use arrow_array::RecordBatchIterator;
use arrow_schema::ArrowError;
use arrow_wasm::{Table, arrow_js::table::JSTable, error::WasmResult};
use serde::Serialize;
use serde_wasm_bindgen::Serializer;
use stac::Item;
use std::io::Cursor;
use thiserror::Error;
use wasm_bindgen::prelude::*;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}

#[wasm_bindgen(js_name = arrowToStacJson)]
pub fn arrow_to_stac_json(table: JSTable) -> WasmResult<JsValue> {
    let table = Table::from_js(&table)?;
    let reader = RecordBatchIterator::new(
        table.record_batches().into_iter().map(From::from).map(Ok),
        table.schema().into(),
    );
    let items = stac::geoarrow::json::from_record_batch_reader(reader)?;
    let serializer = Serializer::json_compatible();
    let items = items.serialize(&serializer)?;
    Ok(items)
}

#[wasm_bindgen(js_name = stacJsonToParquet)]
pub fn stac_json_to_parquet(value: JsValue) -> Result<Vec<u8>, JsError> {
    let items: Vec<Item> = serde_wasm_bindgen::from_value(value)?;
    let mut cursor = Cursor::new(Vec::new());
    stac::geoparquet::into_writer(&mut cursor, items)?;
    Ok(cursor.into_inner())
}
