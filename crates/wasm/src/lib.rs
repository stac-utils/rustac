use arrow_array::RecordBatchIterator;
use arrow_schema::ArrowError;
use arrow_wasm::{Table, arrow_js::table::JSTable, error::WasmResult};
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
    let items = serde_wasm_bindgen::to_value(&items)?;
    Ok(items)
}
