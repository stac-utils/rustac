//! Convert items to geoarrow record batches.

pub mod json;

use crate::{Error, Item, ItemCollection, Result};
use arrow_array::{Array, RecordBatch, RecordBatchReader, builder::BinaryBuilder, cast::AsArray};
use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, SchemaBuilder, SchemaRef, TimeUnit};
use geo_types::Geometry;
use geoarrow_array::{
    GeoArrowArray,
    array::{WkbArray, from_arrow_array},
    builder::GeometryBuilder,
};
use geoarrow_schema::{GeoArrowType, GeometryType, Metadata};
use serde_json::{Value, json};
use std::{io::Cursor, sync::Arc};

/// The stac-geoparquet version metadata key.
pub const VERSION_KEY: &str = "stac:geoparquet_version";

/// The stac-geoparquet version.
pub const VERSION: &str = "1.0.0";

/// Datetime columns.
pub const DATETIME_COLUMNS: [&str; 8] = [
    "datetime",
    "start_datetime",
    "end_datetime",
    "created",
    "updated",
    "expires",
    "published",
    "unpublished",
];

/// Encodes items into a record batch.
pub fn encode(items: Vec<Item>) -> Result<(RecordBatch, SchemaRef)> {
    encode_with_options(items, Options::default())
}

/// Encodes items into a record batch with options.
pub fn encode_with_options(items: Vec<Item>, options: Options) -> Result<(RecordBatch, SchemaRef)> {
    let (encoder, record_batch) = Encoder::new(items, options)?;
    Ok((record_batch, encoder.into_schema()))
}

/// A structure for encoding [Items](Item) into a [RecordBatch].
#[derive(Debug)]
pub struct Encoder {
    options: Options,
    base_schema: SchemaRef,
    schema: SchemaRef,
}

/// Options for encoding to arrow.
#[derive(Debug)]
pub struct Options {
    /// Whether to drop invalid attributes.
    ///
    /// If false, an invalid attribute will cause an error. If true, an invalid
    /// attribute will trigger a warning.
    ///
    /// Invalid attributes are values in `properties` that would conflict with a STAC-defined top-level key.
    pub drop_invalid_attributes: bool,
}

#[derive(Debug)]
struct Writer {
    values: Vec<Value>,
    geometry_builder: GeometryBuilder,
    proj_geometry_builder: BinaryBuilder,
}

impl Encoder {
    /// Creates a new encoder
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, geoarrow::{Encoder, Options}};
    /// use geojson::{Geometry, Value};
    ///
    /// let mut item = Item::new("an-id");
    /// item.geometry = Some(Geometry::new(Value::Point(vec![-105.1, 41.1])));
    /// let (encoder, record_batch) = Encoder::new(vec![item], Options::default()).unwrap();
    /// ```
    pub fn new(items: Vec<Item>, options: Options) -> Result<(Encoder, RecordBatch)> {
        let mut writer = Writer::new(items.len());
        for result in iter_items(items, options.drop_invalid_attributes) {
            writer.add(result?)?;
        }
        let base_schema = writer.infer_base_schema()?;
        let record_batch = writer.write(base_schema.clone())?;
        Ok((
            Encoder {
                options,
                base_schema,
                schema: record_batch.schema().clone(),
            },
            record_batch,
        ))
    }

    /// Encodes items into a record batch.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, geoarrow::{Encoder, Options}};
    /// use geojson::{Geometry, Value};
    ///
    /// let mut item = Item::new("an-id");
    /// item.geometry = Some(Geometry::new(Value::Point(vec![-105.1, 41.1])));
    /// let (encoder, _) = Encoder::new(vec![item.clone()], Options::default()).unwrap();
    /// let record_batch = encoder.encode(vec![item]).unwrap();
    /// ```
    pub fn encode(&self, items: Vec<Item>) -> Result<RecordBatch> {
        let mut writer = Writer::new(items.len());
        for result in iter_items(items, self.options.drop_invalid_attributes) {
            writer.add(result?)?;
        }
        let record_batch = writer.write(self.base_schema.clone())?;
        if record_batch.schema() != self.schema {
            Err(Error::ArrowSchemaMismatch)
        } else {
            Ok(record_batch)
        }
    }

    /// Consumes this encoder and returns its schema.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, geoarrow::{Encoder, Options}};
    /// use geojson::{Geometry, Value};
    ///
    /// let mut item = Item::new("an-id");
    /// item.geometry = Some(Geometry::new(Value::Point(vec![-105.1, 41.1])));
    /// let (encoder, _) = Encoder::new(vec![item], Options::default()).unwrap();
    /// let schema = encoder.into_schema();
    /// ```
    pub fn into_schema(self) -> SchemaRef {
        self.schema
    }
}

impl Writer {
    fn new(capacity: usize) -> Writer {
        Writer {
            values: Vec::with_capacity(capacity),
            geometry_builder: GeometryBuilder::new(GeometryType::new(Default::default())),
            proj_geometry_builder: BinaryBuilder::new(),
        }
    }

    fn add(&mut self, mut value: Value) -> Result<()> {
        let object = value
            .as_object_mut()
            .expect("a flat item should serialize to an object");
        if let Some(value) = object.remove("geometry") {
            let geometry = geojson::Geometry::from_json_value(value).map_err(Box::new)?;
            self.geometry_builder
                .push_geometry(Some(&(Geometry::try_from(geometry).map_err(Box::new)?)))?;
        }
        if let Some(value) = object.remove("proj:geometry") {
            let geometry = geojson::Geometry::from_json_value(value).map_err(Box::new)?;
            let mut cursor = Cursor::new(Vec::new());
            wkb::writer::write_geometry(
                &mut cursor,
                &Geometry::try_from(geometry).map_err(Box::new)?,
                &Default::default(),
            )?;
            self.proj_geometry_builder.append_value(cursor.into_inner());
        }
        if let Some(bbox) = object.remove("bbox") {
            let bbox = convert_bbox(bbox)?;
            let _ = object.insert("bbox".to_string(), bbox);
        }
        self.values.push(value);
        Ok(())
    }

    fn infer_base_schema(&self) -> Result<SchemaRef> {
        let schema =
            arrow_json::reader::infer_json_schema_from_iterator(self.values.iter().map(Ok))?;
        let mut schema_builder = SchemaBuilder::new();
        for field in schema.fields().iter() {
            if DATETIME_COLUMNS.contains(&field.name().as_str()) {
                schema_builder.push(Field::new(
                    field.name(),
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                    field.is_nullable(),
                ));
            } else {
                schema_builder.push(field.clone());
            }
        }
        Ok(Arc::new(schema_builder.finish()))
    }

    fn write(mut self, base_schema: SchemaRef) -> Result<RecordBatch> {
        let mut decoder = ReaderBuilder::new(base_schema.clone()).build_decoder()?;
        decoder.serialize(&self.values)?;
        let record_batch = decoder.flush()?.ok_or(Error::NoItems)?;
        let mut schema_builder = SchemaBuilder::from(base_schema.fields());
        let mut columns = record_batch.columns().to_vec();
        let geometry_array = self.geometry_builder.finish();
        columns.push(geometry_array.to_array_ref());
        schema_builder.push(geometry_array.data_type().to_field("geometry", true));
        let proj_geometry_array = self.proj_geometry_builder.finish();
        if !proj_geometry_array.is_empty() {
            let data_type = proj_geometry_array.data_type().clone();
            columns.push(Arc::new(proj_geometry_array));
            schema_builder.push(Field::new("proj:geometry", data_type, true));
        }
        let _ = schema_builder
            .metadata_mut()
            .insert(VERSION_KEY.to_string(), VERSION.into());
        let schema = Arc::new(schema_builder.finish());
        let record_batch = RecordBatch::try_new(schema, columns)?;
        Ok(record_batch)
    }
}

impl Default for Options {
    fn default() -> Self {
        Options {
            drop_invalid_attributes: true,
        }
    }
}

fn iter_items(
    items: Vec<Item>,
    drop_invalid_attributes: bool,
) -> impl Iterator<Item = Result<Value>> {
    items.into_iter().map(move |item| {
        item.into_flat_item(drop_invalid_attributes)
            .and_then(|flat_item| serde_json::to_value(flat_item).map_err(Error::from))
    })
}

/// Converts a [RecordBatchReader] to an [ItemCollection].
///
/// # Examples
///
/// ```
/// use stac::{Item, geoarrow};
/// use arrow_array::RecordBatchIterator;
/// use geojson::{Geometry, Value};
///
/// let mut item = Item::new("an-id");
/// item.geometry = Some(Geometry::new(Value::Point(vec![-105.1, 41.1])));
/// let (record_batch, schema) = geoarrow::encode(vec![item]).unwrap();
/// let reader = RecordBatchIterator::new(vec![record_batch].into_iter().map(Ok), schema);
/// let item_collection = geoarrow::from_record_batch_reader(reader).unwrap();
/// ```
pub fn from_record_batch_reader<R: RecordBatchReader>(reader: R) -> Result<ItemCollection> {
    let item_collection = json::from_record_batch_reader(reader)?
        .into_iter()
        .map(|item| serde_json::from_value(Value::Object(item)).map_err(Error::from))
        .collect::<Result<Vec<_>>>()
        .map(ItemCollection::from)?;
    Ok(item_collection)
}

/// Converts a geometry column to geoarrow native type.
///
/// # Examples
///
/// ```
/// use stac::{Item, geoarrow};
/// use geojson::{Geometry, Value};
///
/// let mut item = Item::new("an-id");
/// item.geometry = Some(Geometry::new(Value::Point(vec![-105.1, 41.1])));
/// let (record_batch, _) = geoarrow::encode(vec![item]).unwrap();
/// // First convert to WKB, then back to native
/// let record_batch = geoarrow::with_wkb_geometry(record_batch, "geometry").unwrap();
/// let record_batch = geoarrow::with_native_geometry(record_batch, "geometry").unwrap();
/// ```
pub fn with_native_geometry(
    mut record_batch: RecordBatch,
    column_name: &str,
) -> Result<RecordBatch> {
    if let Some((index, _)) = record_batch.schema().column_with_name(column_name) {
        let geometry_column = record_batch.remove_column(index);
        let wkb_array = WkbArray::new(
            geometry_column.as_binary::<i32>().clone(),
            Default::default(),
        );
        let geometry_array = geoarrow_array::cast::from_wkb(
            &wkb_array,
            GeoArrowType::Geometry(GeometryType::new(Metadata::default().into())),
        )?;
        let mut columns = record_batch.columns().to_vec();
        let mut schema_builder = SchemaBuilder::from(&*record_batch.schema());
        schema_builder.push(geometry_array.data_type().to_field(column_name, true));
        let schema = schema_builder.finish();
        columns.push(geometry_array.to_array_ref());
        record_batch = RecordBatch::try_new(schema.into(), columns)?;
    }
    Ok(record_batch)
}

/// Converts a geometry column to geoarrow.wkb.
///
/// # Examples
///
/// ```
/// use stac::{Item, geoarrow};
/// use geojson::{Geometry, Value};
///
/// let mut item = Item::new("an-id");
/// item.geometry = Some(Geometry::new(Value::Point(vec![-105.1, 41.1])));
/// let (record_batch, _) = geoarrow::encode(vec![item]).unwrap();
/// let record_batch = geoarrow::with_wkb_geometry(record_batch, "geometry").unwrap();
/// ```
pub fn with_wkb_geometry(mut record_batch: RecordBatch, column_name: &str) -> Result<RecordBatch> {
    if let Some((index, field)) = record_batch.schema().column_with_name(column_name) {
        let geometry_column = record_batch.remove_column(index);
        let wkb_array = geoarrow_array::cast::to_wkb::<i32>(
            from_arrow_array(&geometry_column, field)?.as_ref(),
        )?;
        let mut columns = record_batch.columns().to_vec();
        let mut schema_builder = SchemaBuilder::from(&*record_batch.schema());
        schema_builder.push(wkb_array.data_type().to_field(column_name, true));
        let schema = schema_builder.finish();
        columns.push(wkb_array.to_array_ref());
        record_batch = RecordBatch::try_new(schema.into(), columns)?;
    }
    Ok(record_batch)
}

/// Adds geoarrow wkb metadata to a geometry column.
///
/// # Examples
///
/// ```
/// use stac::{Item, geoarrow};
/// use geojson::{Geometry, Value};
///
/// let mut item = Item::new("an-id");
/// item.geometry = Some(Geometry::new(Value::Point(vec![-105.1, 41.1])));
/// let (record_batch, _) = geoarrow::encode(vec![item]).unwrap();
/// // First convert to WKB format
/// let record_batch = geoarrow::with_wkb_geometry(record_batch, "geometry").unwrap();
/// let record_batch = geoarrow::add_wkb_metadata(record_batch, "geometry").unwrap();
/// ```
pub fn add_wkb_metadata(mut record_batch: RecordBatch, column_name: &str) -> Result<RecordBatch> {
    if let Some((index, field)) = record_batch.schema().column_with_name(column_name) {
        let mut metadata = field.metadata().clone();
        let _ = metadata.insert(
            "ARROW:extension:name".to_string(),
            "geoarrow.wkb".to_string(),
        );
        let field = field.clone().with_metadata(metadata);
        let mut schema_builder = SchemaBuilder::from(&*record_batch.schema());
        let field_ref = schema_builder.field_mut(index);
        *field_ref = field.into();
        let schema = schema_builder.finish();
        record_batch = record_batch.with_schema(schema.into())?;
    }
    Ok(record_batch)
}

fn convert_bbox(bbox: Value) -> Result<Value> {
    let bbox = bbox
        .as_array()
        .expect("STAC items should always have a list as their bbox");
    if bbox.len() == 4 {
        Ok(json!({
            "xmin": bbox[0].as_number().expect("all bbox values should be a number"),
            "ymin": bbox[1].as_number().expect("all bbox values should be a number"),
            "xmax": bbox[2].as_number().expect("all bbox values should be a number"),
            "ymax": bbox[3].as_number().expect("all bbox values should be a number"),
        }))
    } else if bbox.len() == 6 {
        Ok(json!({
            "xmin": bbox[0].as_number().expect("all bbox values should be a number"),
            "ymin": bbox[1].as_number().expect("all bbox values should be a number"),
            "zmin": bbox[2].as_number().expect("all bbox values should be a number"),
            "xmax": bbox[3].as_number().expect("all bbox values should be a number"),
            "ymax": bbox[4].as_number().expect("all bbox values should be a number"),
            "zmax": bbox[5].as_number().expect("all bbox values should be a number"),
        }))
    } else {
        Err(Error::InvalidBbox(
            bbox.iter().filter_map(|v| v.as_f64()).collect(),
            "must have 4 or 6 values",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::Encoder;
    use crate::{Item, ItemCollection};
    use arrow_array::RecordBatchIterator;

    #[test]
    fn encode() {
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let (_, schema) = super::encode(vec![item]).unwrap();
        assert_eq!(schema.metadata["stac:geoparquet_version"], "1.0.0");
    }

    #[test]
    fn has_type() {
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let (_, schema) = super::encode(vec![item]).unwrap();
        let _ = schema.field_with_name("type").unwrap();
    }

    #[test]
    fn roundtrip() {
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let (record_batch, schema) = super::encode(vec![item]).unwrap();
        let _ = super::from_record_batch_reader(RecordBatchIterator::new(
            vec![record_batch].into_iter().map(Ok),
            schema,
        ))
        .unwrap();
    }

    #[test]
    fn roundtrip_with_missing_asset() {
        let item_collection: ItemCollection =
            crate::read("data/two-sentinel-2-items.json").unwrap();
        let (record_batch, schema) = super::encode(item_collection.items).unwrap();
        let _ = super::from_record_batch_reader(RecordBatchIterator::new(
            vec![record_batch].into_iter().map(Ok),
            schema,
        ))
        .unwrap();
    }

    #[test]
    fn with_wkb_geometry() {
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let (record_batch, _) = super::encode(vec![item]).unwrap();
        let _ = super::with_wkb_geometry(record_batch, "geometry").unwrap();
    }

    #[test]
    fn has_proj_geometry() {
        let item: Item =
            crate::read("examples/extensions-collection/proj-example/proj-example.json").unwrap();
        let (record_batch, _) = super::encode(vec![item]).unwrap();
        assert!(
            record_batch
                .schema()
                .column_with_name("proj:geometry")
                .is_some()
        );
    }

    #[test]
    fn two_batches() {
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let (encoder, _) = Encoder::new(vec![item.clone()], Default::default()).unwrap();
        let _ = encoder.encode(vec![item]).unwrap();
    }
}
