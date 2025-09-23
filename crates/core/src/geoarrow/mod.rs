//! Convert between [ItemCollection] and [Table].

pub mod json;

use crate::{Error, ItemCollection, Result};
use arrow_array::{
    Array, RecordBatch, RecordBatchIterator, RecordBatchReader, builder::BinaryBuilder,
    cast::AsArray,
};
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

/// A **stac-geoarrow** table.
///
/// `Table` existed in **geoarrow** v0.3 but was removed in v0.4. We preserve it
/// here as a useful arrow-ish analog to [ItemCollection].
#[derive(Debug)]
pub struct Table {
    record_batches: Vec<RecordBatch>,
    schema: SchemaRef,
}

/// A builder for converting an [ItemCollection] to a [Table]
#[derive(Debug)]
pub struct TableBuilder {
    /// The item collection.
    pub item_collection: ItemCollection,

    /// Whether to drop invalid attributes.
    ///
    /// If false, an invalid attribute will cause an error. If true, an invalid
    /// attribute will trigger a warning.
    ///
    /// Invalid attributes are values in `properties` that would conflict with a STAC-defined top-level key.
    pub drop_invalid_attributes: bool,
}

impl TableBuilder {
    /// Builds a [Table].
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::geoarrow::TableBuilder;
    ///
    /// let item = stac::read("examples/simple-item.json").unwrap();
    /// let builder = TableBuilder {
    ///     item_collection: vec![item].into(),
    ///     drop_invalid_attributes: false,
    /// };
    /// let table = builder.build().unwrap();
    /// ```
    pub fn build(self) -> Result<Table> {
        let mut values = Vec::with_capacity(self.item_collection.items.len());
        let mut geometry_builder = GeometryBuilder::new(GeometryType::new(Default::default()));
        let mut proj_geometry_builder = BinaryBuilder::new();

        for item in self.item_collection.items {
            let mut value =
                serde_json::to_value(item.into_flat_item(self.drop_invalid_attributes)?)?;
            {
                let value = value
                    .as_object_mut()
                    .expect("a flat item should serialize to an object");
                if let Some(value) = value.remove("geometry") {
                    let geometry = geojson::Geometry::from_json_value(value).map_err(Box::new)?;
                    geometry_builder
                        .push_geometry(Some(&(Geometry::try_from(geometry).map_err(Box::new)?)))?;
                }
                if let Some(value) = value.remove("proj:geometry") {
                    let geometry = geojson::Geometry::from_json_value(value).map_err(Box::new)?;
                    let mut cursor = Cursor::new(Vec::new());
                    wkb::writer::write_geometry(
                        &mut cursor,
                        &Geometry::try_from(geometry).map_err(Box::new)?,
                        &Default::default(),
                    )?;
                    proj_geometry_builder.append_value(cursor.into_inner());
                }
                if let Some(bbox) = value.remove("bbox") {
                    let bbox = convert_bbox(bbox)?;
                    let _ = value.insert("bbox".to_string(), bbox);
                }
            }
            values.push(value);
        }

        // Create a geometry-less record batch of our items.
        // TODO do this in one pass: https://github.com/stac-utils/rustac/issues/767
        let schema = arrow_json::reader::infer_json_schema_from_iterator(values.iter().map(Ok))?;
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
        let schema = Arc::new(schema_builder.finish());
        let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder()?;
        decoder.serialize(&values)?;
        let record_batch = decoder.flush()?.ok_or(Error::NoItems)?;

        // Add the geometries back in.
        let mut schema_builder = SchemaBuilder::from(schema.fields());
        let mut columns = record_batch.columns().to_vec();
        let geometry_array = geometry_builder.finish();
        columns.push(geometry_array.to_array_ref());
        schema_builder.push(geometry_array.data_type().to_field("geometry", true));
        let proj_geometry_array = proj_geometry_builder.finish();
        if !proj_geometry_array.is_empty() {
            let data_type = proj_geometry_array.data_type().clone();
            columns.push(Arc::new(proj_geometry_array));
            schema_builder.push(Field::new("proj:geometry", data_type, true));
        }
        let _ = schema_builder
            .metadata_mut()
            .insert(VERSION_KEY.to_string(), VERSION.into());
        let schema = Arc::new(schema_builder.finish());
        let record_batch = RecordBatch::try_new(schema.clone(), columns)?;
        Ok(Table {
            record_batches: vec![record_batch],
            schema,
        })
    }
}

impl Table {
    /// Creates a [Table] from a vector of record batches and a schema.
    pub fn new(record_batches: Vec<RecordBatch>, schema: SchemaRef) -> Table {
        Table {
            record_batches,
            schema,
        }
    }

    /// Creates a [Table] from a [ItemCollection].
    ///
    /// Any invalid attributes in the items (e.g. top-level attributes that conflict
    /// with STAC spec attributes) will be dropped with a warning.
    ///
    /// For more control over the conversion, use a [TableBuilder].
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{ItemCollection, geoarrow::Table};
    ///
    /// let item = stac::read("examples/simple-item.json").unwrap();
    /// let item_collection = ItemCollection::from(vec![item]);
    /// let table = Table::from_item_collection(item_collection).unwrap();
    /// ```
    pub fn from_item_collection(item_collection: impl Into<ItemCollection>) -> Result<Table> {
        TableBuilder {
            item_collection: item_collection.into(),
            drop_invalid_attributes: true,
        }
        .build()
    }

    /// Returns this table's schema as a reference.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Converts this table into a [RecordBatchIterator].
    pub fn into_reader(self) -> impl RecordBatchReader {
        RecordBatchIterator::new(self.record_batches.into_iter().map(Ok), self.schema)
    }

    /// Converts this table into its record batches and schema.
    pub fn into_inner(self) -> (Vec<RecordBatch>, SchemaRef) {
        (self.record_batches, self.schema)
    }

    /// Returns the total number of records in this table.
    pub fn len(&self) -> usize {
        self.record_batches
            .iter()
            .map(|record_batch| record_batch.num_rows())
            .sum()
    }

    /// Returns true if this is an empty table.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Converts a [RecordBatchReader] to an [ItemCollection].
pub fn from_record_batch_reader<R: RecordBatchReader>(reader: R) -> Result<ItemCollection> {
    let item_collection = json::from_record_batch_reader(reader)?
        .into_iter()
        .map(|item| serde_json::from_value(Value::Object(item)).map_err(Error::from))
        .collect::<Result<Vec<_>>>()
        .map(ItemCollection::from)?;
    Ok(item_collection)
}

/// Converts a geometry column to geoarrow native type.
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
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::Table;
    use crate::{Item, ItemCollection};

    #[test]
    fn to_table() {
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let table = Table::from_item_collection(vec![item]).unwrap();
        assert_eq!(table.schema().metadata["stac:geoparquet_version"], "1.0.0");
    }

    #[test]
    fn has_type() {
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let table = Table::from_item_collection(vec![item]).unwrap();
        let _ = table.schema().field_with_name("type").unwrap();
    }

    #[test]
    fn roundtrip() {
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let table = Table::from_item_collection(vec![item]).unwrap();
        let _ = super::from_record_batch_reader(table.into_reader()).unwrap();
    }

    #[test]
    fn roundtrip_with_missing_asset() {
        let items: ItemCollection = crate::read("data/two-sentinel-2-items.json").unwrap();
        let table = Table::from_item_collection(items).unwrap();
        let _ = super::from_record_batch_reader(table.into_reader()).unwrap();
    }

    #[test]
    fn with_wkb_geometry() {
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let table = Table::from_item_collection(vec![item]).unwrap();
        let (mut record_batches, _) = table.into_inner();
        assert_eq!(record_batches.len(), 1);
        let record_batch = record_batches.pop().unwrap();
        let _ = super::with_wkb_geometry(record_batch, "geometry").unwrap();
    }

    #[test]
    fn has_proj_geometry() {
        let item: Item =
            crate::read("examples/extensions-collection/proj-example/proj-example.json").unwrap();
        let table = Table::from_item_collection(vec![item]).unwrap();
        let (mut record_batches, _) = table.into_inner();
        assert_eq!(record_batches.len(), 1);
        let record_batch = record_batches.pop().unwrap();
        assert!(
            record_batch
                .schema()
                .column_with_name("proj:geometry")
                .is_some()
        );
    }
}
