//! Convert between [ItemCollection] and [Table].

pub mod json;

use crate::{Error, ItemCollection, Result};
use arrow_array::{OffsetSizeTrait, RecordBatchIterator, RecordBatchReader};
use arrow_array::{RecordBatch, cast::AsArray};
use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, SchemaBuilder, SchemaRef, TimeUnit};
use geo_types::Geometry;
use geoarrow_array::array::{WkbArray, from_arrow_array};
use geoarrow_array::builder::{
    GeometryBuilder, GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder,
    MultiPointBuilder, MultiPolygonBuilder, PointBuilder, PolygonBuilder, WkbBuilder,
};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::error::GeoArrowError;
use geoarrow_array::{ArrayAccessor, GeoArrowArray, GeoArrowType};
use geoarrow_schema::{GeometryType, Metadata, WkbType};
use serde_json::{Value, json};
use std::sync::Arc;

const DATETIME_COLUMNS: [&str; 8] = [
    "datetime",
    "start_datetime",
    "end_datetime",
    "created",
    "updated",
    "expires",
    "published",
    "unpublished",
];

/// The stac-geoparquet version metadata key.
pub const VERSION_KEY: &str = "geoparquet:version";

/// The stac-geoparquet version.
pub const VERSION: &str = "1.0.0";

pub struct Table {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
}

impl Table {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn into_reader(self) -> impl RecordBatchReader {
        RecordBatchIterator::new(self.batches.into_iter().map(Ok), self.schema)
    }

    pub fn into_inner(self) -> (Vec<RecordBatch>, SchemaRef) {
        (self.batches, self.schema)
    }
}

/// Converts an [ItemCollection] to a [Table].
///
/// Any invalid attributes in the items (e.g. top-level attributes that conflict
/// with STAC spec attributes) will be dropped with a warning.
///
/// For more control over the conversion, use a [TableBuilder].
///
/// # Examples
///
/// ```
/// use stac::ItemCollection;
///
/// let item = stac::read("examples/simple-item.json").unwrap();
/// let item_collection = ItemCollection::from(vec![item]);
/// let table = stac::geoarrow::to_table(item_collection).unwrap();
/// ```
pub fn to_table(item_collection: impl Into<ItemCollection>) -> Result<Table> {
    TableBuilder {
        item_collection: item_collection.into(),
        drop_invalid_attributes: true,
    }
    .build()
}

/// A builder for converting an [ItemCollection] to a [Table]
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
#[derive(Debug)]
pub struct TableBuilder {
    /// The item collection.
    pub item_collection: ItemCollection,

    /// Whether to drop invalid attributes.
    ///
    /// If false, an invalid attribute will cause an error. If true, an invalid
    /// attribute will trigger a warning.
    pub drop_invalid_attributes: bool,
}

impl TableBuilder {
    /// Builds a [Table]
    pub fn build(self) -> Result<Table> {
        let mut values = Vec::with_capacity(self.item_collection.items.len());
        let mut builder = GeometryBuilder::new();
        for mut item in self.item_collection.items {
            builder.push_geometry(
                item.geometry
                    .take()
                    .and_then(|geometry| Geometry::try_from(geometry).ok())
                    .as_ref(),
            )?;
            let flat_item = item.into_flat_item(self.drop_invalid_attributes)?;
            let mut value = serde_json::to_value(flat_item)?;
            {
                let value = value
                    .as_object_mut()
                    .expect("a flat item should serialize to an object");
                let _ = value.remove("geometry");
                if let Some(bbox) = value.remove("bbox") {
                    let bbox = bbox
                        .as_array()
                        .expect("STAC items should always have a list as their bbox");
                    if bbox.len() == 4 {
                        let _ = value.insert("bbox".into(), json!({
                        "xmin": bbox[0].as_number().expect("all bbox values should be a number"),
                        "ymin": bbox[1].as_number().expect("all bbox values should be a number"),
                        "xmax": bbox[2].as_number().expect("all bbox values should be a number"),
                        "ymax": bbox[3].as_number().expect("all bbox values should be a number"),
                    }));
                    } else if bbox.len() == 6 {
                        let _ = value.insert("bbox".into(), json!({
                        "xmin": bbox[0].as_number().expect("all bbox values should be a number"),
                        "ymin": bbox[1].as_number().expect("all bbox values should be a number"),
                        "zmin": bbox[2].as_number().expect("all bbox values should be a number"),
                        "xmax": bbox[3].as_number().expect("all bbox values should be a number"),
                        "ymax": bbox[4].as_number().expect("all bbox values should be a number"),
                        "zmax": bbox[5].as_number().expect("all bbox values should be a number"),
                    }));
                    } else {
                        return Err(Error::InvalidBbox(
                            bbox.iter().filter_map(|v| v.as_f64()).collect(),
                        ));
                    }
                }
            }
            values.push(value);
        }
        let schema = arrow::json::reader::infer_json_schema_from_iterator(values.iter().map(Ok))?;
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
        let mut metadata = schema.metadata;
        let _ = metadata.insert(VERSION_KEY.to_string(), VERSION.into());
        let schema = Arc::new(schema_builder.finish().with_metadata(metadata));
        let mut decoder = ReaderBuilder::new(schema.clone()).build_decoder()?;
        decoder.serialize(&values)?;
        let batch = decoder.flush()?.ok_or(Error::NoItems)?;
        let array = builder.finish();
        Table::from_arrow_and_geometry(
            vec![batch],
            schema,
            geoarrow::chunked_array::ChunkedNativeArrayDyn::from_geoarrow_chunks(&[&array])?
                .into_inner(),
        )
        .map_err(Error::from)
    }
}

/// Converts a [RecordBatchReader] to an [ItemCollection].
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "geoparquet")]
/// # {
/// use std::fs::File;
/// use geoarrow::io::parquet::GeoParquetRecordBatchReaderBuilder;
///
/// let file = File::open("data/extended-item.parquet").unwrap();
/// let reader = GeoParquetRecordBatchReaderBuilder::try_new(file)
///     .unwrap()
///     .build()
///     .unwrap();
/// let table = reader.read_table().unwrap();
/// let item_collection = stac::geoarrow::from_table(table).unwrap();
/// # }
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
        let geometry_array = from_wkb(
            &wkb_array,
            GeoArrowType::Geometry(GeometryType::new(
                geoarrow_schema::CoordType::Interleaved,
                Metadata::default().into(),
            )),
            false,
        )?;
        let mut columns = record_batch.columns().to_vec();
        let mut schema_builder = SchemaBuilder::from(&*record_batch.schema());
        schema_builder.push(geometry_array.data_type().to_field("geometry", true));
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
        let wkb_array = to_wkb::<i32>(from_arrow_array(&geometry_column, field)?.as_ref())?;
        let mut columns = record_batch.columns().to_vec();
        let mut schema_builder = SchemaBuilder::from(&*record_batch.schema());
        schema_builder.push(wkb_array.data_type().to_field("geometry", true));
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

/// Parse a [WKBArray] to a GeometryArray with GeoArrow native encoding.
///
/// NOTE(Kyle): the refactored geoarrow-* crates don't yet have a public API for parsing WKB, so we
/// vendor code here for now. This comes from the internals of geoarrow-geoparquet:
/// https://github.com/geoarrow/geoarrow-rs/blob/0951163e3e8724a528c2f680a4158106197e7a32/rust/geoarrow-geoparquet/src/reader/parse.rs#L185-L244
///
/// This supports either ISO or EWKB-flavored data.
///
/// The returned array is guaranteed to have exactly the type of `target_type`.
///
/// `GeoArrowType::Rect` is currently not allowed.
fn from_wkb<O: OffsetSizeTrait>(
    arr: &WkbArray<O>,
    target_type: GeoArrowType,
    prefer_multi: bool,
) -> Result<Arc<dyn GeoArrowArray>> {
    use GeoArrowType::*;

    let geoms = arr
        .iter()
        .map(|x| x.transpose())
        .collect::<std::result::Result<Vec<Option<_>>, _>>()?;

    match target_type {
        Point(typ) => {
            let builder = PointBuilder::from_nullable_geometries(&geoms, typ)?;
            Ok(Arc::new(builder.finish()))
        }
        LineString(typ) => {
            let builder = LineStringBuilder::from_nullable_geometries(&geoms, typ)?;
            Ok(Arc::new(builder.finish()))
        }
        Polygon(typ) => {
            let builder = PolygonBuilder::from_nullable_geometries(&geoms, typ)?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPoint(typ) => {
            let builder = MultiPointBuilder::from_nullable_geometries(&geoms, typ)?;
            Ok(Arc::new(builder.finish()))
        }
        MultiLineString(typ) => {
            let builder = MultiLineStringBuilder::from_nullable_geometries(&geoms, typ)?;
            Ok(Arc::new(builder.finish()))
        }
        MultiPolygon(typ) => {
            let builder = MultiPolygonBuilder::from_nullable_geometries(&geoms, typ)?;
            Ok(Arc::new(builder.finish()))
        }
        GeometryCollection(typ) => {
            let builder =
                GeometryCollectionBuilder::from_nullable_geometries(&geoms, typ, prefer_multi)?;
            Ok(Arc::new(builder.finish()))
        }
        Rect(_) => {
            Err(GeoArrowError::General(format!("Unexpected data type {:?}", target_type,)).into())
        }
        Geometry(typ) => {
            let builder = GeometryBuilder::from_nullable_geometries(&geoms, typ, prefer_multi)?;
            Ok(Arc::new(builder.finish()))
        }
        _ => todo!("Handle target WKB/WKT in `from_wkb`"),
    }
}

/// Convert to WKB
fn to_wkb<O: OffsetSizeTrait>(arr: &dyn GeoArrowArray) -> Result<WkbArray<O>> {
    use GeoArrowType::*;
    match arr.data_type() {
        Point(_) => impl_to_wkb(arr.as_point()),
        LineString(_) => impl_to_wkb(arr.as_line_string()),
        Polygon(_) => impl_to_wkb(arr.as_polygon()),
        MultiPoint(_) => impl_to_wkb(arr.as_multi_point()),
        MultiLineString(_) => impl_to_wkb(arr.as_multi_line_string()),
        MultiPolygon(_) => impl_to_wkb(arr.as_multi_polygon()),
        Geometry(_) => impl_to_wkb(arr.as_geometry()),
        GeometryCollection(_) => impl_to_wkb(arr.as_geometry_collection()),
        Rect(_) => impl_to_wkb(arr.as_rect()),
        Wkb(_) => impl_to_wkb(arr.as_wkb::<i32>()),
        LargeWkb(_) => impl_to_wkb(arr.as_wkb::<i64>()),
        Wkt(_) => impl_to_wkb(arr.as_wkt::<i32>()),
        LargeWkt(_) => impl_to_wkb(arr.as_wkt::<i64>()),
    }
}

fn impl_to_wkb<'a, O: OffsetSizeTrait>(geo_arr: &'a impl ArrayAccessor<'a>) -> Result<WkbArray<O>> {
    let metadata = geo_arr.data_type().metadata().clone();

    let geoms = geo_arr
        .iter()
        .map(|x| x.transpose())
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let wkb_type = WkbType::new(metadata);
    Ok(WkbBuilder::from_nullable_geometries(geoms.as_slice(), wkb_type).finish())
}

// We only run tests when the geoparquet feature is enabled so that we don't
// have to add geoarrow as a dev dependency for all builds.
#[cfg(all(test, feature = "geoparquet"))]
mod tests {
    use crate::{Item, ItemCollection};
    use geoarrow_geoparquet::GeoParquetRecordBatchReaderBuilder;
    use std::fs::File;

    #[test]
    fn to_table() {
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let table = super::to_table(vec![item]).unwrap();
        assert_eq!(table.schema().metadata["geoparquet:version"], "1.0.0");
    }

    #[test]
    fn from_table() {
        let file = File::open("data/extended-item.parquet").unwrap();
        let reader = GeoParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let item_collection = super::from_record_batch_reader(reader).unwrap();
        assert_eq!(item_collection.items.len(), 1);
    }

    #[test]
    fn roundtrip() {
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let table = super::to_table(vec![item]).unwrap();
        let _ = super::from_record_batch_reader(table.into_reader()).unwrap();
    }

    #[test]
    fn roundtrip_with_missing_asset() {
        let items: ItemCollection = crate::read("data/two-sentinel-2-items.json").unwrap();
        let table = super::to_table(items).unwrap();
        let _ = super::from_record_batch_reader(table.into_reader()).unwrap();
    }

    #[test]
    fn with_wkb_geometry() {
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let table = super::to_table(vec![item]).unwrap();
        let (mut record_batches, _) = table.into_inner();
        assert_eq!(record_batches.len(), 1);
        let record_batch = record_batches.pop().unwrap();
        let _ = super::with_wkb_geometry(record_batch, "geometry").unwrap();
    }
}
