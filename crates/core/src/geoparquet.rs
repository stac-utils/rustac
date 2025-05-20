//! Read data from and write data in [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet/blob/main/spec/stac-geoparquet-spec.md).

use crate::{
    Catalog, Collection, Item, ItemCollection, Result, Value,
    geoarrow::{Table, VERSION, VERSION_KEY},
};
use bytes::Bytes;
use geoarrow_geoparquet::{GeoParquetRecordBatchReaderBuilder, GeoParquetWriterOptions};
use parquet::{
    file::{properties::WriterProperties, reader::ChunkReader},
    format::KeyValue,
};
use std::io::Write;

pub use parquet::basic::Compression;

/// Reads a [ItemCollection] from a [ChunkReader] as
/// [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet).
///
/// # Examples
///
/// ```
/// use std::fs::File;
///
/// let file = File::open("data/extended-item.parquet").unwrap();
/// let item_collection = stac::geoparquet::from_reader(file).unwrap();
/// ```
pub fn from_reader<R>(reader: R) -> Result<ItemCollection>
where
    R: ChunkReader + 'static,
{
    let reader = GeoParquetRecordBatchReaderBuilder::try_new(reader)?.build()?;
    crate::geoarrow::from_record_batch_reader(reader)
}

/// Writes a [ItemCollection] to a [std::io::Write] as
/// [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet).
///
/// Currently, will throw an error if the value is not an item or an item
/// collection.
///
/// # Examples
///
/// ```
/// use std::io::Cursor;
/// use stac::Item;
///
/// let item: Item = stac::read("examples/simple-item.json").unwrap();
/// let mut cursor = Cursor::new(Vec::new());
/// stac::geoparquet::into_writer(&mut cursor, vec![item]).unwrap();
/// ```
pub fn into_writer<W>(writer: W, item_collection: impl Into<ItemCollection>) -> Result<()>
where
    W: Write + Send,
{
    into_writer_with_options(writer, item_collection, &Default::default())
}

/// Writes a [ItemCollection] to a [std::io::Write] as
/// [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet) with the provided compression.
///
/// # Examples
///
/// ```
/// use std::io::Cursor;
/// use stac::{Item, geoparquet::Compression};
///
/// # #[cfg(feature = "geoparquet-compression")]
/// # {
/// let item: Item = stac::read("examples/simple-item.json").unwrap();
/// let mut cursor = Cursor::new(Vec::new());
/// stac::geoparquet::into_writer_with_compression(&mut cursor, vec![item], Compression::SNAPPY).unwrap();
/// # }
/// ```
pub fn into_writer_with_compression<W>(
    writer: W,
    item_collection: impl Into<ItemCollection>,
    compression: Compression,
) -> Result<()>
where
    W: Write + Send,
{
    let mut options = GeoParquetWriterOptions::default();
    let writer_properties = WriterProperties::builder()
        .set_compression(compression)
        .set_key_value_metadata(Some(vec![KeyValue {
            key: VERSION_KEY.to_string(),
            value: Some(VERSION.to_string()),
        }]))
        .build();
    options.writer_properties = Some(writer_properties);
    into_writer_with_options(writer, item_collection, &options)
}

/// Writes a [ItemCollection] to a [std::io::Write] as
/// [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet) with the provided options.
///
/// # Examples
///
/// ```
/// use std::io::Cursor;
/// use stac::{Item, geoparquet::Compression};
///
/// let item: Item = stac::read("examples/simple-item.json").unwrap();
/// let mut cursor = Cursor::new(Vec::new());
/// stac::geoparquet::into_writer_with_options(&mut cursor, vec![item], &Default::default()).unwrap();
/// ```
pub fn into_writer_with_options<W>(
    writer: W,
    item_collection: impl Into<ItemCollection>,
    options: &GeoParquetWriterOptions,
) -> Result<()>
where
    W: Write + Send,
{
    let table = Table::from_item_collection(item_collection)?;
    geoarrow_geoparquet::write_geoparquet(Box::new(table.into_reader()), writer, options)?;
    Ok(())
}
/// Create a STAC object from geoparquet data.
pub trait FromGeoparquet: Sized {
    /// Creates a STAC object from geoparquet bytes.
    #[allow(unused_variables)]
    fn from_geoparquet_bytes(bytes: impl Into<Bytes>) -> Result<Self>;
}

/// Write a STAC object to geoparquet.
pub trait IntoGeoparquet: Sized {
    /// Writes a value to a writer as stac-geoparquet.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac::{IntoGeoparquet, ItemCollection, Item};
    ///
    /// let item_collection: ItemCollection = vec![Item::new("a"), Item::new("b")].into();
    /// let mut buf = Vec::new();
    /// item_collection.into_geoparquet_writer(&mut buf, None).unwrap();
    /// ```
    fn into_geoparquet_writer(
        self,
        writer: impl Write + Send,
        compression: Option<Compression>,
    ) -> Result<()>;

    /// Writes a value to a writer as stac-geoparquet to some bytes.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac::{IntoGeoparquet, ItemCollection, Item};
    ///
    /// let item_collection: ItemCollection = vec![Item::new("a"), Item::new("b")].into();
    /// let bytes = item_collection.into_geoparquet_vec(None).unwrap();
    /// ```
    fn into_geoparquet_vec(self, compression: Option<Compression>) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.into_geoparquet_writer(&mut buf, compression)?;
        Ok(buf)
    }
}

macro_rules! impl_from_geoparquet {
    ($object:ty) => {
        impl FromGeoparquet for $object {
            fn from_geoparquet_bytes(
                _: impl Into<Bytes>,
            ) -> std::result::Result<Self, crate::Error> {
                Err(crate::Error::UnsupportedGeoparquetType)
            }
        }
    };
}
macro_rules! impl_into_geoparquet {
    ($object:ty) => {
        impl IntoGeoparquet for $object {
            fn into_geoparquet_writer(
                self,
                _: impl Write + Send,
                _: Option<Compression>,
            ) -> std::result::Result<(), crate::Error> {
                Err(crate::Error::UnsupportedGeoparquetType)
            }
        }
    };
}

impl_from_geoparquet!(Item);
impl_from_geoparquet!(Catalog);
impl_from_geoparquet!(Collection);
impl_into_geoparquet!(Catalog);
impl_into_geoparquet!(Collection);

impl FromGeoparquet for ItemCollection {
    fn from_geoparquet_bytes(bytes: impl Into<Bytes>) -> Result<Self> {
        let item_collection = from_reader(bytes.into())?;
        Ok(item_collection)
    }
}

impl FromGeoparquet for Value {
    fn from_geoparquet_bytes(bytes: impl Into<Bytes>) -> Result<Self> {
        Ok(Value::ItemCollection(
            ItemCollection::from_geoparquet_bytes(bytes)?,
        ))
    }
}

impl IntoGeoparquet for ItemCollection {
    fn into_geoparquet_writer(
        self,
        writer: impl Write + Send,
        compression: Option<Compression>,
    ) -> Result<()> {
        if let Some(compression) = compression {
            into_writer_with_compression(writer, self, compression)
        } else {
            into_writer(writer, self)
        }
    }
}

impl IntoGeoparquet for Item {
    fn into_geoparquet_writer(
        self,
        writer: impl Write + Send,
        compression: Option<Compression>,
    ) -> Result<()> {
        ItemCollection::from(vec![self]).into_geoparquet_writer(writer, compression)
    }
}

impl IntoGeoparquet for Value {
    fn into_geoparquet_writer(
        self,
        writer: impl Write + Send,
        compression: Option<Compression>,
    ) -> Result<()> {
        ItemCollection::try_from(self)?.into_geoparquet_writer(writer, compression)
    }
}

impl IntoGeoparquet for serde_json::Value {
    fn into_geoparquet_writer(
        self,
        writer: impl Write + Send,
        compression: Option<Compression>,
    ) -> Result<()> {
        let item_collection: ItemCollection = serde_json::from_value(self)?;
        item_collection.into_geoparquet_writer(writer, compression)
    }
}

#[cfg(test)]
mod tests {
    use crate::{FromGeoparquet, Item, ItemCollection, SelfHref, Value};
    use bytes::Bytes;
    use std::{
        fs::File,
        io::{Cursor, Read},
    };

    #[test]
    fn to_writer_item_collection() {
        let mut cursor = Cursor::new(Vec::new());
        let item = crate::read("examples/simple-item.json").unwrap();
        let item_collection = ItemCollection::from(vec![item]);
        super::into_writer(&mut cursor, item_collection).unwrap();
    }

    #[test]
    fn from_reader() {
        let file = File::open("data/extended-item.parquet").unwrap();
        let item_collection = super::from_reader(file).unwrap();
        assert_eq!(item_collection.items.len(), 1);
    }

    #[test]
    fn roundtrip() {
        let mut item: Item = crate::read("examples/simple-item.json").unwrap();
        item.clear_self_href();
        let mut cursor = Cursor::new(Vec::new());
        super::into_writer(&mut cursor, vec![item.clone()]).unwrap();
        let bytes = Bytes::from(cursor.into_inner());
        let item_collection = super::from_reader(bytes).unwrap();
        assert_eq!(item_collection.items[0], item);
    }

    #[test]
    fn roundtrip_proj_geometry() {
        let item_collection: ItemCollection = crate::read("data/multi-polygons.json").unwrap();
        let mut cursor = Cursor::new(Vec::new());
        super::into_writer(&mut cursor, item_collection).unwrap();
        let bytes = Bytes::from(cursor.into_inner());
        let item_collection = super::from_reader(bytes).unwrap();
        assert_eq!(item_collection.items.len(), 2);
    }

    #[test]
    fn from_bytes() {
        let mut buf = Vec::new();
        let _ = File::open("data/extended-item.parquet")
            .unwrap()
            .read_to_end(&mut buf)
            .unwrap();
        let _ = ItemCollection::from_geoparquet_bytes(buf).unwrap();
    }

    #[test]
    fn value_from_bytes() {
        let mut buf = Vec::new();
        let _ = File::open("data/extended-item.parquet")
            .unwrap()
            .read_to_end(&mut buf)
            .unwrap();
        let _ = Value::from_geoparquet_bytes(buf).unwrap();
    }

    #[test]
    fn multipolygon() {
        let items: ItemCollection = stac::read("data/multi-polygons.json").unwrap();
        let cursor = Cursor::new(Vec::new());
        super::into_writer(cursor, items).unwrap();
    }
}
