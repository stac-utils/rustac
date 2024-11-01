use super::{FromGeoparquet, IntoGeoparquet};
use crate::{Error, Item, ItemCollection, Result, Value};
use bytes::Bytes;
use geoarrow::io::parquet::{GeoParquetRecordBatchReaderBuilder, GeoParquetWriterOptions};
use parquet::{
    basic::Compression,
    file::{properties::WriterProperties, reader::ChunkReader},
};
use std::{fs::File, io::Write, path::Path};

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
    let table = reader.read_table()?;
    crate::geoarrow::from_table(table).map_err(Error::from)
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
/// use stac::Item;
/// use parquet::basic::Compression;
///
/// let item: Item = stac::read("examples/simple-item.json").unwrap();
/// let mut cursor = Cursor::new(Vec::new());
/// stac::geoparquet::into_writer_with_compression(&mut cursor, vec![item], Compression::SNAPPY).unwrap();
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
/// use stac::Item;
/// use parquet::basic::Compression;
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
    let table = crate::geoarrow::to_table(item_collection)?;
    geoarrow::io::parquet::write_geoparquet(table.into_record_batch_reader(), writer, options)
        .map_err(Error::from)
}

impl FromGeoparquet for ItemCollection {
    fn from_geoparquet_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = File::open(path)?;
        from_reader(file)
    }

    fn from_geoparquet_bytes(bytes: impl Into<Bytes>) -> Result<Self> {
        let item_collection = from_reader(bytes.into())?;
        Ok(item_collection)
    }
}

impl FromGeoparquet for Value {
    fn from_geoparquet_path(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Value::ItemCollection(ItemCollection::from_geoparquet_path(
            path,
        )?))
    }

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
        *item.self_href_mut() = None;
        let mut cursor = Cursor::new(Vec::new());
        super::into_writer(&mut cursor, vec![item.clone()]).unwrap();
        let bytes = Bytes::from(cursor.into_inner());
        let item_collection = super::from_reader(bytes).unwrap();
        assert_eq!(item_collection.items[0], item);
    }

    #[test]
    fn read() {
        let _ = ItemCollection::from_geoparquet_path("data/extended-item.parquet");
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
    fn read_value() {
        let _ = Value::from_geoparquet_path("data/extended-item.parquet").unwrap();
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
}
