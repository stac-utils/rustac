//! Read data from and write data in [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet/blob/main/spec/stac-geoparquet-spec.md).

use crate::{
    Catalog, Collection, Error, Item, ItemCollection, Result, Value,
    geoarrow::{Encoder, Options, VERSION, VERSION_KEY},
};
use bytes::Bytes;
use geoparquet::{
    reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader},
    writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptionsBuilder},
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::{properties::WriterProperties, reader::ChunkReader},
    format::KeyValue,
};
use std::io::Write;

pub use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};

/// Default stac-geoparquet compression
pub fn default_compression() -> Compression {
    Compression::ZSTD(ZstdLevel::try_new(15).unwrap())
}

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
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)?;
    let geoparquet_metadata = builder
        .geoparquet_metadata()
        .transpose()?
        .ok_or(Error::MissingGeoparquetMetadata)?;
    let geoarrow_schema =
        builder.geoarrow_schema(&geoparquet_metadata, true, Default::default())?;
    let reader = builder.build()?;
    let reader = GeoParquetRecordBatchReader::try_new(reader, geoarrow_schema)?;
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
    WriterBuilder::new(writer)
        .build(item_collection.into().items)?
        .finish()
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
/// let item: Item = stac::read("examples/simple-item.json").unwrap();
/// let mut cursor = Cursor::new(Vec::new());
/// stac::geoparquet::into_writer_with_compression(&mut cursor, vec![item], Compression::SNAPPY).unwrap();
/// ```
pub fn into_writer_with_compression<W>(
    writer: W,
    // TODO should we switch to just take a vector of items in the signature?
    item_collection: impl Into<ItemCollection>,
    compression: Compression,
) -> Result<()>
where
    W: Write + Send,
{
    WriterBuilder::new(writer)
        .compression(compression)
        .build(item_collection.into().items)
        .and_then(|mut writer| writer.finish())
}

/// Builder for a stac-geoparquet writer.
#[derive(Debug)]
pub struct WriterBuilder<W: Write + Send> {
    writer: W,
    options: Options,
    compression: Option<Compression>,
}

/// Write items to stac-geoparquet.
#[allow(missing_debug_implementations)]
pub struct Writer<W: Write + Send> {
    geoarrow_encoder: Encoder,
    // We make this an option so we can consume it during write but keep write
    // as only requiring a mutable reference.
    encoder: Option<GeoParquetRecordBatchEncoder>,
    arrow_writer: ArrowWriter<W>,
}

impl<W: Write + Send> WriterBuilder<W> {
    /// Creates a new writer builder.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io::Cursor;
    /// use stac::{Item, geoparquet::WriterBuilder};
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let cursor = Cursor::new(Vec::new());
    /// let writer = WriterBuilder::new(cursor).build(vec![item]).unwrap();
    /// ```
    pub fn new(writer: W) -> WriterBuilder<W> {
        WriterBuilder {
            writer,
            options: Options::default(),
            compression: Some(default_compression()),
        }
    }

    /// Sets the parquet compression.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io::Cursor;
    /// use stac::{Item, geoparquet::{WriterBuilder, Compression}};
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let cursor = Cursor::new(Vec::new());
    /// let writer = WriterBuilder::new(cursor)
    ///     .compression(Compression::SNAPPY)
    ///     .build(vec![item])
    ///     .unwrap();
    /// ```
    pub fn compression(mut self, compression: impl Into<Option<Compression>>) -> WriterBuilder<W> {
        self.compression = compression.into();
        self
    }

    /// Sets the geoarrow encoding options
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io::Cursor;
    /// use stac::{Item, geoarrow::Options, geoparquet::WriterBuilder};
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let cursor = Cursor::new(Vec::new());
    /// let options = Options::default();
    /// let writer = WriterBuilder::new(cursor)
    ///     .options(options)
    ///     .build(vec![item])
    ///     .unwrap();
    /// ```
    pub fn options(mut self, options: Options) -> WriterBuilder<W> {
        self.options = options;
        self
    }

    /// Builds the writer.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io::Cursor;
    /// use stac::{Item, geoparquet::WriterBuilder};
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let cursor = Cursor::new(Vec::new());
    /// let mut writer = WriterBuilder::new(cursor).build(vec![item]).unwrap();
    /// writer.finish().unwrap();
    /// ```
    pub fn build(self, items: Vec<Item>) -> Result<Writer<W>> {
        Writer::new(self.writer, self.options, self.compression, items)
    }
}

impl<W: Write + Send> Writer<W> {
    fn new(
        writer: W,
        options: Options,
        compression: Option<Compression>,
        items: Vec<Item>,
    ) -> Result<Self> {
        let (geoarrow_encoder, record_batch) = Encoder::new(items, options)?;
        let options = GeoParquetWriterOptionsBuilder::default()
            .set_primary_column("geometry".to_string())
            .build();
        let mut encoder = GeoParquetRecordBatchEncoder::try_new(&record_batch.schema(), &options)?;
        let mut builder = WriterProperties::builder();
        if let Some(compression) = compression {
            builder = builder.set_compression(compression);
        }
        let properties = builder.build();
        let mut arrow_writer =
            ArrowWriter::try_new(writer, encoder.target_schema(), Some(properties))?;
        let record_batch = encoder.encode_record_batch(&record_batch)?;
        arrow_writer.write(&record_batch)?;
        Ok(Writer {
            geoarrow_encoder,
            encoder: Some(encoder),
            arrow_writer,
        })
    }

    /// Writes more items to this writer.
    ///
    /// It's an error to write after `finish` has been called.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io::Cursor;
    /// use stac::{Item, geoparquet::WriterBuilder};
    ///
    /// let item1: Item = stac::read("examples/simple-item.json").unwrap();
    /// let item2 = item1.clone();
    /// let cursor = Cursor::new(Vec::new());
    /// let mut writer = WriterBuilder::new(cursor).build(vec![item1]).unwrap();
    /// writer.write(vec![item2]).unwrap();
    /// writer.finish().unwrap();
    /// ```
    pub fn write(&mut self, items: Vec<Item>) -> Result<()> {
        let record_batch = self.geoarrow_encoder.encode(items)?;
        let record_batch = if let Some(encoder) = self.encoder.as_mut() {
            encoder.encode_record_batch(&record_batch)?
        } else {
            return Err(Error::ClosedGeoparquetWriter);
        };
        self.arrow_writer.write(&record_batch)?;
        Ok(())
    }

    /// Finishes writing.
    ///
    /// It's an error to call finish twice.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io::Cursor;
    /// use stac::{Item, geoparquet::WriterBuilder};
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let cursor = Cursor::new(Vec::new());
    /// let mut writer = WriterBuilder::new(cursor).build(vec![item]).unwrap();
    /// writer.finish().unwrap();
    /// ```
    pub fn finish(&mut self) -> Result<()> {
        if let Some(encoder) = self.encoder.take() {
            self.arrow_writer
                .append_key_value_metadata(encoder.into_keyvalue()?);
        } else {
            return Err(Error::ClosedGeoparquetWriter);
        }
        self.arrow_writer.append_key_value_metadata(KeyValue::new(
            VERSION_KEY.to_string(),
            Some(VERSION.to_string()),
        ));
        let _ = self.arrow_writer.finish()?;
        Ok(())
    }
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
    use parquet::file::reader::{FileReader, SerializedFileReader};
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
    fn geometry_primary_column() {
        // https://github.com/stac-utils/rustac/issues/755
        let item_collection: ItemCollection = crate::read("data/multi-polygons.json").unwrap();
        let mut cursor = Cursor::new(Vec::new());
        super::into_writer(&mut cursor, item_collection).unwrap();
        let bytes = Bytes::from(cursor.into_inner());
        let reader = SerializedFileReader::new(bytes).unwrap();
        let key_value = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .find(|key_value| key_value.key == "geo")
            .unwrap();
        let value: serde_json::Value =
            serde_json::from_str(key_value.value.as_deref().unwrap()).unwrap();
        assert_eq!(value["primary_column"], "geometry");
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

    #[test]
    fn no_proj_geometry_metadata() {
        let item: Item =
            crate::read("examples/extensions-collection/proj-example/proj-example.json").unwrap();
        let mut cursor = Cursor::new(Vec::new());
        super::into_writer(&mut cursor, vec![item]).unwrap();
        let bytes = Bytes::from(cursor.into_inner());
        let reader = SerializedFileReader::new(bytes).unwrap();
        let key_value = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .find(|key_value| key_value.key == "geo")
            .unwrap();
        let value: serde_json::Value =
            serde_json::from_str(key_value.value.as_deref().unwrap()).unwrap();
        assert!(
            !value["columns"]
                .as_object()
                .unwrap()
                .contains_key("proj:geometry")
        );
    }
}
