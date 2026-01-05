//! Read data from and write data in [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet/blob/main/spec/stac-geoparquet-spec.md).

use crate::{
    Catalog, Collection, Error, Item, ItemCollection, Result, Value,
    geoarrow::{Encoder, Options},
};
use arrow_array::RecordBatch;
use bytes::Bytes;
use geoparquet::{
    reader::{GeoParquetReaderBuilder, GeoParquetRecordBatchReader},
    writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptionsBuilder},
};
pub use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::{properties::WriterProperties, reader::ChunkReader},
    format::KeyValue,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, io::Write};

/// Default stac-geoparquet compression
pub fn default_compression() -> Compression {
    Compression::ZSTD(ZstdLevel::try_new(15).unwrap())
}

/// Default stac-geoparquet max row group size
pub const DEFAULT_STAC_MAX_ROW_GROUP_SIZE: usize = 150_000;

/// The stac-geoparquet metadata key.
pub const METADATA_KEY: &str = "stac-geoparquet";

/// The stac-geoparquet version.
pub const VERSION: &str = "1.0.0";

/// Options for writing stac-geoparquet files.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct WriterOptions {
    /// Parquet compression codec
    pub compression: Option<Compression>,

    /// Maximum number of rows in a row group
    pub max_row_group_size: usize,
}

/// An encoder for writing stac-geoparquet
#[allow(missing_debug_implementations)]
pub struct WriterEncoder {
    geoarrow_encoder: Encoder,
    encoder: GeoParquetRecordBatchEncoder,
}

impl WriterOptions {
    /// Creates a new WriterOptions with default values.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::geoparquet::WriterOptions;
    ///
    /// let options = WriterOptions::new();
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the compression codec.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::geoparquet::{WriterOptions, Compression};
    ///
    /// let options = WriterOptions::new().with_compression(Compression::SNAPPY);
    /// ```
    pub fn with_compression(mut self, compression: impl Into<Option<Compression>>) -> Self {
        self.compression = compression.into();
        self
    }

    /// Sets the maximum row group size.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::geoparquet::WriterOptions;
    ///
    /// let options = WriterOptions::new().with_max_row_group_size(50000);
    /// ```
    pub fn with_max_row_group_size(mut self, size: usize) -> Self {
        self.max_row_group_size = size;
        self
    }
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            compression: Some(default_compression()),
            max_row_group_size: DEFAULT_STAC_MAX_ROW_GROUP_SIZE,
        }
    }
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
        .writer_options(WriterOptions::new().with_compression(compression))
        .build(item_collection.into().items)
        .and_then(|writer| writer.finish())
}

/// Builder for a stac-geoparquet writer.
#[derive(Debug)]
pub struct WriterBuilder<W: Write + Send> {
    writer: W,
    options: Options,
    writer_options: WriterOptions,
}

/// Write items to stac-geoparquet.
#[allow(missing_debug_implementations)]
pub struct Writer<W: Write + Send> {
    state: WriterState,
    arrow_writer: ArrowWriter<W>,
}

/// stac-geoparquet metadata
#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    /// The stac-geoparquet version.
    pub version: String,

    /// Any STAC collections stored alongside the items.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub collections: HashMap<String, Collection>,
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
            writer_options: WriterOptions::default(),
        }
    }

    /// Sets the writer options for parquet writing (compression, row group size, etc).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io::Cursor;
    /// use stac::{Item, geoparquet::{WriterBuilder, WriterOptions, Compression}};
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let cursor = Cursor::new(Vec::new());
    /// let options = WriterOptions::new()
    ///     .with_compression(Compression::SNAPPY)
    ///     .with_max_row_group_size(50000);
    /// let writer = WriterBuilder::new(cursor)
    ///     .writer_options(options)
    ///     .build(vec![item])
    ///     .unwrap();
    /// ```
    pub fn writer_options(mut self, writer_options: WriterOptions) -> WriterBuilder<W> {
        self.writer_options = writer_options;
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
        Writer::new(self.writer, self.options, self.writer_options, items)
    }
}

impl WriterEncoder {
    /// Creates a new writer encoder.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, geoarrow::Options, geoparquet::WriterEncoder};
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let options = Options::default();
    /// let (encoder, record_batch) = WriterEncoder::new(options, vec![item]).unwrap();
    /// assert_eq!(record_batch.num_rows(), 1);
    /// ```
    pub fn new(options: Options, items: Vec<Item>) -> Result<(WriterEncoder, RecordBatch)> {
        let (geoarrow_encoder, record_batch) = Encoder::new(items, options)?;
        let options = GeoParquetWriterOptionsBuilder::default()
            .set_primary_column("geometry".to_string())
            .build();
        let mut encoder = GeoParquetRecordBatchEncoder::try_new(&record_batch.schema(), &options)?;
        let record_batch = encoder.encode_record_batch(&record_batch)?;
        Ok((
            WriterEncoder {
                geoarrow_encoder,
                encoder,
            },
            record_batch,
        ))
    }

    /// Encodes items into a record batch.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, geoarrow::Options, geoparquet::WriterEncoder};
    ///
    /// let item1: Item = stac::read("examples/simple-item.json").unwrap();
    /// let item2 = item1.clone();
    /// let options = Options::default();
    /// let (mut encoder, record_batch) = WriterEncoder::new(options, vec![item1]).unwrap();
    /// let record_batch = encoder.encode(vec![item2]).unwrap();
    /// assert_eq!(record_batch.num_rows(), 1);
    /// ```
    pub fn encode(&mut self, items: Vec<Item>) -> Result<RecordBatch> {
        let record_batch = self.geoarrow_encoder.encode(items)?;
        let record_batch = self.encoder.encode_record_batch(&record_batch)?;
        Ok(record_batch)
    }

    /// Consumes this encoder and returns the keys and values.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, geoarrow::Options, geoparquet::WriterEncoder};
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let options = Options::default();
    /// let (encoder, _) = WriterEncoder::new(options, vec![item]).unwrap();
    /// let key_value = encoder.into_keyvalue().unwrap();
    /// assert_eq!(key_value.key, "geo");
    /// ```
    pub fn into_keyvalue(self) -> Result<KeyValue> {
        let keyvalue = self.encoder.into_keyvalue()?;
        Ok(keyvalue)
    }
}

/// Shared state for STAC geoparquet writers (both sync and async).
///
/// This struct encapsulates the common logic for encoding items and
/// managing metadata across different writer implementations.
#[allow(missing_debug_implementations)]
pub struct WriterState {
    encoder: WriterEncoder,
    metadata: Metadata,
}

impl WriterState {
    /// Creates a new WriterState and returns the initial record batch.
    ///
    /// This should be called during writer construction to initialize
    /// the encoder and create the first record batch.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, geoarrow::Options, geoparquet::WriterState};
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let (state, record_batch) = WriterState::new(Options::default(), vec![item]).unwrap();
    /// assert_eq!(record_batch.num_rows(), 1);
    /// ```
    pub fn new(options: Options, items: Vec<Item>) -> Result<(WriterState, RecordBatch)> {
        let (encoder, record_batch) = WriterEncoder::new(options, items)?;
        Ok((
            WriterState {
                encoder,
                metadata: Metadata::default(),
            },
            record_batch,
        ))
    }

    /// Encodes items into a record batch.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, geoarrow::Options, geoparquet::WriterState};
    ///
    /// let item1: Item = stac::read("examples/simple-item.json").unwrap();
    /// let item2 = item1.clone();
    /// let (mut state, _) = WriterState::new(Options::default(), vec![item1]).unwrap();
    /// let record_batch = state.encode(vec![item2]).unwrap();
    /// assert_eq!(record_batch.num_rows(), 1);
    /// ```
    pub fn encode(&mut self, items: Vec<Item>) -> Result<RecordBatch> {
        self.encoder.encode(items)
    }

    /// Adds a collection to the metadata.
    ///
    /// Warns and overwrites if there's already a collection with the same id.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, Collection, geoarrow::Options, geoparquet::WriterState};
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let (mut state, _) = WriterState::new(Options::default(), vec![item]).unwrap();
    /// state.add_collection(Collection::new("test-id", "description"));
    /// ```
    pub fn add_collection(&mut self, collection: Collection) {
        if let Some(previous_collection) = self
            .metadata
            .collections
            .insert(collection.id.clone(), collection)
        {
            log::warn!(
                "Collection with id={} already existed in writer, overwriting",
                previous_collection.id
            )
        }
    }

    /// Consumes the state and returns the metadata key-value pairs.
    ///
    /// This returns both the geo metadata and the stac-geoparquet metadata
    /// that should be appended to the parquet file.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, geoarrow::Options, geoparquet::WriterState};
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let (state, _) = WriterState::new(Options::default(), vec![item]).unwrap();
    /// let metadata = state.into_metadata().unwrap();
    /// assert_eq!(metadata.len(), 2); // geo + stac-geoparquet metadata
    /// assert_eq!(metadata[0].key, "geo");
    /// assert_eq!(metadata[1].key, "stac-geoparquet");
    /// ```
    pub fn into_metadata(self) -> Result<Vec<KeyValue>> {
        let mut metadata = Vec::with_capacity(2);
        metadata.push(self.encoder.into_keyvalue()?);
        metadata.push(KeyValue::new(
            METADATA_KEY.to_string(),
            serde_json::to_string(&self.metadata)?,
        ));
        Ok(metadata)
    }
}

impl<W: Write + Send> Writer<W> {
    fn new(
        writer: W,
        options: Options,
        writer_options: WriterOptions,
        items: Vec<Item>,
    ) -> Result<Self> {
        let (state, record_batch) = WriterState::new(options, items)?;
        let mut arrow_writer =
            ArrowWriter::try_new(writer, record_batch.schema(), Some(writer_options.into()))?;
        arrow_writer.write(&record_batch)?;
        Ok(Writer {
            state,
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
        let record_batch = self.state.encode(items)?;
        self.arrow_writer.write(&record_batch)?;
        Ok(())
    }

    /// Adds a collection to this writer's metadata.
    ///
    /// Warns and overwrites if there's already a collection with the same id.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, Collection, geoparquet::WriterBuilder};
    /// use std::io::Cursor;
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let cursor = Cursor::new(Vec::new());
    /// let writer = WriterBuilder::new(cursor)
    ///     .build(vec![item])
    ///     .unwrap()
    ///     .add_collection(Collection::new("an-id", "a description"))
    ///     .unwrap();
    /// writer.finish().unwrap();
    /// ```
    pub fn add_collection(mut self, collection: Collection) -> Result<Writer<W>> {
        self.state.add_collection(collection);
        Ok(self)
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
    /// let writer = WriterBuilder::new(cursor).build(vec![item]).unwrap();
    /// writer.finish().unwrap();
    /// ```
    pub fn finish(mut self) -> Result<()> {
        let metadata = self.state.into_metadata()?;
        for kv in metadata {
            self.arrow_writer.append_key_value_metadata(kv);
        }
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
    /// use stac::geoparquet::WriterOptions;
    ///
    /// let item_collection: ItemCollection = vec![Item::new("a"), Item::new("b")].into();
    /// let mut buf = Vec::new();
    /// item_collection.into_geoparquet_writer(&mut buf, WriterOptions::default()).unwrap();
    /// ```
    fn into_geoparquet_writer(
        self,
        writer: impl Write + Send,
        writer_options: WriterOptions,
    ) -> Result<()>;

    /// Writes a value to a writer as stac-geoparquet to some bytes.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac::{IntoGeoparquet, ItemCollection, Item};
    /// use stac::geoparquet::WriterOptions;
    ///
    /// let item_collection: ItemCollection = vec![Item::new("a"), Item::new("b")].into();
    /// let bytes = item_collection.into_geoparquet_vec(WriterOptions::default()).unwrap();
    /// ```
    fn into_geoparquet_vec(self, writer_options: WriterOptions) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        self.into_geoparquet_writer(&mut buf, writer_options)?;
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
                _: WriterOptions,
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
        writer_options: WriterOptions,
    ) -> Result<()> {
        WriterBuilder::new(writer)
            .writer_options(writer_options)
            .build(self.items)?
            .finish()
    }
}

impl IntoGeoparquet for Item {
    fn into_geoparquet_writer(
        self,
        writer: impl Write + Send,
        writer_options: WriterOptions,
    ) -> Result<()> {
        ItemCollection::from(vec![self]).into_geoparquet_writer(writer, writer_options)
    }
}

impl IntoGeoparquet for Value {
    fn into_geoparquet_writer(
        self,
        writer: impl Write + Send,
        writer_options: WriterOptions,
    ) -> Result<()> {
        ItemCollection::try_from(self)?.into_geoparquet_writer(writer, writer_options)
    }
}

impl IntoGeoparquet for serde_json::Value {
    fn into_geoparquet_writer(
        self,
        writer: impl Write + Send,
        writer_options: WriterOptions,
    ) -> Result<()> {
        let item_collection: ItemCollection = serde_json::from_value(self)?;
        item_collection.into_geoparquet_writer(writer, writer_options)
    }
}

impl From<WriterOptions> for WriterProperties {
    fn from(value: WriterOptions) -> Self {
        let mut builder = WriterProperties::builder();
        if let Some(compression) = value.compression {
            builder = builder.set_compression(compression);
        }
        builder = builder.set_max_row_group_size(value.max_row_group_size);
        builder.build()
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Metadata {
            version: VERSION.to_string(),
            collections: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Collection, FromGeoparquet, Item, ItemCollection, SelfHref, Value,
        geoparquet::{METADATA_KEY, Metadata, VERSION, WriterBuilder},
    };
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

    #[test]
    fn custom_max_row_group_size() {
        // Create multiple items to test row grouping
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let items: Vec<Item> = (0..100).map(|_| item.clone()).collect();

        let mut cursor = Cursor::new(Vec::new());
        let options = super::WriterOptions::new().with_max_row_group_size(25);
        WriterBuilder::new(&mut cursor)
            .writer_options(options)
            .build(items)
            .unwrap()
            .finish()
            .unwrap();

        let bytes = Bytes::from(cursor.into_inner());
        let reader = SerializedFileReader::new(bytes).unwrap();

        // Should have 4 row groups (100 items / 25 per group)
        assert_eq!(reader.metadata().num_row_groups(), 4);
    }

    #[test]
    fn default_max_row_group_size() {
        // Create multiple items to test row grouping
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let items: Vec<Item> = (0..1000).map(|_| item.clone()).collect();

        let mut cursor = Cursor::new(Vec::new());
        WriterBuilder::new(&mut cursor)
            .build(items)
            .unwrap()
            .finish()
            .unwrap();

        let bytes = Bytes::from(cursor.into_inner());
        let reader = SerializedFileReader::new(bytes).unwrap();

        // Should have a single row group
        assert_eq!(reader.metadata().num_row_groups(), 1);
    }

    #[test]
    fn no_assets() {
        let mut item: Item = crate::read("examples/simple-item.json").unwrap();
        item.assets = Default::default();
        let mut writer = Cursor::new(Vec::new());
        super::into_writer(&mut writer, vec![item]).unwrap();
        let item_collection = super::from_reader(Bytes::from(writer.into_inner())).unwrap();
        assert!(item_collection.items[0].assets.is_empty());
    }

    #[test]
    fn metadata() {
        let item: Item = crate::read("examples/simple-item.json").unwrap();
        let mut cursor = Cursor::new(Vec::new());
        WriterBuilder::new(&mut cursor)
            .build(vec![item])
            .unwrap()
            .add_collection(Collection::new("an-id", "a description"))
            .unwrap()
            .finish()
            .unwrap();
        let bytes = Bytes::from(cursor.into_inner());
        let reader = SerializedFileReader::new(bytes).unwrap();
        let metadata = reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap()
            .iter()
            .find_map(|key_value| {
                if key_value.key == METADATA_KEY {
                    Some(
                        serde_json::from_str::<Metadata>(key_value.value.as_ref().unwrap())
                            .unwrap(),
                    )
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(metadata.version, VERSION);
        assert_eq!(metadata.collections["an-id"].description, "a description");
    }
}
