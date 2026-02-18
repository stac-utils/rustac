use crate::{Format, Readable, Result, Writeable};
use object_store::{ObjectStore, ObjectStoreScheme, PutResult, path::Path};
use std::{fmt::Debug, sync::Arc};
use tracing::instrument;
use url::Url;

/// Parses an href into a [StacStore] and a [Path].
pub fn parse_href(href: impl ToString) -> Result<(StacStore, Path)> {
    parse_href_opts(href, [] as [(&str, &str); 0])
}

/// Parses an href and options into [StacStore] and a [Path].
///
/// Relative string hrefs are made absolute `file://` hrefs relative to the current directory.`
pub fn parse_href_opts<I, K, V>(href: impl ToString, options: I) -> Result<(StacStore, Path)>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let href = href.to_string();
    let mut url = stac::href::make_url(&href)?;
    let parse = || -> Result<(Box<dyn ObjectStore>, Path)> {
        // It's technically inefficient to parse it twice, but we're doing this to
        // then do IO so who cares.
        let (scheme, path) = ObjectStoreScheme::parse(&url).map_err(object_store::Error::from)?;

        #[cfg(feature = "store-aws")]
        if matches!(scheme, ObjectStoreScheme::AmazonS3) {
            let mut builder = object_store::aws::AmazonS3Builder::from_env();
            for (key, value) in options {
                builder = builder.with_config(key.as_ref().parse()?, value);
            }
            return Ok((Box::new(builder.with_url(url.to_string()).build()?), path));
        }

        #[cfg(feature = "store-azure")]
        if matches!(scheme, ObjectStoreScheme::MicrosoftAzure) {
            let mut builder = object_store::azure::MicrosoftAzureBuilder::from_env();
            for (key, value) in options {
                builder = builder.with_config(key.as_ref().parse()?, value);
            }
            return Ok((Box::new(builder.with_url(url.to_string()).build()?), path));
        }

        #[cfg(feature = "store-gcp")]
        if matches!(scheme, ObjectStoreScheme::GoogleCloudStorage) {
            let mut builder = object_store::gcp::GoogleCloudStorageBuilder::from_env();
            for (key, value) in options {
                builder = builder.with_config(key.as_ref().parse()?, value);
            }
            return Ok((Box::new(builder.with_url(url.to_string()).build()?), path));
        }

        let pair = object_store::parse_url_opts(&url, options)?;
        Ok(pair)
    };
    let (store, path) = parse()?;
    tracing::debug!("{url} parsed into path {path}");
    url.set_path("");
    Ok((StacStore::new(Arc::new(store), url), path))
}

/// Reads STAC from an [ObjectStore].
#[derive(Debug, Clone)]
pub struct StacStore {
    store: Arc<dyn ObjectStore>,
    root: Option<Url>,
}

impl StacStore {
    /// Creates a new [StacStore] from an [ObjectStore] and a root href.
    ///
    /// The root href is used to set the self href on all read STAC values,
    /// since we can't get that from the store.
    ///
    /// # Examples
    ///
    /// ```
    /// use object_store::local::LocalFileSystem;
    /// use stac_io::StacStore;
    /// use std::sync::Arc;
    ///
    /// let stac_store = StacStore::new(Arc::new(LocalFileSystem::new()), "file://".parse().unwrap());
    /// ```
    pub fn new(store: Arc<dyn ObjectStore>, root: Url) -> StacStore {
        StacStore {
            store: Arc::new(store),
            root: Some(root),
        }
    }

    /// Gets a STAC value from the store.
    ///
    /// The format will be inferred from the href's file extension.
    pub async fn get<T>(&self, href: impl ToString + AsRef<str> + Debug) -> Result<T>
    where
        T: Readable,
    {
        let format = Format::infer_from_href(href.as_ref()).unwrap_or_default();
        self.get_format(href, format).await
    }

    /// Gets a STAC value from the store in a specific format.
    #[instrument(skip(self))]
    pub async fn get_format<T>(&self, href: impl ToString + Debug, format: Format) -> Result<T>
    where
        T: Readable,
    {
        let href = href.to_string();
        let path = self.path(&href)?;
        let get_result = self.store.get(&path).await?;
        let bytes = get_result.bytes().await?;
        let mut value: T = format.from_bytes(bytes)?;
        if let Some(root) = self.root.as_ref() {
            value.set_self_href(root.join(path.as_ref())?);
        }
        Ok(value)
    }

    /// Puts a STAC value to the store.
    pub async fn put<T>(&self, href: impl AsRef<str> + Debug, value: T) -> Result<PutResult>
    where
        T: Writeable + Debug,
    {
        let format = Format::infer_from_href(href.as_ref()).unwrap_or_default();
        self.put_format(href, value, format).await
    }

    /// Puts a STAC value to the store in a specific format.
    #[instrument(skip(self))]
    pub async fn put_format<T>(
        &self,
        href: impl AsRef<str> + Debug,
        value: T,
        format: Format,
    ) -> Result<PutResult>
    where
        T: Writeable + Debug,
    {
        let path = self.path(href.as_ref())?;
        let bytes = format.into_vec(value)?;
        let put_result = self.store.put(&path, bytes.into()).await?;
        Ok(put_result)
    }

    /// Gets items from the store as a stream.
    ///
    /// For ndjson and geoparquet, items are yielded one at a time without
    /// materializing the entire collection in memory. For JSON, the full
    /// value is read and items are yielded from it.
    #[instrument(skip(self))]
    pub async fn get_item_stream(
        &self,
        href: impl ToString + Debug,
        format: Format,
    ) -> Result<Box<dyn Iterator<Item = Result<stac::Item>> + Send>> {
        let href = href.to_string();
        let path = self.path(&href)?;
        let get_result = self.store.get(&path).await?;
        let bytes = get_result.bytes().await?;
        match format {
            Format::NdJson => {
                let cursor = std::io::BufReader::new(std::io::Cursor::new(bytes));
                Ok(Box::new(crate::ndjson::ndjson_item_reader(cursor)))
            }
            #[cfg(feature = "geoparquet")]
            Format::Geoparquet(_) => {
                let iter = stac::geoparquet::from_reader_iter(bytes)?;
                Ok(Box::new(iter.flat_map(|result| match result {
                    Ok(items) => Box::new(items.into_iter().map(Ok))
                        as Box<dyn Iterator<Item = Result<stac::Item>> + Send>,
                    Err(e) => Box::new(std::iter::once(Err(e.into()))),
                })))
            }
            Format::Json(_) => {
                let item_collection: stac::ItemCollection = format.from_bytes(bytes)?;
                Ok(Box::new(item_collection.items.into_iter().map(Ok)))
            }
        }
    }

    /// Puts items from an iterator to the store.
    ///
    /// For ndjson, items are serialized one per line. For geoparquet, items
    /// are batched using the writer options' max row group size and written
    /// incrementally via [StacGeoparquetObjectWriter](geoparquet::StacGeoparquetObjectWriter).
    /// For JSON, items are collected into an ItemCollection.
    #[instrument(skip(self, items))]
    pub async fn put_item_stream(
        &self,
        href: impl AsRef<str> + Debug,
        items: impl Iterator<Item = stac::Item>,
        format: Format,
    ) -> Result<PutResult> {
        let path = self.path(href.as_ref())?;
        match format {
            Format::NdJson => {
                let mut buf = Vec::new();
                for item in items {
                    serde_json::to_writer(&mut buf, &item)?;
                    buf.push(b'\n');
                }
                let put_result = self.store.put(&path, buf.into()).await?;
                Ok(put_result)
            }
            #[cfg(feature = "geoparquet")]
            Format::Geoparquet(writer_options) => {
                let batch_size = writer_options.max_row_group_size;
                let mut batch = Vec::with_capacity(batch_size);
                let mut writer: Option<geoparquet::StacGeoparquetObjectWriter> = None;
                for item in items {
                    batch.push(item);
                    if batch.len() >= batch_size {
                        let items = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                        if let Some(ref mut writer) = writer {
                            writer.write(items).await?;
                        } else {
                            writer = Some(
                                geoparquet::StacGeoparquetObjectWriter::new(
                                    self.store.clone(),
                                    path.clone(),
                                    items,
                                    Default::default(),
                                    writer_options,
                                )
                                .await?,
                            );
                        }
                    }
                }
                if let Some(mut writer) = writer {
                    if !batch.is_empty() {
                        writer.write(batch).await?;
                    }
                    writer.close().await?;
                } else if !batch.is_empty() {
                    let writer = geoparquet::StacGeoparquetObjectWriter::new(
                        self.store.clone(),
                        path,
                        batch,
                        Default::default(),
                        writer_options,
                    )
                    .await?;
                    writer.close().await?;
                }
                Ok(PutResult {
                    e_tag: None,
                    version: None,
                })
            }
            _ => {
                let item_collection = stac::ItemCollection::from(items.collect::<Vec<_>>());
                let bytes = format.into_vec(item_collection)?;
                let put_result = self.store.put(&path, bytes.into()).await?;
                Ok(put_result)
            }
        }
    }

    fn path(&self, href: &str) -> Result<Path> {
        let result = if stac::href::is_windows_absolute_path(href) {
            Path::parse(href)
        } else if let Ok(url) = Url::parse(href) {
            // TODO check to see if the host and such match? or not?
            Path::from_url_path(url.path())
        } else {
            Path::parse(href)
        };
        let path = result.map_err(object_store::Error::from)?;
        Ok(path)
    }
}

impl<T> From<T> for StacStore
where
    T: Into<Arc<dyn ObjectStore>>,
{
    fn from(store: T) -> Self {
        let store: Arc<dyn ObjectStore> = store.into();
        StacStore { store, root: None }
    }
}

#[cfg(feature = "geoparquet")]
pub mod geoparquet {
    use crate::Result;
    use object_store::{ObjectStore, path::Path};
    use parquet::arrow::async_writer::{AsyncArrowWriter, ParquetObjectWriter};
    use stac::geoarrow::Options;
    use stac::geoparquet::{WriterOptions, WriterState};
    use stac::{Collection, Item};
    use std::sync::Arc;

    /// Writes stac-geoparquet to an object store.
    pub struct StacGeoparquetObjectWriter {
        state: WriterState,
        writer: AsyncArrowWriter<ParquetObjectWriter>,
    }

    impl StacGeoparquetObjectWriter {
        pub async fn new(
            store: Arc<dyn ObjectStore>,
            path: Path,
            items: Vec<Item>,
            options: Options,
            writer_options: WriterOptions,
        ) -> Result<StacGeoparquetObjectWriter> {
            let (state, record_batch) = WriterState::new(options, items)?;
            let object_store_writer = ParquetObjectWriter::new(store.clone(), path);
            let mut writer = AsyncArrowWriter::try_new(
                object_store_writer,
                record_batch.schema(),
                Some(writer_options.into()),
            )?;
            writer.write(&record_batch).await?;
            Ok(StacGeoparquetObjectWriter { state, writer })
        }

        pub async fn write(&mut self, items: Vec<Item>) -> Result<()> {
            let record_batch = self.state.encode(items)?;
            self.writer.write(&record_batch).await?;
            Ok(())
        }

        /// Adds a collection to this writer's metadata.
        ///
        /// Warns and overwrites if there's already a collection with the same id.
        pub fn add_collection(&mut self, collection: Collection) {
            self.state.add_collection(collection);
        }

        pub async fn close(mut self) -> Result<()> {
            let metadata = self.state.into_metadata()?;
            for kv in metadata {
                self.writer.append_key_value_metadata(kv);
            }
            self.writer.close().await?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::{memory::InMemory, path::Path};
    use stac::{Item, SelfHref};
    use std::sync::Arc;

    #[tokio::test]
    async fn get_local() {
        let (store, path) = super::parse_href("examples/simple-item.json").unwrap();
        assert!(path.to_string().ends_with("examples/simple-item.json"));
        let item: Item = store.get(path).await.unwrap();
        let self_href = item.self_href().unwrap();
        assert!(self_href.starts_with("file:///"));
        assert!(self_href.ends_with("examples/simple-item.json"));
    }

    #[tokio::test]
    async fn get_local_href() {
        let (store, path) = super::parse_href("examples/simple-item.json").unwrap();
        let href = format!("file:///{path}");
        let _: Item = store.get(href).await.unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "geoparquet")]
    async fn write_parquet() {
        use object_store::ObjectStore;

        use super::geoparquet::StacGeoparquetObjectWriter;

        let store = Arc::new(InMemory::new());
        let item: Item = stac::read("examples/simple-item.json").unwrap();
        let mut writer = StacGeoparquetObjectWriter::new(
            store.clone(),
            Path::from("test"),
            vec![item.clone()],
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();
        writer.write(vec![item]).await.unwrap();
        writer.close().await.unwrap();

        let bytes = store
            .get(&Path::from("test"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let item_collection = stac::geoparquet::from_reader(bytes).unwrap();
        assert_eq!(item_collection.items.len(), 2);
    }

    #[tokio::test]
    #[cfg(feature = "geoparquet")]
    async fn write_parquet_with_collection() {
        use object_store::ObjectStore;
        use parquet::file::reader::{FileReader, SerializedFileReader};

        use super::geoparquet::StacGeoparquetObjectWriter;

        let store = Arc::new(InMemory::new());
        let item: Item = stac::read("examples/simple-item.json").unwrap();
        let collection = stac::Collection::new("test-collection", "Test description");

        let mut writer = StacGeoparquetObjectWriter::new(
            store.clone(),
            Path::from("test-with-collection"),
            vec![item.clone()],
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();
        writer.add_collection(collection);

        writer.close().await.unwrap();

        let bytes = store
            .get(&Path::from("test-with-collection"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let reader = SerializedFileReader::new(bytes.clone()).unwrap();
        let file_metadata = reader.metadata().file_metadata();
        let key_value_metadata = file_metadata.key_value_metadata().unwrap();
        let stac_metadata = key_value_metadata
            .iter()
            .find(|kv| kv.key == "stac-geoparquet")
            .expect("stac-geoparquet metadata should exist");
        let metadata: stac::geoparquet::Metadata =
            serde_json::from_str(stac_metadata.value.as_ref().unwrap()).unwrap();
        assert!(metadata.collections.contains_key("test-collection"));
        assert_eq!(
            metadata.collections["test-collection"].description,
            "Test description"
        );

        let item_collection = stac::geoparquet::from_reader(bytes).unwrap();
        assert_eq!(item_collection.items.len(), 1);
    }
}
