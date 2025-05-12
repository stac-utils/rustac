use crate::{
    Error, FromJson, FromNdjson, Href, RealizedHref, Result, SelfHref, ToJson, ToNdjson,
    geoparquet::{Compression, FromGeoparquet, IntoGeoparquet},
};
use bytes::Bytes;
use std::{fmt::Display, path::Path, str::FromStr};

/// The format of STAC data.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Format {
    /// JSON data (the default).
    ///
    /// If `true`, the data will be pretty-printed on write.
    Json(bool),

    /// Newline-delimited JSON.
    NdJson,

    /// [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet)
    Geoparquet(Option<Compression>),
}

impl Format {
    /// Infer the format from a file extension.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Format;
    ///
    /// assert_eq!(Format::Json(false), Format::infer_from_href("item.json").unwrap());
    /// ```
    pub fn infer_from_href(href: &str) -> Option<Format> {
        let format = href.rsplit_once('.').and_then(|(_, ext)| ext.parse().ok());
        if let Some(Format::Geoparquet(_)) = format {
            Some(Format::geoparquet()) // to pick up the default compression
        } else {
            format
        }
    }

    /// Returns true if this is a geoparquet href.
    pub fn is_geoparquet_href(href: &str) -> bool {
        matches!(Format::infer_from_href(href), Some(Format::Geoparquet(_)))
    }

    /// Reads a STAC object from an href in this format.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Format, Item};
    ///
    /// let item: Item = Format::json().read("examples/simple-item.json").unwrap();
    /// ```
    #[allow(unused_variables)]
    pub fn read<T: SelfHref + FromJson + FromNdjson + FromGeoparquet>(
        &self,
        href: impl Into<Href>,
    ) -> Result<T> {
        let mut href = href.into();
        let mut value: T = match href.clone().realize() {
            RealizedHref::Url(url) => {
                #[cfg(feature = "reqwest")]
                {
                    let bytes = reqwest::blocking::get(url)?.bytes()?;
                    self.from_bytes(bytes)?
                }
                #[cfg(not(feature = "reqwest"))]
                {
                    return Err(Error::FeatureNotEnabled("reqwest"));
                }
            }
            RealizedHref::PathBuf(path) => {
                let path = path.canonicalize()?;
                let value = self.from_path(&path)?;
                href = path.as_path().into();
                value
            }
        };
        *value.self_href_mut() = Some(href);
        Ok(value)
    }

    /// Reads a local file in the given format.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Format, Item};
    ///
    /// let item: Item = Format::json().from_path("examples/simple-item.json").unwrap();
    /// ```
    pub fn from_path<T: FromJson + FromNdjson + FromGeoparquet + SelfHref>(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<T> {
        let path = path.as_ref().canonicalize()?;
        match self {
            Format::Json(_) => T::from_json_path(&path),
            Format::NdJson => T::from_ndjson_path(&path),
            Format::Geoparquet(_) => T::from_geoparquet_path(&path),
        }
        .map_err(|err| {
            if let Error::Io(err) = err {
                Error::FromPath {
                    io: err,
                    path: path.to_string_lossy().into_owned(),
                }
            } else {
                err
            }
        })
    }

    /// Reads a STAC object from some bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Format, Item};
    /// use std::{io::Read, fs::File};
    ///
    /// let mut buf = Vec::new();
    /// File::open("examples/simple-item.json").unwrap().read_to_end(&mut buf).unwrap();
    /// let item: Item = Format::json().from_bytes(buf).unwrap();
    /// ```
    pub fn from_bytes<T: FromJson + FromNdjson + FromGeoparquet>(
        &self,
        bytes: impl Into<Bytes>,
    ) -> Result<T> {
        match self {
            Format::Json(_) => T::from_json_slice(&bytes.into()),
            Format::NdJson => T::from_ndjson_bytes(bytes),
            Format::Geoparquet(_) => T::from_geoparquet_bytes(bytes),
        }
    }

    /// Gets a STAC value from an object store with the provided options.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac::{Catalog, Format};
    ///
    /// #[cfg(feature = "object-store-aws")]
    /// {
    /// # tokio_test::block_on(async {
    ///     let catalog: Catalog = stac::io::get_opts("s3://nz-elevation/catalog.json",
    ///         [("skip_signature", "true"), ("region", "ap-southeast-2")],
    ///     ).await.unwrap();
    /// # })
    /// }
    /// ```
    #[cfg(feature = "object-store")]
    pub async fn get_opts<T, I, K, V>(&self, href: impl Into<Href>, options: I) -> Result<T>
    where
        T: SelfHref + FromJson + FromNdjson + FromGeoparquet,
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let href = href.into();
        match href.clone().realize() {
            RealizedHref::Url(url) => {
                let (object_store, path) = parse_url_opts(&url, options)?;
                let mut value: T =
                    self.get_store(object_store.into(), path)
                        .await
                        .map_err(|err| Error::Get {
                            href,
                            message: err.to_string(),
                        })?;
                *value.self_href_mut() = Some(Href::Url(url));
                Ok(value)
            }
            RealizedHref::PathBuf(path) => {
                tracing::debug!(
                    "getting {self} from {} with the standard library",
                    path.display()
                );
                self.from_path(path).map_err(|err| Error::Get {
                    href,
                    message: err.to_string(),
                })
            }
        }
    }

    /// Gets a STAC value from an object store.
    #[cfg(feature = "object-store")]
    pub async fn get_store<T>(
        &self,
        object_store: std::sync::Arc<dyn object_store::ObjectStore>,
        path: impl Into<object_store::path::Path>,
    ) -> Result<T>
    where
        T: SelfHref + FromJson + FromNdjson + FromGeoparquet,
    {
        let path = path.into();
        tracing::debug!("getting {self} from {path} with object store");
        let get_result = object_store.get(&path).await?;
        let value: T = self.from_bytes(get_result.bytes().await?)?;
        Ok(value)
    }

    /// Writes a STAC value to the provided path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac::{Item, Format};
    ///
    /// Format::json().write("an-id.json", Item::new("an-id")).unwrap();
    /// ```
    pub fn write<T: ToJson + ToNdjson + IntoGeoparquet>(
        &self,
        path: impl AsRef<Path>,
        value: T,
    ) -> Result<()> {
        match self {
            Format::Json(pretty) => value.to_json_path(path, *pretty),
            Format::NdJson => value.to_ndjson_path(path),
            Format::Geoparquet(compression) => value.into_geoparquet_path(path, *compression),
        }
    }

    /// Converts a STAC object into some bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Format, Item};
    ///
    /// let item = Item::new("an-id");
    /// let bytes = Format::json().into_vec(item).unwrap();
    /// ```
    pub fn into_vec<T: ToJson + ToNdjson + IntoGeoparquet>(&self, value: T) -> Result<Vec<u8>> {
        match self {
            Format::Json(pretty) => value.to_json_vec(*pretty),
            Format::NdJson => value.to_ndjson_vec(),
            Format::Geoparquet(compression) => value.into_geoparquet_vec(*compression),
        }
    }

    /// Puts a STAC value to an object store with the provided options.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac::{Item, Format};
    ///
    /// let item = Item::new("an-id");
    /// #[cfg(feature = "object-store-aws")]
    /// {
    /// # tokio_test::block_on(async {
    ///     Format::json().put_opts("s3://bucket/item.json", item, [("aws_access_key_id", "...")]).await.unwrap();
    /// # })
    /// }
    /// ```
    #[cfg(feature = "object-store")]
    pub async fn put_opts<T, I, K, V>(
        &self,
        href: impl ToString,
        value: T,
        options: I,
    ) -> Result<Option<object_store::PutResult>>
    where
        T: ToJson + ToNdjson + IntoGeoparquet,
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: Into<String>,
    {
        let href = href.to_string();
        if let Ok(url) = url::Url::parse(&href) {
            let (object_store, path) = parse_url_opts(&url, options)?;
            self.put_store(object_store.into(), path, value)
                .await
                .map(Some)
        } else {
            self.write(href, value).map(|_| None)
        }
    }

    /// Puts a STAC value into an object store.
    #[cfg(feature = "object-store")]
    pub async fn put_store<T>(
        &self,
        object_store: std::sync::Arc<dyn object_store::ObjectStore>,
        path: impl Into<object_store::path::Path>,
        value: T,
    ) -> Result<object_store::PutResult>
    where
        T: ToJson + ToNdjson + IntoGeoparquet,
    {
        let bytes = self.into_vec(value)?;
        let put_result = object_store.put(&path.into(), bytes.into()).await?;
        Ok(put_result)
    }

    /// Returns the default JSON format (compact).
    pub fn json() -> Format {
        Format::Json(false)
    }

    /// Returns the newline-delimited JSON format.
    pub fn ndjson() -> Format {
        Format::NdJson
    }

    /// Returns the default geoparquet format (snappy compression if compression is enabled).
    pub fn geoparquet() -> Format {
        #[cfg(feature = "geoparquet-compression")]
        {
            Format::Geoparquet(Some(Compression::SNAPPY))
        }
        #[cfg(not(feature = "geoparquet-compression"))]
        {
            Format::Geoparquet(None)
        }
    }
}

#[cfg(feature = "object-store")]
fn parse_url_opts<I, K, V>(
    url: &url::Url,
    options: I,
) -> Result<(Box<dyn object_store::ObjectStore>, object_store::path::Path)>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    // It's technically inefficient to parse it twice, but we're doing this to
    // then do IO so who cares.
    #[cfg(feature = "object-store-aws")]
    if let Ok((object_store::ObjectStoreScheme::AmazonS3, path)) =
        object_store::ObjectStoreScheme::parse(url)
    {
        let mut builder = object_store::aws::AmazonS3Builder::from_env();
        for (key, value) in options {
            builder = builder.with_config(key.as_ref().parse()?, value);
        }
        return Ok((Box::new(builder.with_url(url.to_string()).build()?), path));
    }

    let result = object_store::parse_url_opts(url, options)?;
    Ok(result)
}

impl Default for Format {
    fn default() -> Self {
        Self::Json(false)
    }
}

impl Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(pretty) => {
                if *pretty {
                    f.write_str("json-pretty")
                } else {
                    f.write_str("json")
                }
            }
            Self::NdJson => f.write_str("ndjson"),
            Self::Geoparquet(compression) => {
                if let Some(compression) = *compression {
                    write!(f, "geoparquet[{}]", compression)
                } else {
                    f.write_str("geoparquet")
                }
            }
        }
    }
}

impl FromStr for Format {
    type Err = Error;

    #[cfg_attr(not(feature = "geoparquet"), allow(unused_variables))]
    fn from_str(s: &str) -> Result<Format> {
        match s.to_ascii_lowercase().as_str() {
            "json" | "geojson" => Ok(Self::Json(false)),
            "json-pretty" | "geojson-pretty" => Ok(Self::Json(true)),
            "ndjson" => Ok(Self::NdJson),
            _ => {
                if s.starts_with("parquet") || s.starts_with("geoparquet") {
                    if let Some((_, compression)) = s.split_once('[') {
                        if let Some(stop) = compression.find(']') {
                            #[cfg(feature = "geoparquet")]
                            {
                                Ok(Self::Geoparquet(Some(compression[..stop].parse()?)))
                            }
                            #[cfg(not(feature = "geoparquet"))]
                            {
                                Ok(Self::Geoparquet(Some(Compression)))
                            }
                        } else {
                            Err(Error::UnsupportedFormat(s.to_string()))
                        }
                    } else {
                        Ok(Self::Geoparquet(None))
                    }
                } else {
                    Err(Error::UnsupportedFormat(s.to_string()))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Format;
    use crate::geoparquet::Compression;

    #[test]
    fn parse_geoparquet() {
        assert_eq!(
            "parquet".parse::<Format>().unwrap(),
            Format::Geoparquet(None)
        );
    }

    #[test]
    #[cfg(feature = "geoparquet")]
    fn parse_geoparquet_compression() {
        let format: Format = "geoparquet[snappy]".parse().unwrap();
        assert_eq!(format, Format::Geoparquet(Some(Compression::SNAPPY)));
    }

    #[test]
    #[cfg(not(feature = "geoparquet"))]
    fn parse_geoparquet_compression() {
        let format: Format = "geoparquet[snappy]".parse().unwrap();
        assert_eq!(format, Format::Geoparquet(Some(Compression)));
    }

    #[test]
    #[cfg(feature = "geoparquet-compression")]
    fn infer_from_href() {
        assert_eq!(
            Format::Geoparquet(Some(Compression::SNAPPY)),
            Format::infer_from_href("out.parquet").unwrap()
        );
    }

    #[test]
    #[cfg(not(feature = "geoparquet-compression"))]
    fn infer_from_href() {
        assert_eq!(
            Format::Geoparquet(None),
            Format::infer_from_href("out.parquet").unwrap()
        );
    }

    #[tokio::test]
    #[cfg(feature = "object-store")]
    async fn prefix_store_read() {
        use crate::Item;
        use std::sync::Arc;

        let object_store =
            object_store::local::LocalFileSystem::new_with_prefix("examples").unwrap();
        let _: Item = Format::json()
            .get_store(Arc::new(object_store), "simple-item.json")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[cfg(feature = "object-store")]
    async fn store_write() {
        use crate::Item;
        use object_store::ObjectStore;
        use std::sync::Arc;

        let object_store = Arc::new(object_store::memory::InMemory::new());
        let item = Item::new("an-id");
        let _ = Format::json()
            .put_store(object_store.clone(), "item.json", item)
            .await
            .unwrap();
        let get_result = object_store.get(&"item.json".into()).await.unwrap();
        let _: Item = serde_json::from_slice(&get_result.bytes().await.unwrap()).unwrap();
    }
}
