use crate::{Format, Readable, Result, Writeable};
use object_store::{ObjectStore, ObjectStoreScheme, PutResult, path::Path};
use stac::Href;
use std::{fmt::Debug, sync::Arc};
use tracing::instrument;
use url::Url;

/// Parses an href into a [StacStore] and a [Path].
pub fn parse_href(href: impl Into<Href>) -> Result<(StacStore, Path)> {
    parse_href_opts(href, [] as [(&str, &str); 0])
}

/// Parses an href and options into [StacStore] and a [Path].
///
/// Relative string hrefs are made absolute `file://` hrefs relative to the current directory.`
pub fn parse_href_opts<I, K, V>(href: impl Into<Href>, options: I) -> Result<(StacStore, Path)>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let mut url = match href.into() {
        Href::Url(url) => url,
        Href::String(s) => {
            let s = if s.starts_with("/") {
                format!("file://{s}")
            } else {
                let path_buf = std::fs::canonicalize(s)?;
                format!("file://{}", path_buf.display())
            };
            Url::parse(&s)?
        }
    };
    let parse = || -> Result<(Box<dyn ObjectStore>, Path)> {
        tracing::debug!("parsing url={url}");
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
        if matches!(scheme, ObjectStoreScheme::AmazonS3) {
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
    tracing::debug!("url {url} parsed into path {path}");
    url.set_path("");
    Ok((StacStore::new(Arc::new(store), url), path))
}

/// Reads STAC from an [ObjectStore].
#[derive(Debug, Clone)]
pub struct StacStore {
    store: Arc<dyn ObjectStore>,
    root: Url,
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
            root,
        }
    }

    /// Gets a STAC value from the store.
    ///
    /// The format will be inferred from the path's file extension.
    ///
    /// # Examples
    ///
    /// ```
    /// use object_store::local::LocalFileSystem;
    /// use stac_io::StacStore;
    ///
    /// let store = LocalFileSystem::new_with_prefix(std::env::current_dir().unwrap()).unwrap();
    /// let stac_store = StacStore::from(store);
    /// # tokio_test::block_on(async {
    /// let item: stac::Item = stac_store.get("examples/simple-item.json").await.unwrap();
    /// });
    /// ```
    pub async fn get<T>(&self, path: impl Into<Path>) -> Result<T>
    where
        T: Readable,
    {
        let path = path.into();
        let format = Format::infer_from_href(path.as_ref()).unwrap_or_default();
        self.get_format(path, format).await
    }

    /// Gets a STAC value from the store in a specific format.
    #[instrument(skip(self))]
    pub async fn get_format<T>(&self, path: impl Into<Path> + Debug, format: Format) -> Result<T>
    where
        T: Readable,
    {
        let path = path.into();
        let get_result = self.store.get(&path).await?;
        let bytes = get_result.bytes().await?;
        let mut value: T = format.from_bytes(bytes)?;
        value.set_self_href(self.root.join(path.as_ref())?);
        Ok(value)
    }

    /// Puts a STAC value to the store.
    pub async fn put<T>(&self, path: impl Into<Path>, value: T) -> Result<PutResult>
    where
        T: Writeable + Debug,
    {
        let path = path.into();
        let format = Format::infer_from_href(path.as_ref()).unwrap_or_default();
        self.put_format(path, value, format).await
    }

    /// Puts a STAC value to the store in a specific format.
    #[instrument(skip(self))]
    pub async fn put_format<T>(
        &self,
        path: impl Into<Path> + Debug,
        value: T,
        format: Format,
    ) -> Result<PutResult>
    where
        T: Writeable + Debug,
    {
        let path = path.into();
        let bytes = format.into_vec(value)?;
        let put_result = self.store.put(&path, bytes.into()).await?;
        Ok(put_result)
    }
}

#[cfg(test)]
mod tests {
    use stac::{Item, SelfHref};

    #[tokio::test]
    async fn get_local() {
        let (store, path) = super::parse_href("examples/simple-item.json").unwrap();
        assert_eq!(
            path,
            std::fs::canonicalize("examples/simple-item.json")
                .unwrap()
                .to_string_lossy()
                .into_owned()
                .strip_prefix("/")
                .unwrap()
                .into()
        );
        let item: Item = store.get(path).await.unwrap();
        assert_eq!(
            item.self_href().unwrap().to_string(),
            format!(
                "file://{}",
                std::fs::canonicalize("examples/simple-item.json")
                    .unwrap()
                    .display()
            )
        )
    }
}
