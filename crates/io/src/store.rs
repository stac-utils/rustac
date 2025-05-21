use crate::{Format, Readable, Result, Writeable};
use async_stream::try_stream;
use futures_core::TryStream;
use object_store::{DynObjectStore, ObjectStore, PutResult, path::Path};
use stac::{Href, Link, Links, Value};
use std::{collections::VecDeque, sync::Arc};
use tokio::task::JoinSet;

/// Parses an href into a [StacStore] and a [Path].
pub fn parse_href(href: impl AsRef<Href>) -> Result<(StacStore, Path)> {
    parse_href_opts(href, [] as [(&str, &str); 0])
}

/// Parses an href and options into [StacStore] and a [Path].
///
/// Relative string hrefs are made absolute `file://` hrefs relative to the current directory.`
pub fn parse_href_opts<I, K, V>(href: impl AsRef<Href>, options: I) -> Result<(StacStore, Path)>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let (store, path): (Box<DynObjectStore>, _) = match href.as_ref() {
        Href::Url(url) => {
            tracing::debug!("parsing url={url}");
            // It's technically inefficient to parse it twice, but we're doing this to
            // then do IO so who cares.
            #[cfg(feature = "store-aws")]
            if let Ok((object_store::ObjectStoreScheme::AmazonS3, path)) =
                object_store::ObjectStoreScheme::parse(url)
            {
                let mut builder = object_store::aws::AmazonS3Builder::from_env();
                for (key, value) in options {
                    builder = builder.with_config(key.as_ref().parse()?, value);
                }
                (Box::new(builder.with_url(url.to_string()).build()?), path)
            } else {
                object_store::parse_url_opts(url, options)?
            }
            #[cfg(not(feature = "store-aws"))]
            {
                object_store::parse_url_opts(url, options)?
            }
        }
        Href::String(s) => {
            if s.starts_with("/") {
                object_store::parse_url_opts(&format!("file://{s}").parse()?, options)?
            } else {
                let s = std::env::current_dir()?.join(s);
                object_store::parse_url_opts(&format!("file://{}", s.display()).parse()?, options)?
            }
        }
    };
    Ok((store.into(), path))
}

/// Reads STAC from an [ObjectStore].
#[derive(Clone, Debug)]
pub struct StacStore(Arc<dyn ObjectStore>);

impl StacStore {
    /// Creates a new [StacStore] from an [ObjectStore].
    ///
    /// # Examples
    ///
    /// ```
    /// use object_store::local::LocalFileSystem;
    /// use stac_io::StacStore;
    /// use std::sync::Arc;
    ///
    /// let stac_store = StacStore::new(Arc::new(LocalFileSystem::new()));
    /// ```
    pub fn new(store: Arc<dyn ObjectStore>) -> StacStore {
        StacStore(Arc::new(store))
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
    pub async fn get_format<T>(&self, path: impl Into<Path>, format: Format) -> Result<T>
    where
        T: Readable,
    {
        let path = path.into();
        tracing::debug!("getting {path} (format={format})");
        let get_result = self.0.get(&path).await?;
        let bytes = get_result.bytes().await?;
        let value: T = format.from_bytes(bytes)?;
        Ok(value)
    }

    /// Crawls a STAC value, streaming all items and children, recursively.
    pub async fn crawl(&self, path: impl Into<Path>) -> impl TryStream<Item = Result<Value>> {
        let mut values = VecDeque::new();
        let path = path.into();
        try_stream! {
            let value: Value = self.get(path.clone()).await?;
            values.push_front((value, path));
            while let Some((value, path)) = values.pop_front() {
                let mut join_set = JoinSet::new();
                for link in value.links().iter().filter(|link| link.is_child() || link.is_item()).cloned() {
                    let store = self.clone();
                    let path = path.clone();
                    join_set.spawn(async move {
                        store.get_link(link, path).await
                    });
                }
                yield value;
                while let Some(result) = join_set.join_next().await {
                    let (value, path) = result??;
                    match value {
                        Value::Catalog(_)  | Value::Collection(_) => values.push_back((value, path)),
                        Value::Item(_) => yield value,
                        Value::ItemCollection(item_collection) => {
                            for item in item_collection.items {
                                yield item.into();
                            }
                        }
                    }
                }
            }
        }
    }

    async fn get_link(&self, link: Link, parent_path: Path) -> Result<(Value, Path)> {
        let path = if link.is_absolute() {
            match link.href {
                Href::Url(url) => Path::from_url_path(url.path())?,
                Href::String(s) => Path::from_absolute_path(s)?,
            }
        } else {
            let parts: Vec<_> = parent_path.parts().collect();
            let path = if parts.len() > 1 {
                let take = parts.len() - 1;
                let prefix = Path::from_iter(parts.into_iter().take(take));
                format!("{}/{}", prefix, link.href.to_string())
            } else {
                link.href.to_string()
            };
            Path::parse(stac::href::normalize_path(&path))?
        };
        let value = self.get(path.clone()).await?;
        Ok((value, path))
    }

    /// Puts a STAC value to the store.
    pub async fn put<T>(&self, path: impl Into<Path>, value: T) -> Result<PutResult>
    where
        T: Writeable,
    {
        let path = path.into();
        let format = Format::infer_from_href(path.as_ref()).unwrap_or_default();
        self.put_format(path, value, format).await
    }

    /// Puts a STAC value to the store in a specific format.
    pub async fn put_format<T>(
        &self,
        path: impl Into<Path>,
        value: T,
        format: Format,
    ) -> Result<PutResult>
    where
        T: Writeable,
    {
        let path = path.into();
        let bytes = format.into_vec(value)?;
        let put_result = self.0.put(&path, bytes.into()).await?;
        Ok(put_result)
    }
}

impl<T> From<T> for StacStore
where
    T: ObjectStore,
{
    fn from(value: T) -> Self {
        StacStore(Arc::new(value))
    }
}

#[cfg(test)]
mod tests {
    use super::StacStore;
    use futures_util::{TryStreamExt, pin_mut};
    use object_store::local::LocalFileSystem;
    use stac::Item;

    #[tokio::test]
    async fn get_local() {
        let store = StacStore::from(
            LocalFileSystem::new_with_prefix(std::env::current_dir().unwrap()).unwrap(),
        );
        let _: Item = store.get("examples/simple-item.json").await.unwrap();
    }

    #[tokio::test]
    async fn crawl() {
        let store = StacStore::from(
            LocalFileSystem::new_with_prefix(std::env::current_dir().unwrap()).unwrap(),
        );
        let stream = store.crawl("examples/catalog.json").await;
        pin_mut!(stream);
        let mut values = Vec::new();
        while let Some(value) = stream.try_next().await.unwrap() {
            values.push(value);
        }
        assert_eq!(values.len(), 6);
    }
}
