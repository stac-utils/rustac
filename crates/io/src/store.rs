use crate::{Format, Readable, Result, Writeable};
use object_store::{ObjectStore, PutResult, path::Path};
use std::sync::Arc;

/// Reads STAC from an [ObjectStore].
#[derive(Debug)]
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
        let get_result = self.0.get(&path).await?;
        let bytes = get_result.bytes().await?;
        let value: T = format.from_bytes(bytes)?;
        Ok(value)
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
    use object_store::local::LocalFileSystem;
    use stac::Item;

    #[tokio::test]
    async fn get_local() {
        let store = StacStore::from(
            LocalFileSystem::new_with_prefix(std::env::current_dir().unwrap()).unwrap(),
        );
        let _: Item = store.get("examples/simple-item.json").await.unwrap();
    }
}
