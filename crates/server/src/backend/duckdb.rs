use super::Backend;
use crate::{Error, Result};
use bb8::{ManageConnection, Pool};
use futures_core::Stream;
use stac::Collection;
use stac::api::{
    CollectionsClient, ItemsClient, Search, StreamItemsClient, TransactionClient,
    stream_pages_generic,
};
use stac_duckdb::Client;

/// A backend that uses [DuckDB](https://duckdb.org/) to query
/// [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet).
#[derive(Clone, Debug)]
pub struct DuckdbBackend {
    pool: Pool<DuckdbConnectionManager>,
}

struct DuckdbConnectionManager {
    href: String,
}

struct DuckdbConnection {
    client: Client,
    href: String,
}

impl DuckdbBackend {
    /// Creates a new DuckDB backend pointing to a single **stac-geoparquet** file.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::DuckdbBackend;
    /// # tokio_test::block_on(async {
    /// let backend = DuckdbBackend::new("data/100-sentinel-2-items.parquet").await.unwrap();
    /// # })
    /// ```
    pub async fn new(href: impl ToString) -> Result<DuckdbBackend> {
        let pool = Pool::builder()
            .build(DuckdbConnectionManager {
                href: href.to_string(),
            })
            .await?;
        Ok(DuckdbBackend { pool })
    }
}

impl ItemsClient for DuckdbBackend {
    type Error = Error;

    async fn search(&self, search: Search) -> Result<stac::api::ItemCollection> {
        let client = self.pool.get().await.map_err(Box::new)?;
        client.search(search)
    }
}

impl CollectionsClient for DuckdbBackend {
    type Error = Error;

    async fn collections(&self) -> Result<Vec<Collection>> {
        let client = self.pool.get().await.map_err(Box::new)?;
        client.collections()
    }

    async fn collection(&self, id: &str) -> Result<Option<Collection>> {
        let client = self.pool.get().await.map_err(Box::new)?;
        client.collection(id)
    }
}

impl TransactionClient for DuckdbBackend {
    type Error = Error;

    async fn add_collection(&mut self, _collection: Collection) -> Result<()> {
        Err(Error::ReadOnly)
    }

    async fn add_item(&mut self, _item: stac::Item) -> Result<()> {
        Err(Error::ReadOnly)
    }
}

impl StreamItemsClient for DuckdbBackend {
    type Error = Error;

    async fn search_stream(
        &self,
        search: Search,
    ) -> Result<impl Stream<Item = std::result::Result<stac::api::Item, Error>> + Send> {
        let page = ItemsClient::search(self, search.clone()).await?;
        Ok(stream_pages_generic(self.clone(), search, page))
    }
}

impl Backend for DuckdbBackend {
    fn has_item_search(&self) -> bool {
        true
    }

    fn has_filter(&self) -> bool {
        false
    }
}

impl ManageConnection for DuckdbConnectionManager {
    type Connection = DuckdbConnection;
    type Error = Error;

    async fn connect(&self) -> Result<DuckdbConnection> {
        DuckdbConnection::new(&self.href)
    }

    async fn is_valid(&self, _conn: &mut DuckdbConnection) -> Result<()> {
        Ok(())
    }

    fn has_broken(&self, _conn: &mut DuckdbConnection) -> bool {
        false
    }
}

impl DuckdbConnection {
    fn new(href: impl ToString) -> Result<DuckdbConnection> {
        let client = Client::new()?;
        Ok(DuckdbConnection {
            client,
            href: href.to_string(),
        })
    }

    fn collections(&self) -> Result<Vec<Collection>> {
        let collections = self.client.collections(&self.href)?;
        Ok(collections)
    }

    fn collection(&self, id: &str) -> Result<Option<Collection>> {
        let collections = self.client.collections(&self.href)?;
        Ok(collections
            .into_iter()
            .find(|collection| collection.id == id))
    }

    fn search(&self, search: Search) -> Result<stac::api::ItemCollection> {
        let item_collection = self.client.search(&self.href, search)?;
        Ok(item_collection)
    }
}

#[cfg(test)]
mod tests {
    use stac::api::CollectionsClient;

    #[tokio::test]
    async fn backend() {
        let backend = super::DuckdbBackend::new("data/100-sentinel-2-items.parquet")
            .await
            .unwrap();
        assert!(
            backend
                .collection("sentinel-2-l2a")
                .await
                .unwrap()
                .is_some()
        );
    }
}
