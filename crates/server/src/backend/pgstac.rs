use crate::{Backend, Error, Result};
use futures_core::Stream;
use futures_util::StreamExt;
use pgstac::{ConnectConfig, PgstacPool, ingest::ConflictPolicy};
use serde_json::Value;
use stac::api::{
    CollectionsClient, ItemCollection, ItemsClient, Search, StreamItemsClient, TransactionClient,
};
use stac::{Collection, Item};

/// A backend for a [pgstac](https://github.com/stac-utils/pgstac) database.
///
/// Wraps pgstac's own [`PgstacPool`] (a `deadpool` pool with rustls TLS). Every request routes through
/// the pool's `stac::api` client impls; this backend only adapts [`pgstac::Error`] to [`Error`].
#[derive(Clone, Debug)]
pub struct PgstacBackend {
    pool: PgstacPool,
}

impl PgstacBackend {
    /// Creates a new `PgstacBackend` from a connection string.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac_server::PgstacBackend;
    /// # tokio_test::block_on(async {
    /// let backend = PgstacBackend::new_from_stringlike("postgresql://username:password@localhost:5432/postgis").await.unwrap();
    /// # })
    /// ```
    pub async fn new_from_stringlike(params: impl ToString) -> Result<PgstacBackend> {
        let config = ConnectConfig {
            dsn: Some(params.to_string()),
            ..Default::default()
        };
        let pool = PgstacPool::connect(config).await?;
        Ok(PgstacBackend { pool })
    }
}

impl ItemsClient for PgstacBackend {
    type Error = Error;

    async fn search(&self, search: Search) -> Result<ItemCollection> {
        self.pool.search(search).await.map_err(Error::from)
    }

    async fn item(&self, collection_id: &str, item_id: &str) -> Result<Option<Item>> {
        self.pool
            .item(collection_id, item_id)
            .await
            .map_err(Error::from)
    }
}

impl CollectionsClient for PgstacBackend {
    type Error = Error;

    async fn collections(&self) -> Result<Vec<Collection>> {
        self.pool.collections().await.map_err(Error::from)
    }

    async fn collection(&self, id: &str) -> Result<Option<Collection>> {
        self.pool.collection(id).await.map_err(Error::from)
    }
}

impl TransactionClient for PgstacBackend {
    type Error = Error;

    async fn add_collection(&mut self, collection: Collection) -> Result<()> {
        self.pool
            .add_collection(collection)
            .await
            .map_err(Error::from)
    }

    async fn add_item(&mut self, item: Item) -> Result<()> {
        self.pool.add_item(item).await.map_err(Error::from)
    }

    async fn add_items(&mut self, items: Vec<Item>) -> Result<()> {
        tracing::debug!("adding {} items using pgstac loading", items.len());
        let values = items
            .into_iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<Value>, _>>()?;
        self.pool
            .create_items(values, ConflictPolicy::Error)
            .await
            .map(|_| ())
            .map_err(Error::from)
    }
}

impl StreamItemsClient for PgstacBackend {
    type Error = Error;

    async fn search_stream(
        &self,
        search: Search,
    ) -> Result<impl Stream<Item = std::result::Result<stac::api::Item, Error>> + Send> {
        // UFCS: PgstacPool has an inherent writer-based `search_stream`; this selects the trait method.
        let stream = StreamItemsClient::search_stream(&self.pool, search)
            .await
            .map_err(Error::from)?;
        Ok(stream.map(|result| result.map_err(Error::from)))
    }
}

impl Backend for PgstacBackend {
    fn has_item_search(&self) -> bool {
        true
    }

    fn has_filter(&self) -> bool {
        true
    }
}
