use crate::{Backend, Error, Result};
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use pgstac::Pgstac;
use rustls::{ClientConfig, RootCertStore};
use serde_json::Map;
use stac::api::{ItemCollection, Items, Search};
use stac::{Collection, Item};
use tokio_postgres::{
    Socket,
    tls::{MakeTlsConnect, TlsConnect},
};
use tokio_postgres_rustls::MakeRustlsConnect;

/// A backend for a [pgstac](https://github.com/stac-utils/pgstac) database.
#[derive(Clone, Debug)]
pub struct PgstacBackend<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pool: Pool<PostgresConnectionManager<Tls>>,
}

impl PgstacBackend<MakeRustlsConnect> {
    /// Creates a new PgstacBackend from a string-like configuration.
    ///
    /// This will use an unverified tls. To provide your own tls, use
    /// [PgstacBackend::new_from_stringlike_and_tls].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac_server::PgstacBackend;
    /// # tokio_test::block_on(async {
    /// let backend = PgstacBackend::new_from_stringlike("postgresql://username:password@localhost:5432/postgis").await.unwrap();
    /// # })
    /// ```
    pub async fn new_from_stringlike(
        params: impl ToString,
    ) -> Result<PgstacBackend<MakeRustlsConnect>> {
        let _ = rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("The default provider should install without problems");
        let config = ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
        let tls = MakeRustlsConnect::new(config);
        PgstacBackend::new_from_stringlike_and_tls(params, tls).await
    }
}

impl<Tls> PgstacBackend<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    /// Creates a new PgstacBackend from a string-like configuration and a tls.
    pub async fn new_from_stringlike_and_tls(
        params: impl ToString,
        tls: Tls,
    ) -> Result<PgstacBackend<Tls>> {
        let params = params.to_string();
        let connection_manager = PostgresConnectionManager::new_from_stringlike(params, tls)?;
        let pool = Pool::builder().build(connection_manager).await?;
        Ok(PgstacBackend { pool })
    }
}

impl<Tls> Backend for PgstacBackend<Tls>
where
    Tls: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
    <Tls as MakeTlsConnect<Socket>>::Stream: Send + Sync,
    <Tls as MakeTlsConnect<Socket>>::TlsConnect: Send,
    <<Tls as MakeTlsConnect<Socket>>::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn has_item_search(&self) -> bool {
        true
    }

    fn has_filter(&self) -> bool {
        true
    }

    async fn add_collection(&mut self, collection: Collection) -> Result<()> {
        let client = self.pool.get().await?;
        client.add_collection(collection).await.map_err(Error::from)
    }

    async fn collection(&self, id: &str) -> Result<Option<Collection>> {
        let client = self.pool.get().await?;
        let value = client.collection(id).await?;
        value
            .map(serde_json::from_value)
            .transpose()
            .map_err(Error::from)
    }

    async fn collections(&self) -> Result<Vec<Collection>> {
        let client = self.pool.get().await?;
        let values = client.collections().await?;
        values
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(Error::from))
            .collect()
    }

    async fn add_item(&mut self, item: Item) -> Result<()> {
        let client = self.pool.get().await?;
        client.add_item(item).await.map_err(Error::from)
    }

    async fn add_items(&mut self, items: Vec<Item>) -> Result<()> {
        tracing::debug!("adding {} items using pgstac loading", items.len());
        let client = self.pool.get().await?;
        client.add_items(&items).await.map_err(Error::from)
    }

    async fn items(&self, collection_id: &str, items: Items) -> Result<Option<ItemCollection>> {
        // TODO should we check for collection existence?
        let search = items.search_collection(collection_id);
        self.search(search).await.map(Some)
    }

    async fn item(&self, collection_id: &str, item_id: &str) -> Result<Option<Item>> {
        let client = self.pool.get().await?;
        let value = client.item(item_id, Some(collection_id)).await?;
        value
            .map(serde_json::from_value)
            .transpose()
            .map_err(Error::from)
    }

    async fn search(&self, search: Search) -> Result<ItemCollection> {
        let client = self.pool.get().await?;
        let page = client.search(search).await?;
        let next_token = page.next_token();
        let prev_token = page.prev_token();
        let mut item_collection = ItemCollection::new(page.features)?;
        if let Some(next_token) = next_token {
            let mut next = Map::new();
            let _ = next.insert("token".into(), next_token.into());
            item_collection.next = Some(next);
        }
        if let Some(prev_token) = prev_token {
            let mut prev = Map::new();
            let _ = prev.insert("token".into(), prev_token.into());
            item_collection.prev = Some(prev);
        }
        item_collection.context = page.context;
        Ok(item_collection)
    }
}
