#[cfg(feature = "duckdb")]
mod duckdb;
mod memory;
#[cfg(feature = "pgstac")]
mod pgstac;

use crate::Result;
#[cfg(feature = "duckdb")]
pub use duckdb::DuckdbBackend;
pub use memory::MemoryBackend;
#[cfg(feature = "pgstac")]
pub use pgstac::PgstacBackend;
use stac::api::{ItemCollection, Items, Search};
use stac::{Collection, Item};
use std::future::Future;

/// Storage backend for a STAC API.
pub trait Backend: Clone + Sync + Send + 'static {
    /// Returns true if this backend has item search capabilities.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{MemoryBackend, Backend};
    ///
    /// assert!(MemoryBackend::new().has_item_search());
    /// ```
    fn has_item_search(&self) -> bool;

    /// Returns true if this backend has [filter](https://github.com/stac-api-extensions/filter) capabilities.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{MemoryBackend, Backend};
    ///
    /// assert!(!MemoryBackend::new().has_filter());
    /// ```
    fn has_filter(&self) -> bool;

    /// Returns all collections.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{MemoryBackend, Backend};
    /// let backend = MemoryBackend::new();
    /// # tokio_test::block_on(async {
    /// let collections = backend.collections().await.unwrap();
    /// assert!(collections.is_empty());
    /// # })
    /// ```
    fn collections(&self) -> impl Future<Output = Result<Vec<Collection>>> + Send;

    /// Returns a single collection.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{MemoryBackend, Backend};
    /// let backend = MemoryBackend::new();
    /// # tokio_test::block_on(async {
    /// let collection = backend.collection("does-not-exist").await.unwrap();
    /// assert!(collection.is_none());
    /// # })
    /// ```
    fn collection(&self, id: &str) -> impl Future<Output = Result<Option<Collection>>> + Send;

    /// Adds a collection.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Collection;
    /// use stac_server::{MemoryBackend, Backend};
    ///
    /// let mut backend = MemoryBackend::new();
    /// # tokio_test::block_on(async {
    /// backend.add_collection(Collection::new("an-id", "a description")).await.unwrap();
    /// # })
    /// ```
    fn add_collection(&mut self, collection: Collection)
    -> impl Future<Output = Result<()>> + Send;

    /// Adds an item.
    ///
    /// If the item doesn't have its `collection` field set, or a collection
    /// with that id does not exist in the backend, throws an error.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Collection, Item};
    /// use stac_server::{MemoryBackend, Backend};
    ///
    /// let mut backend = MemoryBackend::new();
    /// # tokio_test::block_on(async {
    /// assert!(backend.add_item(Item::new("item-id")).await.is_err());
    ///
    /// backend.add_collection(Collection::new("collection-id", "a description")).await.unwrap();
    /// backend.add_item(Item::new("item-id").collection("collection-id")).await.unwrap();
    /// # })
    /// ```
    fn add_item(&mut self, item: Item) -> impl Future<Output = Result<()>> + Send;

    /// Adds multiple items.
    fn add_items(&mut self, items: Vec<Item>) -> impl Future<Output = Result<()>> + Send {
        tracing::debug!("adding {} items using naïve loading", items.len());
        async move {
            for item in items {
                self.add_item(item).await?;
            }
            Ok(())
        }
    }

    /// Retrieves items for a given collection.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Collection, Item};
    /// use stac::api::Items;
    /// use stac_server::{MemoryBackend, Backend};
    ///
    /// let mut backend = MemoryBackend::new();
    /// # tokio_test::block_on(async {
    /// backend.add_collection(Collection::new("collection-id", "a description")).await.unwrap();
    /// backend.add_item(Item::new("item-id").collection("collection-id")).await.unwrap();
    /// let items = backend.items("collection-id", Items::default()).await.unwrap();
    /// # })
    /// ```
    fn items(
        &self,
        collection_id: &str,
        items: Items,
    ) -> impl Future<Output = Result<Option<ItemCollection>>> + Send;

    /// Retrieves an item from a collection.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Collection, Item};
    /// use stac_server::{MemoryBackend, Backend};
    ///
    /// let mut backend = MemoryBackend::new();
    /// # tokio_test::block_on(async {
    /// backend.add_collection(Collection::new("collection-id", "a description")).await.unwrap();
    /// backend.add_item(Item::new("item-id").collection("collection-id")).await.unwrap();
    /// let item = backend.item("collection-id", "item-id").await.unwrap().unwrap();
    /// # })
    /// ```
    fn item(
        &self,
        collection_id: &str,
        item_id: &str,
    ) -> impl Future<Output = Result<Option<Item>>> + Send;

    /// Searches a backend.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Search;
    /// use stac_server::{MemoryBackend, Backend};
    ///
    /// let mut backend = MemoryBackend::new();
    /// # tokio_test::block_on(async {
    /// let item_collection = backend.search(Search::default()).await.unwrap();
    /// # })
    /// ```
    fn search(&self, search: Search) -> impl Future<Output = Result<ItemCollection>> + Send;
}
