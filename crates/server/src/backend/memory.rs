use crate::{Backend, DEFAULT_LIMIT, Error, Result};
use futures_core::Stream;
use serde_json::Map;
use stac::api::{
    CollectionsClient, ItemCollection, ItemsClient, Search, StreamItemsClient, TransactionClient,
    stream_pages,
};
use stac::{Collection, Item};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
};

/// A naive backend that stores collections and items in memory.
///
/// This backend is meant to be used for testing and toy servers, not for production.
#[derive(Clone, Debug)]
pub struct MemoryBackend {
    collections: Arc<RwLock<BTreeMap<String, Collection>>>,
    items: Arc<RwLock<HashMap<String, Vec<Item>>>>,
}

impl MemoryBackend {
    /// Creates a new memory backend.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::MemoryBackend;
    /// let backend = MemoryBackend::new();
    /// ```
    pub fn new() -> MemoryBackend {
        MemoryBackend {
            collections: Arc::new(RwLock::new(BTreeMap::new())),
            items: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl ItemsClient for MemoryBackend {
    type Error = Error;

    async fn search(&self, mut search: Search) -> Result<ItemCollection> {
        let items = self.items.read().unwrap();
        if search.collections.is_empty() {
            search.collections = items.keys().cloned().collect();
        }
        let mut item_references = Vec::new();
        for collection in &search.collections {
            if let Some(items) = items.get(collection) {
                item_references.extend(
                    items
                        .iter()
                        .filter(|item| search.matches(item).unwrap_or_default()),
                );
            }
        }
        let limit = search.limit.unwrap_or(DEFAULT_LIMIT).try_into()?;
        let skip = search
            .additional_fields
            .get("skip")
            .and_then(|skip| {
                skip.as_u64()
                    .or_else(|| skip.as_str().and_then(|skip| skip.parse::<u64>().ok()))
            })
            .unwrap_or_default()
            .try_into()?;
        let len = item_references.len();
        let items = item_references
            .into_iter()
            .skip(skip)
            .take(limit)
            .map(|item| stac::api::Item::try_from(item.clone()).map_err(Error::from))
            .collect::<Result<Vec<_>>>()?;
        let mut item_collection = ItemCollection::new(items)?;
        if len > item_collection.items.len() + skip {
            let mut next = Map::new();
            let _ = next.insert("skip".to_string(), (skip + limit).into());
            item_collection.next = Some(next);
        }
        if skip > 0 {
            let mut prev = Map::new();
            let skip = skip.saturating_sub(limit);
            let _ = prev.insert("skip".to_string(), skip.into());
            item_collection.prev = Some(prev);
        }
        Ok(item_collection)
    }

    async fn item(&self, collection_id: &str, item_id: &str) -> Result<Option<Item>> {
        let items = self.items.read().unwrap();
        Ok(items
            .get(collection_id)
            .and_then(|items| items.iter().find(|item| item.id == item_id).cloned()))
    }
}

impl CollectionsClient for MemoryBackend {
    type Error = Error;

    async fn collections(&self) -> Result<Vec<Collection>> {
        let collections = self.collections.read().unwrap();
        Ok(collections.values().cloned().collect())
    }

    async fn collection(&self, id: &str) -> Result<Option<Collection>> {
        let collections = self.collections.read().unwrap();
        Ok(collections.get(id).cloned())
    }
}

impl TransactionClient for MemoryBackend {
    type Error = Error;

    async fn add_collection(&mut self, collection: Collection) -> Result<()> {
        let mut collections = self.collections.write().unwrap();
        let _ = collections.insert(collection.id.clone(), collection);
        Ok(())
    }

    async fn add_item(&mut self, item: Item) -> Result<()> {
        if let Some(collection_id) = item.collection.clone() {
            if CollectionsClient::collection(self, &collection_id)
                .await?
                .is_none()
            {
                Err(Error::MemoryBackend(format!(
                    "no collection with id='{collection_id}'",
                )))
            } else {
                let mut items = self.items.write().unwrap();
                items.entry(collection_id).or_default().push(item);
                Ok(())
            }
        } else {
            Err(Error::MemoryBackend(format!(
                "collection not set on item: {}",
                item.id
            )))
        }
    }
}

impl StreamItemsClient for MemoryBackend {
    type Error = Error;

    async fn search_stream(
        &self,
        search: Search,
    ) -> Result<impl Stream<Item = std::result::Result<stac::api::Item, Error>> + Send> {
        let page = ItemsClient::search(self, search.clone()).await?;
        Ok(stream_pages(self.clone(), search, page))
    }
}

impl Backend for MemoryBackend {
    fn has_item_search(&self) -> bool {
        true
    }

    fn has_filter(&self) -> bool {
        false
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stac::api::{Items, StreamCollectionsClient};

    async fn populated_backend() -> MemoryBackend {
        let mut backend = MemoryBackend::new();
        backend
            .add_collection(Collection::new("collection-id", "a description"))
            .await
            .unwrap();
        backend
            .add_item(Item::new("item-a").collection("collection-id"))
            .await
            .unwrap();
        backend
            .add_item(Item::new("item-b").collection("collection-id"))
            .await
            .unwrap();
        backend
            .add_item(Item::new("item-c").collection("collection-id"))
            .await
            .unwrap();
        backend
    }

    #[tokio::test]
    async fn stream_items_across_pages_with_real_backend() {
        let backend = populated_backend().await;
        let search = Search::default().limit(1u64);
        let items = backend.collect_items(search).await.unwrap();
        assert_eq!(items.len(), 3);
    }

    #[tokio::test]
    async fn item_count_uses_streaming_path() {
        let backend = populated_backend().await;
        let search = Search::default().limit(1u64);
        let count = backend.item_count(search).await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn search_honors_numeric_skip_token() {
        let backend = populated_backend().await;
        let mut search = Search::default().limit(1u64);
        let _ = search
            .additional_fields
            .insert("skip".to_string(), 1.into());

        let page = backend.search(search).await.unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(
            page.items[0].get("id").and_then(|value| value.as_str()),
            Some("item-b")
        );
    }

    #[tokio::test]
    async fn collections_stream_with_real_backend() {
        let backend = populated_backend().await;
        let collections = backend.collect_collections().await.unwrap();
        assert_eq!(collections.len(), 1);
        assert_eq!(collections[0].id, "collection-id");
        let items = backend
            .items("collection-id", Items::default())
            .await
            .unwrap();
        assert_eq!(items.items.len(), 3);
    }
}
