use crate::{Backend, Error, Result, DEFAULT_LIMIT};
use async_trait::async_trait;
use serde_json::Map;
use stac::{Collection, Item};
use stac_api::{ItemCollection, Items, Search};
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

#[async_trait]
impl Backend for MemoryBackend {
    fn has_item_search(&self) -> bool {
        cfg!(feature = "memory-item-search")
    }

    async fn collections(&self) -> Result<Vec<Collection>> {
        let collections = self.collections.read().unwrap();
        Ok(collections.values().cloned().collect())
    }

    async fn collection(&self, id: &str) -> Result<Option<Collection>> {
        let collections = self.collections.read().unwrap();
        Ok(collections.get(id).cloned())
    }

    async fn add_collection(&mut self, collection: Collection) -> Result<()> {
        let mut collections = self.collections.write().unwrap();
        let _ = collections.insert(collection.id.clone(), collection);
        Ok(())
    }

    async fn add_item(&mut self, item: Item) -> Result<()> {
        if let Some(collection_id) = item.collection.clone() {
            if self.collection(&collection_id).await?.is_none() {
                Err(Error::MemoryBackend(format!(
                    "no collection with id='{}'",
                    collection_id
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

    async fn items(&self, collection_id: &str, items: Items) -> Result<Option<ItemCollection>> {
        {
            let collections = self.collections.read().unwrap();
            if !collections.contains_key(collection_id) {
                return Ok(None);
            }
        };
        let search = items.into_search(collection_id);
        self.search(search).await.map(Some)
    }

    async fn item(&self, collection_id: &str, item_id: &str) -> Result<Option<Item>> {
        let items = self.items.read().unwrap();
        Ok(items
            .get(collection_id)
            .and_then(|items| items.iter().find(|item| item.id == item_id).cloned()))
    }

    async fn search(&self, mut search: Search) -> Result<ItemCollection> {
        let items = self.items.read().unwrap();
        if search.collections.is_none() {
            search.collections = Some(items.keys().cloned().collect());
        }
        let mut item_references = Vec::new();
        for collection in search.collections.as_ref().unwrap() {
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
            .and_then(|skip| skip.as_str())
            .and_then(|skip| skip.parse::<u64>().ok())
            .unwrap_or_default()
            .try_into()?;
        let len = item_references.len();
        let items = item_references
            .into_iter()
            .skip(skip)
            .take(limit)
            .map(|item| stac_api::Item::try_from(item.clone()).map_err(Error::from))
            .collect::<Result<Vec<_>>>()?;
        let mut item_collection = ItemCollection::new(items)?;
        if len > item_collection.items.len() + skip {
            let mut next = Map::new();
            let _ = next.insert("skip".to_string(), (skip + limit).into());
            item_collection.next = Some(next);
        }
        if skip > 0 {
            let mut prev = Map::new();
            let skip = if skip > limit { skip - limit } else { 0 };
            let _ = prev.insert("skip".to_string(), skip.into());
            item_collection.prev = Some(prev);
        }
        Ok(item_collection)
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}
