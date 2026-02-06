use super::{ItemCollection, Items, Search};
use crate::{Collection, Error, Item};
use std::future::Future;

/// A client that can search for STAC items.
///
/// [`SearchClient::search`] is the only required method. [`SearchClient::item`]
/// and [`SearchClient::items`] have default implementations that delegate to
/// `search`.
pub trait SearchClient: Send + Sync {
    /// The error type for this client.
    type Error: Send;

    /// Searches for STAC items matching the given parameters.
    fn search(
        &self,
        search: Search,
    ) -> impl Future<Output = Result<ItemCollection, Self::Error>> + Send;

    /// Returns a single item from a collection.
    ///
    /// The default implementation uses [`SearchClient::search`] with `ids` and
    /// `collections` filters, then deserializes the result.
    fn item(
        &self,
        collection_id: &str,
        item_id: &str,
    ) -> impl Future<Output = Result<Option<Item>, Self::Error>> + Send
    where
        Self::Error: From<Error>,
    {
        async move {
            let search = Search::default()
                .ids(vec![item_id.to_string()])
                .collections(vec![collection_id.to_string()]);
            let mut item_collection = self.search(search).await?;
            if item_collection.items.len() == 1 {
                let api_item = item_collection.items.pop().expect("just checked length");
                let item: Item = serde_json::from_value(serde_json::Value::Object(api_item))
                    .map_err(Error::from)?;
                Ok(Some(item))
            } else {
                Ok(None)
            }
        }
    }

    /// Returns items from a collection.
    ///
    /// The default implementation converts the request to a [`Search`] scoped
    /// to the given collection and delegates to [`SearchClient::search`].
    fn items(
        &self,
        collection_id: &str,
        items: Items,
    ) -> impl Future<Output = Result<ItemCollection, Self::Error>> + Send {
        async move {
            let search = items.search_collection(collection_id);
            self.search(search).await
        }
    }
}

/// A client that can retrieve STAC collections.
///
/// [`CollectionSearchClient::collections`] is the only required method.
/// [`CollectionSearchClient::collection`] has a default implementation that
/// filters the result of `collections`.
pub trait CollectionSearchClient: Send + Sync {
    /// The error type for this client.
    type Error: Send;

    /// Returns all collections.
    fn collections(&self) -> impl Future<Output = Result<Vec<Collection>, Self::Error>> + Send;

    /// Returns a single collection by ID.
    ///
    /// The default implementation calls
    /// [`CollectionSearchClient::collections`] and finds the matching
    /// collection.
    fn collection(
        &self,
        id: &str,
    ) -> impl Future<Output = Result<Option<Collection>, Self::Error>> + Send {
        async move {
            let collections = self.collections().await?;
            Ok(collections.into_iter().find(|c| c.id == id))
        }
    }
}

/// A client that can create or add STAC items and collections.
///
/// [`TransactionClient::add_collection`] and
/// [`TransactionClient::add_item`] are required methods.
/// [`TransactionClient::add_items`] has a default implementation that calls
/// `add_item` in a loop.
pub trait TransactionClient: Send {
    /// The error type for this client.
    type Error: Send;

    /// Adds a collection.
    fn add_collection(
        &mut self,
        collection: Collection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Adds a single item.
    fn add_item(&mut self, item: Item) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Adds multiple items.
    ///
    /// The default implementation calls [`TransactionClient::add_item`] for
    /// each item sequentially.
    fn add_items(
        &mut self,
        items: Vec<Item>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async move {
            for item in items {
                self.add_item(item).await?;
            }
            Ok(())
        }
    }
}

/// A client that can search for STAC items returning Arrow record batches.
///
/// [`ArrowSearchClient::search_to_arrow`] is the only required method.
///
/// Unlike the other client traits, this trait does not require `Send + Sync`
/// and `search_to_arrow` is synchronous. This allows implementations to return
/// borrowing iterators (e.g. iterators that borrow from a database connection).
#[cfg(feature = "geoarrow")]
pub trait ArrowSearchClient {
    /// The error type for this client.
    type Error;

    /// The record batch reader type returned by [`ArrowSearchClient::search_to_arrow`].
    type RecordBatchStream<'a>: arrow_array::RecordBatchReader
    where
        Self: 'a;

    /// Searches for STAC items, returning results as Arrow record batches.
    fn search_to_arrow(&self, search: Search) -> Result<Self::RecordBatchStream<'_>, Self::Error>;
}
