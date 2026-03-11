use super::{ItemCollection, Items, Search};
use crate::{Collection, Error, Item};
use std::future::Future;

#[cfg(feature = "stream")]
use std::pin::Pin;

/// A STAC API Item — a JSON map that may be a full or partial [`Item`].
///
/// Re-exported here to avoid importing from `super` in trait signatures.
#[cfg(feature = "stream")]
type ApiItem = super::Item;

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

/// A client that can produce a paginated stream of STAC API items.
///
/// This is the streaming counterpart to [`SearchClient::search`]. While
/// `SearchClient` returns a single page, `StreamSearchClient` returns a
/// stream that automatically pages through all results.
///
/// For types that already implement [`SearchClient`], a default paginating
/// stream can be obtained via the [`stream_search_pages`] helper function.
///
/// # Examples
///
/// ```no_run
/// use stac::api::{Search, StreamSearchClient};
/// use futures::StreamExt;
///
/// # async fn example(client: impl StreamSearchClient<Error = stac::Error>) {
/// let mut stream = client.search_stream(Search::default()).await.unwrap();
/// while let Some(result) = stream.next().await {
///     let item = result.unwrap();
///     println!("{:?}", item.get("id"));
/// }
/// # }
/// ```
#[cfg(feature = "stream")]
pub trait StreamSearchClient: Send + Sync {
    /// The error type for this client.
    type Error: Send;

    /// Returns a stream of API items by paging through results.
    fn search_stream(
        &self,
        search: Search,
    ) -> impl Future<
        Output = Result<
            Pin<Box<dyn futures::Stream<Item = Result<ApiItem, Self::Error>> + Send + '_>>,
            Self::Error,
        >,
    > + Send;
}

/// Creates a paginated stream from a [`SearchClient`].
///
/// This helper repeatedly calls [`SearchClient::search`], yielding each item
/// from each page. Pagination tokens are carried forward by merging
/// [`ItemCollection::next`] into [`Search::items::additional_fields`].
///
/// Use this inside a [`StreamSearchClient`] implementation to get token-based
/// pagination for free:
///
/// ```ignore
/// impl StreamSearchClient for MyBackend {
///     type Error = MyError;
///     async fn search_stream(&self, search: Search)
///         -> Result<Pin<Box<dyn Stream<Item = Result<ApiItem, Self::Error>> + Send + '_>>, Self::Error>
///     {
///         Ok(stream_search_pages(self, search))
///     }
/// }
/// ```
#[cfg(feature = "stream")]
pub fn stream_search_pages<'a, C>(
    client: &'a C,
    search: Search,
) -> Pin<Box<dyn futures::Stream<Item = Result<ApiItem, C::Error>> + Send + 'a>>
where
    C: SearchClient + ?Sized,
{
    use futures::stream;

    let stream = stream::unfold(
        (client, Some(search), Vec::<ApiItem>::new()),
        |(client, next_search_opt, mut buffer)| async move {
            // Yield buffered items first.
            if !buffer.is_empty() {
                let item = buffer.remove(0);
                return Some((Ok(item), (client, next_search_opt, buffer)));
            }

            // Fetch the next page.
            let search = next_search_opt?;
            match client.search(search.clone()).await {
                Ok(page) => {
                    if page.items.is_empty() {
                        return None;
                    }
                    let next = page.next.and_then(|next_params| {
                        let mut next_search = search;
                        for (k, v) in next_params {
                            let _ = next_search.items.additional_fields.insert(k, v);
                        }
                        Some(next_search)
                    });
                    let mut items = page.items;
                    let first = items.remove(0);
                    Some((Ok(first), (client, next, items)))
                }
                Err(e) => Some((Err(e), (client, None, Vec::new()))),
            }
        },
    );

    Box::pin(stream)
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
