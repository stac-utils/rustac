use super::{ItemCollection, Items, Search};
use crate::{Collection, Error, Item};
#[cfg(feature = "async")]
use futures_core::Stream;
use std::future::Future;

/// A client that can fetch STAC items.
///
/// [`ItemsClient::search`] is the only required method. This trait covers the
/// `/search`, `/collections/{id}/items`, and
/// `/collections/{id}/items/{item_id}` endpoints — all expressed as
/// constrained [`Search`] queries. [`ItemsClient::item`] and
/// [`ItemsClient::items`] have default implementations that delegate to
/// [`ItemsClient::search`].
pub trait ItemsClient: Send + Sync {
    /// The error type for this client.
    type Error: Send;

    /// Searches for STAC items matching the given parameters.
    fn search(
        &self,
        search: Search,
    ) -> impl Future<Output = Result<ItemCollection, Self::Error>> + Send;

    /// Returns a single item from a collection.
    ///
    /// The default implementation uses [`ItemsClient::search`] with `ids` and
    /// `collections` filters, then deserializes the result.
    ///
    /// Override this method if your backend has a native O(1) point-lookup for
    /// `GET /collections/{id}/items/{item_id}`. Both the `pgstac` and `memory`
    /// backends override this.
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
    /// to the given collection and delegates to [`ItemsClient::search`].
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
/// [`CollectionsClient::collections`] is the only required method.
/// [`CollectionsClient::collection`] has a default implementation that
/// scans all collections. Override it for O(1) lookups.
pub trait CollectionsClient: Send + Sync {
    /// The error type for this client.
    type Error: Send;

    /// Returns all collections.
    fn collections(&self) -> impl Future<Output = Result<Vec<Collection>, Self::Error>> + Send;

    /// Returns a single collection by ID.
    ///
    /// The default implementation scans all collections. Override this method
    /// if your backend has an O(1) indexed lookup (e.g. a hash map or database
    /// index). Both the `pgstac` and `memory` backends override this.
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

/// A client that can return STAC items as Arrow record batches.
///
/// [`ArrowItemsClient::search_to_arrow`] is the only required method.
/// [`ArrowItemsClient::items_to_arrow`] has a default implementation that
/// delegates to `search_to_arrow`.
///
/// Unlike the other client traits, this trait does not require `Send + Sync`
/// and `search_to_arrow` is synchronous. This allows implementations to return
/// borrowing iterators (e.g. iterators that borrow from a database connection).
#[cfg(feature = "geoarrow")]
pub trait ArrowItemsClient {
    /// The error type for this client.
    type Error;

    /// The record batch reader type returned by [`ArrowItemsClient::search_to_arrow`].
    type RecordBatchStream<'a>: arrow_array::RecordBatchReader
    where
        Self: 'a;

    /// Searches for STAC items, returning results as Arrow record batches.
    fn search_to_arrow(&self, search: Search) -> Result<Self::RecordBatchStream<'_>, Self::Error>;

    /// Returns items from a collection as Arrow record batches.
    ///
    /// The default implementation calls
    /// [`ArrowItemsClient::search_to_arrow`] with a [`Search`] scoped to the
    /// given collection. Override this method if your backend can serve
    /// collection items in Arrow more efficiently than routing through search.
    fn items_to_arrow(
        &self,
        collection_id: &str,
        items: Items,
    ) -> Result<Self::RecordBatchStream<'_>, Self::Error> {
        self.search_to_arrow(items.search_collection(collection_id))
    }
}

#[cfg(feature = "async")]
/// A client that can stream STAC items across all pages.
///
/// [`StreamItemsClient::search_stream`] is the only required method. The
/// default methods [`StreamItemsClient::collect_items`],
/// [`StreamItemsClient::item_count`], and [`StreamItemsClient::items_stream`]
/// are built on top of it.
///
/// `Stream` is the async equivalent of `Iterator` — think of
/// `StreamExt::next().await` as the async `Iterator::next()`. You cannot
/// implement `std::iter::Iterator` here because `Iterator::next` is
/// synchronous; blocking an async runtime on each item would defeat the
/// purpose. For blocking / sync contexts, wrap your runtime in a
/// `tokio::runtime::Handle` or use the `BlockingClient` in `stac-io`.
///
/// # Examples
///
/// Stream items lazily (low memory):
///
/// ```no_run
/// use futures::StreamExt;
/// use stac::api::{Search, StreamItemsClient};
///
/// async fn example<C>(client: C)
/// where
///     C: StreamItemsClient,
///     C::Error: std::fmt::Debug,
/// {
///     let search = Search::default();
///     let stream = client.search_stream(search).await.unwrap();
///     futures::pin_mut!(stream);
///     while let Some(item) = stream.next().await {
///         println!("Got item: {:?}", item.unwrap());
///     }
/// }
/// ```
///
/// Or collect all into a `Vec` using the default method:
///
/// ```no_run
/// use stac::api::{Search, StreamItemsClient};
///
/// async fn example<C>(client: C)
/// where
///     C: StreamItemsClient,
///     C::Error: std::fmt::Debug,
/// {
///     let items = client.collect_items(Search::default()).await.unwrap();
///     println!("Total: {}", items.len());
/// }
/// ```
pub trait StreamItemsClient: Send + Sync {
    /// The error type for this client.
    type Error: Send;

    /// Searches for STAC items, returning a stream of items.
    ///
    /// This method paginates through all pages of results. For a single page,
    /// use [`ItemsClient::search`].
    fn search_stream(
        &self,
        search: Search,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<super::Item, Self::Error>> + Send, Self::Error>,
    > + Send;

    /// Collects all items from all pages into a `Vec`.
    ///
    /// Drives [`search_stream`](StreamItemsClient::search_stream) to
    /// completion. Prefer [`search_stream`](StreamItemsClient::search_stream)
    /// when working with large result sets to avoid loading everything into
    /// memory at once.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac::api::{Search, StreamItemsClient};
    ///
    /// async fn example<C>(client: C)
    /// where
    ///     C: StreamItemsClient,
    ///     C::Error: std::fmt::Debug,
    /// {
    ///     let items = client.collect_items(Search::default()).await.unwrap();
    ///     println!("Got {} items", items.len());
    /// }
    /// ```
    fn collect_items(
        &self,
        search: Search,
    ) -> impl Future<Output = Result<Vec<super::Item>, Self::Error>> + Send {
        async move {
            use futures::TryStreamExt as _;
            let stream = self.search_stream(search).await?;
            futures::pin_mut!(stream);
            stream.try_collect().await
        }
    }

    /// Counts all items across all pages without collecting them.
    ///
    /// More memory-efficient than [`collect_items`](StreamItemsClient::collect_items)
    /// when only the count is needed. Each item is deserialized and immediately
    /// discarded.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac::api::{Search, StreamItemsClient};
    ///
    /// async fn example<C>(client: C)
    /// where
    ///     C: StreamItemsClient,
    ///     C::Error: std::fmt::Debug,
    /// {
    ///     let count = client.item_count(Search::default()).await.unwrap();
    ///     println!("Total items: {count}");
    /// }
    /// ```
    fn item_count(
        &self,
        search: Search,
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send {
        async move {
            use futures::TryStreamExt as _;
            let stream = self.search_stream(search).await?;
            futures::pin_mut!(stream);
            let mut count = 0usize;
            stream
                .try_for_each(|_| {
                    count += 1;
                    async { Ok(()) }
                })
                .await?;
            Ok(count)
        }
    }

    /// Streams all items belonging to a collection, paginating through all pages.
    ///
    /// The default implementation calls
    /// [`StreamItemsClient::search_stream`] with a [`Search`] scoped to the
    /// given collection. Override this method if your backend has a dedicated
    /// link-following items endpoint (e.g. `stac-io`'s HTTP client).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use futures::StreamExt;
    /// use stac::api::{Items, StreamItemsClient};
    ///
    /// async fn example<C>(client: C)
    /// where
    ///     C: StreamItemsClient,
    ///     C::Error: std::fmt::Debug,
    /// {
    ///     let stream = client.items_stream("my-collection", Items::default()).await.unwrap();
    ///     futures::pin_mut!(stream);
    ///     while let Some(item) = stream.next().await {
    ///         println!("Got item: {:?}", item.unwrap());
    ///     }
    /// }
    /// ```
    fn items_stream(
        &self,
        collection_id: &str,
        items: Items,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<super::Item, Self::Error>> + Send, Self::Error>,
    > + Send {
        async move {
            let search = items.search_collection(collection_id);
            self.search_stream(search).await
        }
    }
}

/// A client that can fetch a single page of STAC collections with cursor
/// pagination.
///
/// This is the paginated counterpart to [`CollectionsClient`]. Implement this
/// trait when your backend supports cursor-based `/collections` pagination
/// (e.g. a future pgstac version, or any backend that returns a `next_token`
/// alongside the collection list).
///
/// # Blanket impl
///
/// Any `T: PagedCollectionsClient + Clone + Send + Sync` automatically
/// implements [`StreamCollectionsClient`] by following the cursor chain.
/// This blanket takes priority over the simpler
/// `CollectionsClient → StreamCollectionsClient` blanket for types that
/// implement `PagedCollectionsClient`.
///
/// # Examples
///
/// ```no_run
/// use stac::Collection;
/// use stac::api::PagedCollectionsClient;
///
/// struct MyBackend;
///
/// impl PagedCollectionsClient for MyBackend {
///     type Error = std::convert::Infallible;
///
///     async fn collections_page(
///         &self,
///         token: Option<String>,
///     ) -> Result<(Vec<Collection>, Option<String>), Self::Error> {
///         // fetch one page; return (collections, next_token)
///         Ok((vec![], None))
///     }
/// }
/// ```
pub trait PagedCollectionsClient: Send + Sync {
    /// The error type for this client.
    type Error: Send;

    /// Fetches one page of collections.
    ///
    /// `token` is the cursor returned by the previous call, or `None` for the
    /// first page. Returns the collections on this page and an optional cursor
    /// for the next page (`None` means no more pages).
    fn collections_page(
        &self,
        token: Option<String>,
    ) -> impl Future<Output = Result<(Vec<Collection>, Option<String>), Self::Error>> + Send;
}

#[cfg(feature = "async")]
/// A client that can stream STAC collections.
///
/// [`StreamCollectionsClient::collections_stream`] is the only required
/// method. This mirrors the naming convention of [`StreamItemsClient`]:
/// the prefix `Stream` indicates a streaming variant of its non-streaming
/// counterpart ([`CollectionsClient`]).
///
/// # Blanket impl
///
/// Any `T: CollectionsClient + Clone + Send + Sync` automatically implements
/// this trait by eagerly fetching all collections in one call and yielding
/// them as a stream.
///
/// For cursor-paginated backends, implement [`PagedCollectionsClient`] and
/// call [`stream_pages_collections`](crate::api::stream_pages_collections)
/// inside your own `StreamCollectionsClient` impl — the same pattern as
/// [`stream_pages`](crate::api::stream_pages) for items.
pub trait StreamCollectionsClient: Send + Sync {
    /// The error type for this client.
    type Error: Send;

    /// Returns all collections as a stream.
    fn collections_stream(
        &self,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<Collection, Self::Error>> + Send, Self::Error>,
    > + Send;

    /// Collects all collections into a `Vec`.
    ///
    /// Convenience wrapper around
    /// [`collections_stream`](StreamCollectionsClient::collections_stream).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac::api::StreamCollectionsClient;
    ///
    /// async fn example<C>(client: C)
    /// where
    ///     C: StreamCollectionsClient,
    ///     C::Error: std::fmt::Debug,
    /// {
    ///     let collections = client.collect_collections().await.unwrap();
    ///     println!("Got {} collections", collections.len());
    /// }
    /// ```
    fn collect_collections(
        &self,
    ) -> impl Future<Output = Result<Vec<Collection>, Self::Error>> + Send {
        async move {
            use futures::TryStreamExt as _;
            let stream = self.collections_stream().await?;
            futures::pin_mut!(stream);
            stream.try_collect().await
        }
    }
}
