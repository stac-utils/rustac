//! Generic adapters between search client traits.
//!
//! These adapters allow any implementation of one search trait to be used as
//! another. For example, any [`ItemsClient`] can be wrapped in
//! [`PagedItemsStream`] to gain a [`StreamItemsClient`] implementation that
//! paginates via the [`ItemCollection::next`] field.
//!
//! # Blanket impls
//!
//! Several conversions are provided automatically via blanket implementations:
//!
//! - Any `T: CollectionsClient + Clone + Send + Sync` automatically implements
//!   [`StreamCollectionsClient`] by eagerly fetching all collections and
//!   yielding them as a stream (for non-paginated backends).
//!
//! - Any `T: PagedCollectionsClient + Clone + Send + Sync` automatically
//!   implements [`StreamCollectionsClient`] by following the cursor chain
//!   page by page (for cursor-paginated backends).
//!
//! - When the `geoarrow` feature is enabled, any
//!   `T: ArrowItemsClient + Send + Sync` automatically implements both
//!   [`ItemsClient`] and [`StreamItemsClient`] by collecting record batches
//!   synchronously.
//!
//! # Why `ItemsClient + Clone` does not have a blanket `StreamItemsClient`
//!
//! Rust's coherence rules prevent adding
//! `impl<T: ItemsClient + Clone + Send + Sync> StreamItemsClient for T`
//! because it would overlap with the `ArrowItemsClient` blanket (a future type
//! could implement both). Use [`PagedItemsStream`] explicitly, or implement
//! [`StreamItemsClient`] directly on your type using [`stream_pages_generic`].

use super::client::{
    CollectionsClient, ItemsClient, PagedCollectionsClient, StreamCollectionsClient,
    StreamItemsClient,
};
use super::{Item, ItemCollection, Search};
use crate::Collection;
use async_stream::try_stream;
use futures_core::Stream;
use std::future::Future;

/// Error type for adapters that may produce errors from both the wrapped client
/// and from STAC core operations (e.g. Arrow decode errors).
///
/// This is the error type for [`ItemsClient`] and [`StreamItemsClient`] when
/// derived from an [`ArrowItemsClient`] blanket impl. Decoding record batches
/// to items can produce a [`crate::Error`] independently of any client error.
#[cfg(feature = "geoarrow")]
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AdapterError<E: std::error::Error + Send> {
    /// Error from the wrapped client.
    #[error(transparent)]
    Client(E),

    /// Error from STAC core (e.g. Arrow decode failure).
    #[error(transparent)]
    Stac(#[from] crate::Error),
}

// ---------------------------------------------------------------------------
// PagedItemsStream

/// Adapter that wraps an [`ItemsClient`] to provide [`StreamItemsClient`].
///
/// Pagination uses the [`ItemCollection::next`] field, which is set by
/// token/skip-based backends such as pgstac (cursor tokens) and the in-memory
/// backend (skip offsets). When `next` is `Some`, its fields are merged into
/// `additional_fields` of the subsequent search request.
///
/// For link-based HTTP pagination (STAC API over HTTP), use the IO crate's
/// native [`StreamItemsClient`] implementation instead, which follows `rel="next"`
/// links and correctly carries HTTP method, headers, and body.
#[derive(Debug)]
pub struct PagedItemsStream<T> {
    inner: T,
}

impl<T> PagedItemsStream<T> {
    /// Wraps a search client.
    pub fn new(inner: T) -> Self {
        Self { inner }
    }

    /// Returns the wrapped client.
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> StreamItemsClient for PagedItemsStream<T>
where
    T: ItemsClient + Clone + Send + Sync,
    T::Error: Send,
{
    type Error = T::Error;

    fn search_stream(
        &self,
        search: Search,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<Item, T::Error>> + Send, T::Error>> + Send
    {
        let client = self.inner.clone();
        async move {
            let page = client.search(search.clone()).await?;
            Ok(stream_pages_generic(client, search, page))
        }
    }
}

/// Streams items from an [`ItemsClient`] using token/skip-based pagination.
///
/// Yields every item from each successive page. When [`ItemCollection::next`]
/// is `Some`, its entries are merged into `additional_fields` of the next
/// search request (overwriting any previous pagination values). The stream ends
/// when the page is empty or `next` is `None`.
pub fn stream_pages_generic<T>(
    client: T,
    initial_search: Search,
    initial_page: ItemCollection,
) -> impl Stream<Item = Result<Item, T::Error>> + Send
where
    T: ItemsClient + Clone + Send + Sync,
    T::Error: Send,
{
    try_stream! {
        let mut page = initial_page;
        let mut current_search = initial_search;
        loop {
            if page.items.is_empty() {
                break;
            }
            let next = page.next.clone();
            for item in page.items {
                yield item;
            }
            match next {
                Some(next_fields) => {
                    // Merge pagination token/offset into the search's additional
                    // fields, overwriting any previous pagination entry.
                    current_search.additional_fields.extend(next_fields);
                    page = client.search(current_search.clone()).await?;
                }
                None => break,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Blanket: CollectionsClient + Clone + Send + Sync → StreamCollectionsClient
//
// Collections are fetched in a single call (no pagination is defined in the
// current STAC API spec for most backends), so the blanket unconditionally
// collects then yields.
//
// For cursor-paginated backends, implement PagedCollectionsClient and call
// stream_pages_collections_generic inside your own StreamCollectionsClient
// impl — exactly the same pattern as stream_pages_generic for items.

impl<T> StreamCollectionsClient for T
where
    T: CollectionsClient + Clone + Send + Sync,
    T::Error: Send,
{
    type Error = T::Error;

    fn collections_stream(
        &self,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<Collection, T::Error>> + Send, T::Error>,
    > + Send {
        let client = self.clone();
        async move {
            let collections = client.collections().await?;
            Ok(futures::stream::iter(collections.into_iter().map(Ok)))
        }
    }
}

/// Streams collections from a [`PagedCollectionsClient`] using cursor pagination.
///
/// Yields every collection from each successive page. When the returned token
/// is `Some`, it is passed to the next `collections_page` call. The stream
/// ends when the page is empty or the token is `None`.
///
/// This is the collections equivalent of [`stream_pages_generic`].
pub fn stream_pages_collections_generic<T>(
    client: T,
    initial_page: Vec<Collection>,
    initial_token: Option<String>,
) -> impl Stream<Item = Result<Collection, T::Error>> + Send
where
    T: PagedCollectionsClient + Clone + Send + Sync,
    T::Error: Send,
{
    try_stream! {
        let mut page = initial_page;
        let mut cursor = initial_token;
        loop {
            if page.is_empty() {
                break;
            }
            let next_cursor = cursor;
            for collection in page {
                yield collection;
            }
            match next_cursor {
                Some(t) => {
                    let (next_page, next_t) = client.collections_page(Some(t)).await?;
                    page = next_page;
                    cursor = next_t;
                }
                None => break,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Arrow adapters (geoarrow feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "geoarrow")]
mod geoarrow_adapters {
    use super::super::client::{ArrowItemsClient, ItemsClient, StreamItemsClient};
    use super::super::{Item, ItemCollection, Search};
    use super::AdapterError;
    use futures_core::Stream;
    use std::future::Future;

    // -----------------------------------------------------------------------
    // RecordBatchReaderAdapter
    // -----------------------------------------------------------------------

    /// Generic adapter that wraps any `Iterator<Item = Result<RecordBatch, E>>`
    /// and a schema into an [`arrow_array::RecordBatchReader`].
    ///
    /// This is the standard bridge between a crate-specific iterator error type
    /// and the `ArrowError` required by `RecordBatchReader`. Every
    /// [`ArrowItemsClient`] implementation that builds on a sync iterator (e.g.
    /// DuckDB's `Statement::step`) can use this instead of hand-rolling the same
    /// three-line boilerplate.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "geoarrow")]
    /// # {
    /// use stac::api::RecordBatchReaderAdapter;
    /// use arrow_schema::SchemaRef;
    ///
    /// fn wrap_iter<I, E>(iter: I, schema: SchemaRef) -> RecordBatchReaderAdapter<I>
    /// where
    ///     I: Iterator<Item = Result<arrow_array::RecordBatch, E>>,
    ///     E: std::error::Error + Send + Sync + 'static,
    /// {
    ///     RecordBatchReaderAdapter::new(iter, schema)
    /// }
    /// # }
    /// ```
    pub struct RecordBatchReaderAdapter<I> {
        inner: I,
        schema: arrow_schema::SchemaRef,
    }

    impl<I> std::fmt::Debug for RecordBatchReaderAdapter<I> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("RecordBatchReaderAdapter")
                .field("schema", &self.schema)
                .finish_non_exhaustive()
        }
    }

    impl<I> RecordBatchReaderAdapter<I> {
        /// Creates a new adapter from an iterator and a schema.
        pub fn new(inner: I, schema: arrow_schema::SchemaRef) -> Self {
            Self { inner, schema }
        }
    }

    impl<I, E> Iterator for RecordBatchReaderAdapter<I>
    where
        I: Iterator<Item = Result<arrow_array::RecordBatch, E>>,
        E: std::error::Error + Send + Sync + 'static,
    {
        type Item = Result<arrow_array::RecordBatch, arrow_schema::ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            self.inner
                .next()
                .map(|r| r.map_err(|e| arrow_schema::ArrowError::ExternalError(Box::new(e))))
        }
    }

    impl<I, E> arrow_array::RecordBatchReader for RecordBatchReaderAdapter<I>
    where
        I: Iterator<Item = Result<arrow_array::RecordBatch, E>>,
        E: std::error::Error + Send + Sync + 'static,
    {
        fn schema(&self) -> arrow_schema::SchemaRef {
            self.schema.clone()
        }
    }

    // -----------------------------------------------------------------------
    // Blanket impls: ArrowItemsClient → ItemsClient + StreamItemsClient
    //
    // Any type that is ArrowItemsClient + Send + Sync automatically satisfies
    // both ItemsClient and StreamItemsClient.  The Arrow query runs
    // synchronously (search_to_arrow borrows &self), so we collect all batches
    // while holding the borrow, then return owned data.  This removes the need
    // for explicit wrapper structs.

    impl<T> ItemsClient for T
    where
        T: ArrowItemsClient + Send + Sync,
        T::Error: std::error::Error + Send,
    {
        type Error = AdapterError<T::Error>;

        fn search(
            &self,
            search: Search,
        ) -> impl Future<Output = Result<ItemCollection, Self::Error>> + Send {
            // Collect synchronously while we hold `&self` (required because the
            // RecordBatchReader's lifetime is tied to the client borrow).
            let result: Result<Vec<Item>, AdapterError<T::Error>> = self
                .search_to_arrow(search)
                .map_err(AdapterError::Client)
                .and_then(|reader| {
                    crate::geoarrow::json::from_record_batch_reader(reader).map_err(Into::into)
                });
            async move { Ok(ItemCollection::from(result?)) }
        }
    }

    impl<T> StreamItemsClient for T
    where
        T: ArrowItemsClient + Send + Sync,
        T::Error: std::error::Error + Send,
    {
        type Error = AdapterError<T::Error>;

        fn search_stream(
            &self,
            search: Search,
        ) -> impl Future<
            Output = Result<impl Stream<Item = Result<Item, Self::Error>> + Send, Self::Error>,
        > + Send {
            // Collect synchronously while we hold `&self`.
            let result: Result<Vec<Item>, AdapterError<T::Error>> = self
                .search_to_arrow(search)
                .map_err(AdapterError::Client)
                .and_then(|reader| {
                    crate::geoarrow::json::from_record_batch_reader(reader).map_err(Into::into)
                });
            async move {
                let items = result?;
                Ok(futures::stream::iter(
                    items.into_iter().map(Ok::<_, AdapterError<T::Error>>),
                ))
            }
        }
    }
}

#[cfg(feature = "geoarrow")]
pub use geoarrow_adapters::RecordBatchReaderAdapter;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Collection;
    use crate::api::{
        CollectionsClient, Item, ItemCollection, ItemsClient, PagedCollectionsClient, Search,
        StreamCollectionsClient, StreamItemsClient,
    };
    use futures::StreamExt as _;
    use serde_json::{Map, Value};
    use std::convert::Infallible;

    // -----------------------------------------------------------------------
    // Mock clients
    // -----------------------------------------------------------------------

    /// A client that returns a fixed set of pages.  Each page is a `Vec<Item>`.
    /// Successive pages are linked via a `"_page"` token in `additional_fields`.
    #[derive(Clone, Debug)]
    struct MockPagedClient {
        pages: Vec<Vec<Item>>,
    }

    impl MockPagedClient {
        fn new(pages: Vec<Vec<Item>>) -> Self {
            Self { pages }
        }

        fn item(id: &str) -> Item {
            let mut m = Map::new();
            let _ = m.insert("id".into(), Value::String(id.to_string()));
            m
        }
    }

    impl ItemsClient for MockPagedClient {
        type Error = Infallible;

        async fn search(&self, search: Search) -> Result<ItemCollection, Infallible> {
            let page_idx: usize = search
                .additional_fields
                .get("_page")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize;

            let items = self.pages.get(page_idx).cloned().unwrap_or_default();

            let next = if page_idx + 1 < self.pages.len() {
                let mut next = Map::new();
                let _ = next.insert("_page".into(), Value::Number((page_idx + 1).into()));
                Some(next)
            } else {
                None
            };

            let mut coll = ItemCollection::default();
            coll.items = items;
            coll.next = next;
            Ok(coll)
        }
    }

    #[derive(Clone, Debug)]
    struct MockCollectionClient {
        collections: Vec<Collection>,
    }

    impl CollectionsClient for MockCollectionClient {
        type Error = Infallible;

        async fn collections(&self) -> Result<Vec<Collection>, Infallible> {
            Ok(self.collections.clone())
        }
    }

    /// A mock paginated collections client.
    ///
    /// Pages are stored as a `Vec<Vec<Collection>>`. The cursor is the page
    /// index encoded as a decimal string.
    #[derive(Clone, Debug)]
    struct MockPagedCollectionClient {
        pages: Vec<Vec<Collection>>,
    }

    impl MockPagedCollectionClient {
        fn new(pages: Vec<Vec<Collection>>) -> Self {
            Self { pages }
        }
    }

    impl PagedCollectionsClient for MockPagedCollectionClient {
        type Error = Infallible;

        async fn collections_page(
            &self,
            token: Option<String>,
        ) -> Result<(Vec<Collection>, Option<String>), Infallible> {
            let idx: usize = token.as_deref().and_then(|t| t.parse().ok()).unwrap_or(0);
            let page = self.pages.get(idx).cloned().unwrap_or_default();
            let next = if idx + 1 < self.pages.len() {
                Some((idx + 1).to_string())
            } else {
                None
            };
            Ok((page, next))
        }
    }

    // -----------------------------------------------------------------------
    // stream_pages_generic tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn stream_single_page() {
        let client = MockPagedClient::new(vec![vec![
            MockPagedClient::item("a"),
            MockPagedClient::item("b"),
        ]]);
        let wrapped = PagedItemsStream::new(client);
        let items = wrapped.collect_items(Search::default()).await.unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["id"], "a");
        assert_eq!(items[1]["id"], "b");
    }

    #[tokio::test]
    async fn stream_multiple_pages() {
        let client = MockPagedClient::new(vec![
            vec![MockPagedClient::item("a"), MockPagedClient::item("b")],
            vec![MockPagedClient::item("c")],
            vec![MockPagedClient::item("d"), MockPagedClient::item("e")],
        ]);
        let wrapped = PagedItemsStream::new(client);
        let items = wrapped.collect_items(Search::default()).await.unwrap();
        assert_eq!(items.len(), 5);
        let ids: Vec<&str> = items.iter().map(|m| m["id"].as_str().unwrap()).collect();
        assert_eq!(ids, ["a", "b", "c", "d", "e"]);
    }

    #[tokio::test]
    async fn stream_empty_page_terminates() {
        let client = MockPagedClient::new(vec![vec![]]);
        let wrapped = PagedItemsStream::new(client);
        let items = wrapped.collect_items(Search::default()).await.unwrap();
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn stream_no_pages_terminates() {
        let client = MockPagedClient::new(vec![]);
        let wrapped = PagedItemsStream::new(client);
        let items = wrapped.collect_items(Search::default()).await.unwrap();
        assert!(items.is_empty());
    }

    // -----------------------------------------------------------------------
    // item_count default method
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn item_count_across_pages() {
        let client = MockPagedClient::new(vec![
            vec![MockPagedClient::item("a"), MockPagedClient::item("b")],
            vec![MockPagedClient::item("c")],
        ]);
        let wrapped = PagedItemsStream::new(client);
        let count = wrapped.item_count(Search::default()).await.unwrap();
        assert_eq!(count, 3);
    }

    // -----------------------------------------------------------------------
    // PagedItemsStream::into_inner round-trip
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn paged_items_stream_into_inner() {
        let client = MockPagedClient::new(vec![]);
        let wrapped = PagedItemsStream::new(client.clone());
        // into_inner returns the original client; calling search on it directly
        // should still work identically.
        let inner = wrapped.into_inner();
        let page = inner.search(Search::default()).await.unwrap();
        assert!(page.items.is_empty());
    }

    // -----------------------------------------------------------------------
    // stream_pages_generic: explicit stream ordering
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn stream_pages_generic_ordering() {
        let page0: Vec<Item> = (0..3)
            .map(|i| MockPagedClient::item(&format!("p0-{i}")))
            .collect();
        let page1: Vec<Item> = (0..2)
            .map(|i| MockPagedClient::item(&format!("p1-{i}")))
            .collect();
        let client = MockPagedClient::new(vec![page0, page1]);
        let initial = client.search(Search::default()).await.unwrap();
        let stream = stream_pages_generic(client, Search::default(), initial);
        futures::pin_mut!(stream);
        let items: Vec<_> = stream.map(|r| r.unwrap()).collect().await;
        assert_eq!(items.len(), 5);
        // Verify iteration order is preserved across page boundaries.
        assert_eq!(items[2]["id"], "p0-2");
        assert_eq!(items[3]["id"], "p1-0");
    }

    // -----------------------------------------------------------------------
    // CollectionsClient blanket → StreamCollectionsClient

    #[tokio::test]
    async fn blanket_collections_stream_yields_all() {
        let client = MockCollectionClient {
            collections: vec![
                Collection::new("col-1", "First"),
                Collection::new("col-2", "Second"),
            ],
        };
        // MockCollectionClient is Clone + Send + Sync, so the blanket fires.
        let cols = client.collect_collections().await.unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].id, "col-1");
        assert_eq!(cols[1].id, "col-2");
    }

    #[tokio::test]
    async fn blanket_collections_stream_empty() {
        let client = MockCollectionClient {
            collections: vec![],
        };
        let cols = client.collect_collections().await.unwrap();
        assert!(cols.is_empty());
    }

    // -----------------------------------------------------------------------
    // stream_pages_collections_generic: paginated collections

    #[tokio::test]
    async fn paged_collections_stream_yields_all_pages() {
        let client = MockPagedCollectionClient::new(vec![
            vec![Collection::new("col-a", "A"), Collection::new("col-b", "B")],
            vec![Collection::new("col-c", "C")],
        ]);
        let (first_page, first_token) = client.collections_page(None).await.unwrap();
        let stream = stream_pages_collections_generic(client, first_page, first_token);
        futures::pin_mut!(stream);
        let cols: Vec<_> = stream.map(|r| r.unwrap()).collect().await;
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].id, "col-a");
        assert_eq!(cols[1].id, "col-b");
        assert_eq!(cols[2].id, "col-c");
    }

    #[tokio::test]
    async fn paged_collections_stream_single_page() {
        let client = MockPagedCollectionClient::new(vec![vec![Collection::new("only", "Only")]]);
        let (first_page, first_token) = client.collections_page(None).await.unwrap();
        let stream = stream_pages_collections_generic(client, first_page, first_token);
        futures::pin_mut!(stream);
        let cols: Vec<_> = stream.map(|r| r.unwrap()).collect().await;
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].id, "only");
    }

    #[tokio::test]
    async fn paged_collections_stream_empty_terminates() {
        let client = MockPagedCollectionClient::new(vec![]);
        let (first_page, first_token) = client.collections_page(None).await.unwrap();
        let stream = stream_pages_collections_generic(client, first_page, first_token);
        futures::pin_mut!(stream);
        let cols: Vec<_> = stream.map(|r| r.unwrap()).collect().await;
        assert!(cols.is_empty());
    }
}
