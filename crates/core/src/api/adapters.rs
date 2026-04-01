//! Generic adapters between search client traits.
//!
//! These adapters allow any implementation of one search trait to be used as
//! another. For example, any [`ItemsClient`] can be wrapped in
//! [`PagedItemsStream`] to gain a [`StreamItemsClient`] implementation that
//! paginates via the [`ItemCollection::next`] field.

#[cfg(feature = "async")]
mod async_adapters {
    use crate::Collection;
    use crate::api::client::{
        CollectionsClient, ItemsClient, PagedCollectionsClient, StreamCollectionsClient,
        StreamItemsClient,
    };
    use crate::api::{Item, ItemCollection, Search};
    use async_stream::try_stream;
    use futures_core::Stream;
    use std::future::Future;

    /// Adapter that wraps an [`ItemsClient`] to provide [`StreamItemsClient`].
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
        ) -> impl Future<
            Output = Result<impl Stream<Item = Result<Item, T::Error>> + Send, T::Error>,
        > + Send {
            let client = self.inner.clone();
            async move {
                let page = client.search(search.clone()).await?;
                Ok(stream_pages(client, search, page))
            }
        }
    }

    /// Streams items from an [`ItemsClient`] using token/skip-based pagination.
    pub fn stream_pages<T>(
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
                        current_search.additional_fields.extend(next_fields);
                        page = client.search(current_search.clone()).await?;
                    }
                    None => break,
                }
            }
        }
    }

    /// Blanket impl for non-paginated collection clients.
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
    pub fn stream_pages_collections<T>(
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
}

#[cfg(feature = "async")]
pub use async_adapters::{PagedItemsStream, stream_pages, stream_pages_collections};

#[cfg(feature = "geoarrow")]
mod geoarrow_adapters {
    use super::super::client::{ArrowItemsClient, ItemsClient};
    use super::super::{Item, ItemCollection, Search};
    use std::future::Future;

    /// Generic adapter that wraps any `Iterator<Item = Result<RecordBatch, E>>`
    /// and a schema into an [`arrow_array::RecordBatchReader`].
    #[allow(missing_debug_implementations)]
    pub struct RecordBatchReaderAdapter<I> {
        inner: I,
        schema: arrow_schema::SchemaRef,
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

    impl<T> ItemsClient for T
    where
        T: ArrowItemsClient + Send + Sync,
        T::Error: std::error::Error + Send + Sync + 'static,
    {
        type Error = crate::Error;

        fn search(
            &self,
            search: Search,
        ) -> impl Future<Output = Result<ItemCollection, Self::Error>> + Send {
            let result: Result<Vec<Item>, crate::Error> = self
                .search_to_arrow(search)
                .map_err(|err| crate::Error::ArrowAdapterClient(Box::new(err)))
                .and_then(crate::geoarrow::json::from_record_batch_reader);
            async move { Ok(ItemCollection::from(result?)) }
        }
    }

    #[cfg(feature = "async")]
    mod async_geoarrow_adapters {
        use super::ArrowItemsClient;
        use crate::api::client::StreamItemsClient;
        use crate::api::{Item, Search};
        use futures_core::Stream;
        use std::future::Future;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        struct ArrowItemStream<R> {
            reader: R,
            current_items: std::vec::IntoIter<Item>,
        }

        impl<R> ArrowItemStream<R> {
            fn new(reader: R) -> Self {
                Self {
                    reader,
                    current_items: Vec::new().into_iter(),
                }
            }
        }

        impl<R> Unpin for ArrowItemStream<R> {}

        impl<R> Stream for ArrowItemStream<R>
        where
            R: arrow_array::RecordBatchReader,
        {
            type Item = Result<Item, crate::Error>;

            fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
                let this = self.get_mut();
                loop {
                    if let Some(item) = this.current_items.next() {
                        return Poll::Ready(Some(Ok(item)));
                    }

                    match this.reader.next() {
                        Some(Ok(batch)) => {
                            match crate::geoarrow::json::record_batch_to_json_rows(batch) {
                                Ok(items) => {
                                    this.current_items = items.into_iter();
                                }
                                Err(err) => return Poll::Ready(Some(Err(err))),
                            }
                        }
                        Some(Err(err)) => return Poll::Ready(Some(Err(err.into()))),
                        None => return Poll::Ready(None),
                    }
                }
            }
        }

        impl<T> StreamItemsClient for T
        where
            T: ArrowItemsClient + Send + Sync,
            T::Error: std::error::Error + Send + Sync + 'static,
            for<'a> T::RecordBatchStream<'a>: Send,
        {
            type Error = crate::Error;

            fn search_stream(
                &self,
                search: Search,
            ) -> impl Future<
                Output = Result<impl Stream<Item = Result<Item, Self::Error>> + Send, Self::Error>,
            > + Send {
                let reader = self
                    .search_to_arrow(search)
                    .map_err(|err| crate::Error::ArrowAdapterClient(Box::new(err)));
                async move { Ok(ArrowItemStream::new(reader?)) }
            }
        }
    }
}

#[cfg(feature = "geoarrow")]
pub use geoarrow_adapters::RecordBatchReaderAdapter;
