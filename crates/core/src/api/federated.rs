use super::{Item, ItemCollection, Search, SearchClient, StreamSearchClient};
use crate::Error;
use futures::StreamExt;
use std::future::Future;
use std::pin::Pin;

/// A federated search client that fans out queries to multiple backends
/// and merges results into a single sorted stream.
///
/// Each inner client is queried in parallel via
/// [`StreamSearchClient::search_stream`], and the per-client streams are
/// merged using [`sort_sortable_streams`](crate::sort::sort_sortable_streams)
/// so that the combined output respects the requested sort order.
///
/// `FederatedSearchClient` itself implements both [`SearchClient`] and
/// [`StreamSearchClient`], so it can be composed with other clients or nested.
///
/// # Examples
///
/// ```no_run
/// use stac::api::{FederatedSearchClient, Search, SearchClient};
///
/// # async fn example<C: SearchClient<Error = stac::Error> + stac::api::StreamSearchClient<Error = stac::Error>>(clients: Vec<C>) {
/// let federated = FederatedSearchClient::new(clients);
/// let results = federated.search(Search::default()).await.unwrap();
/// # }
/// ```
#[derive(Debug)]
pub struct FederatedSearchClient<C> {
    clients: Vec<C>,
}

impl<C> FederatedSearchClient<C> {
    /// Creates a new federated client wrapping the given backends.
    pub fn new(clients: Vec<C>) -> Self {
        Self { clients }
    }

    /// Returns a reference to the inner clients.
    pub fn clients(&self) -> &[C] {
        &self.clients
    }
}

impl<C> SearchClient for FederatedSearchClient<C>
where
    C: SearchClient + StreamSearchClient + Send + Sync,
    <C as SearchClient>::Error: Into<Error> + Send + 'static,
    <C as StreamSearchClient>::Error: Into<Error> + Send + 'static,
{
    type Error = Error;

    fn search(
        &self,
        search: Search,
    ) -> impl Future<Output = Result<ItemCollection, Self::Error>> + Send {
        async move {
            let mut stream = StreamSearchClient::search_stream(self, search).await?;
            let mut items = Vec::new();
            while let Some(result) = stream.next().await {
                items.push(result?);
            }
            Ok(ItemCollection::from(items))
        }
    }
}

impl<C> StreamSearchClient for FederatedSearchClient<C>
where
    C: SearchClient + StreamSearchClient + Send + Sync,
    <C as SearchClient>::Error: Into<Error> + Send + 'static,
    <C as StreamSearchClient>::Error: Into<Error> + Send + 'static,
{
    type Error = Error;

    fn search_stream(
        &self,
        search: Search,
    ) -> impl Future<
        Output = Result<
            Pin<Box<dyn futures::Stream<Item = Result<Item, Self::Error>> + Send + '_>>,
            Self::Error,
        >,
    > + Send {
        async move {
            // Build the sort config from the search's sortby field.
            let sort_config = if search.items.sortby.is_empty() {
                // Use the default sort (datetime desc, id asc).
                serde_json::json!({
                    "sortby": [
                        { "field": "datetime", "direction": "desc" },
                        { "field": "id", "direction": "asc" }
                    ]
                })
            } else {
                serde_json::json!({"sortby": search.items.sortby})
            };

            // Fan out: get a paging stream from each client.
            let mut streams: Vec<Pin<Box<dyn futures::Stream<Item = Item> + Send + '_>>> =
                Vec::with_capacity(self.clients.len());

            for client in &self.clients {
                let client_stream = client
                    .search_stream(search.clone())
                    .await
                    .map_err(Into::into)?;

                // Map Result<Item, C::Error> → Item, filtering out errors.
                let mapped = client_stream.filter_map(|result| async move {
                    match result {
                        Ok(item) => Some(item),
                        Err(e) => {
                            let err: Error = e.into();
                            tracing::warn!("federated client error, skipping: {err}");
                            None
                        }
                    }
                });
                streams.push(Box::pin(mapped));
            }

            // Merge all streams using sorted k-way merge.
            let merged =
                crate::sort::sort_sortable_streams(streams, sort_config).map_err(Error::from)?;

            // Wrap each yielded item back into Ok(...) for the trait signature.
            let result_stream = merged.map(Ok);

            let result: Pin<Box<dyn futures::Stream<Item = Result<Item, Error>> + Send + '_>> =
                Box::pin(result_stream);
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::stream_search_pages;
    use serde_json::{Map, Value, json};

    /// A mock search client that returns pre-configured API items with
    /// token-based pagination (using a "skip" key).
    struct MockClient {
        /// Items stored in this mock, already sorted in the order the
        /// mock should return them.
        items: Vec<Item>,
        page_size: usize,
    }

    impl MockClient {
        fn new(items: Vec<Item>, page_size: usize) -> Self {
            Self { items, page_size }
        }
    }

    impl SearchClient for MockClient {
        type Error = Error;

        async fn search(&self, search: Search) -> Result<ItemCollection, Self::Error> {
            let skip = search
                .additional_fields
                .get("skip")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as usize;

            let page_items: Vec<Item> = self
                .items
                .iter()
                .skip(skip)
                .take(self.page_size)
                .cloned()
                .collect();

            let mut ic = ItemCollection::from(page_items);
            let next_skip = skip + self.page_size;
            if next_skip < self.items.len() {
                let mut next = Map::new();
                let _ = next.insert("skip".to_string(), json!(next_skip));
                ic.next = Some(next);
            }
            Ok(ic)
        }
    }

    impl StreamSearchClient for MockClient {
        type Error = Error;

        async fn search_stream(
            &self,
            search: Search,
        ) -> Result<
            Pin<Box<dyn futures::Stream<Item = Result<Item, Self::Error>> + Send + '_>>,
            Self::Error,
        > {
            Ok(stream_search_pages(self, search))
        }
    }

    /// Helper to build an API item (JSON map) with id and datetime.
    fn make_item(id: &str, datetime: &str) -> Item {
        let val = json!({
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": id,
            "geometry": null,
            "bbox": null,
            "properties": {
                "datetime": datetime
            },
            "links": [],
            "assets": {}
        });
        serde_json::from_value::<Map<String, Value>>(val).unwrap()
    }

    #[tokio::test]
    async fn federated_empty_clients() {
        let federated: FederatedSearchClient<MockClient> = FederatedSearchClient::new(vec![]);
        let ic = federated.search(Search::default()).await.unwrap();
        assert!(ic.items.is_empty());
    }

    #[tokio::test]
    async fn federated_single_client() {
        let items = vec![
            make_item("c", "2024-03-01T00:00:00Z"),
            make_item("b", "2024-02-01T00:00:00Z"),
            make_item("a", "2024-01-01T00:00:00Z"),
        ];
        let client = MockClient::new(items, 2);
        let federated = FederatedSearchClient::new(vec![client]);

        let mut stream = StreamSearchClient::search_stream(&federated, Search::default())
            .await
            .unwrap();

        // Default sort: datetime desc, id asc — items should come out
        // in the order the mock provides them (already datetime desc).
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first["id"], "c");
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second["id"], "b");
        let third = stream.next().await.unwrap().unwrap();
        assert_eq!(third["id"], "a");
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn federated_merge_sorted_by_datetime_desc() {
        // Client 1: items with datetimes 2024-03, 2024-01
        let client1 = MockClient::new(
            vec![
                make_item("x", "2024-03-01T00:00:00Z"),
                make_item("y", "2024-01-01T00:00:00Z"),
            ],
            10,
        );
        // Client 2: items with datetimes 2024-04, 2024-02
        let client2 = MockClient::new(
            vec![
                make_item("a", "2024-04-01T00:00:00Z"),
                make_item("b", "2024-02-01T00:00:00Z"),
            ],
            10,
        );

        let federated = FederatedSearchClient::new(vec![client1, client2]);
        let mut stream = StreamSearchClient::search_stream(&federated, Search::default())
            .await
            .unwrap();

        // Default sort: datetime desc, id asc.
        // Expected order: 2024-04 (a), 2024-03 (x), 2024-02 (b), 2024-01 (y)
        let ids: Vec<String> = {
            let mut v = Vec::new();
            while let Some(Ok(item)) = stream.next().await {
                v.push(item["id"].as_str().unwrap().to_string());
            }
            v
        };
        assert_eq!(ids, vec!["a", "x", "b", "y"]);
    }

    #[tokio::test]
    async fn federated_merge_sorted_by_id_asc() {
        use crate::api::Sortby;

        let client1 = MockClient::new(
            vec![
                make_item("alpha", "2024-01-01T00:00:00Z"),
                make_item("charlie", "2024-03-01T00:00:00Z"),
            ],
            10,
        );
        let client2 = MockClient::new(
            vec![
                make_item("bravo", "2024-02-01T00:00:00Z"),
                make_item("delta", "2024-04-01T00:00:00Z"),
            ],
            10,
        );

        let federated = FederatedSearchClient::new(vec![client1, client2]);
        let search = Search::default().sortby(vec![Sortby::asc("id".to_string())]);
        let mut stream = StreamSearchClient::search_stream(&federated, search)
            .await
            .unwrap();

        let ids: Vec<String> = {
            let mut v = Vec::new();
            while let Some(Ok(item)) = stream.next().await {
                v.push(item["id"].as_str().unwrap().to_string());
            }
            v
        };
        assert_eq!(ids, vec!["alpha", "bravo", "charlie", "delta"]);
    }

    #[tokio::test]
    async fn federated_search_collects_all_items() {
        let client1 = MockClient::new(
            vec![
                make_item("c", "2024-03-01T00:00:00Z"),
                make_item("a", "2024-01-01T00:00:00Z"),
            ],
            1, // page size 1 — forces multiple pages
        );
        let client2 = MockClient::new(vec![make_item("b", "2024-02-01T00:00:00Z")], 10);

        let federated = FederatedSearchClient::new(vec![client1, client2]);
        let ic = federated.search(Search::default()).await.unwrap();

        // Should have all 3 items
        assert_eq!(ic.items.len(), 3);

        // Verify sorted order (datetime desc): c, b, a
        let ids: Vec<&str> = ic.items.iter().map(|i| i["id"].as_str().unwrap()).collect();
        assert_eq!(ids, vec!["c", "b", "a"]);
    }

    #[tokio::test]
    async fn federated_three_clients_interleaved() {
        let client1 = MockClient::new(vec![make_item("1", "2024-06-01T00:00:00Z")], 10);
        let client2 = MockClient::new(vec![make_item("2", "2024-05-01T00:00:00Z")], 10);
        let client3 = MockClient::new(vec![make_item("3", "2024-04-01T00:00:00Z")], 10);

        let federated = FederatedSearchClient::new(vec![client1, client2, client3]);
        let mut stream = StreamSearchClient::search_stream(&federated, Search::default())
            .await
            .unwrap();

        let ids: Vec<String> = {
            let mut v = Vec::new();
            while let Some(Ok(item)) = stream.next().await {
                v.push(item["id"].as_str().unwrap().to_string());
            }
            v
        };
        assert_eq!(ids, vec!["1", "2", "3"]);
    }

    #[tokio::test]
    async fn federated_same_datetime_sorts_by_id() {
        // When datetimes are equal, default sort uses id asc as tiebreaker.
        let client1 = MockClient::new(vec![make_item("zebra", "2024-01-01T00:00:00Z")], 10);
        let client2 = MockClient::new(vec![make_item("apple", "2024-01-01T00:00:00Z")], 10);

        let federated = FederatedSearchClient::new(vec![client1, client2]);
        let mut stream = StreamSearchClient::search_stream(&federated, Search::default())
            .await
            .unwrap();

        let ids: Vec<String> = {
            let mut v = Vec::new();
            while let Some(Ok(item)) = stream.next().await {
                v.push(item["id"].as_str().unwrap().to_string());
            }
            v
        };
        // Same datetime → id asc → apple before zebra
        assert_eq!(ids, vec!["apple", "zebra"]);
    }
}
