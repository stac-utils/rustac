//! A STAC API client.

use crate::{Error, Result};
use async_stream::try_stream;
use futures::{Stream, StreamExt, pin_mut};
use http::header::{HeaderName, USER_AGENT};
use reqwest::{ClientBuilder, IntoUrl, Method, StatusCode, header::HeaderMap, header::HeaderValue};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use stac::api::{GetItems, Item, ItemCollection, Items, Search, UrlBuilder};
use stac::{Collection, Link, Links, SelfHref};
use std::pin::Pin;
use tokio::{
    runtime::{Builder, Runtime},
    sync::mpsc::{self, error::SendError},
    task::JoinHandle,
};

const DEFAULT_CHANNEL_BUFFER: usize = 4;

/// Searches a STAC API.
pub async fn search(
    href: &str,
    mut search: Search,
    max_items: Option<usize>,
    headers: &Option<String>,
) -> Result<ItemCollection> {
    let mut builder = ApiClientBuilder::new(href)?;
    if let Some(h) = headers {
        builder = builder.with_headers(h)?;
    }
    let client = builder.build()?;
    if search.limit.is_none() {
        if let Some(max_items) = max_items {
            search.limit = Some(max_items.try_into()?);
        }
    }
    let stream = client.search(search).await?;
    let mut items = if let Some(max_items) = max_items {
        if max_items == 0 {
            return Ok(ItemCollection::default());
        }
        Vec::with_capacity(max_items)
    } else {
        Vec::new()
    };
    pin_mut!(stream);
    while let Some(item) = stream.next().await {
        let item = item?;
        items.push(item);
        if let Some(max_items) = max_items {
            if items.len() >= max_items {
                break;
            }
        }
    }
    let item_collection = ItemCollection::new(items)?;
    Ok(item_collection)
}

/// A client for interacting with STAC APIs.
#[derive(Clone, Debug)]
pub struct Client {
    client: reqwest::Client,
    channel_buffer: usize,
    url_builder: UrlBuilder,
}

/// A client for interacting with STAC APIs without async.
#[derive(Debug)]
pub struct BlockingClient(Client);

/// A blocking iterator over items.
#[allow(missing_debug_implementations)]
pub struct BlockingIterator {
    runtime: Runtime,
    stream: Pin<Box<dyn Stream<Item = Result<Item>>>>,
}

pub struct ApiClientBuilder {
    url: String,
    headers: HeaderMap,
}

impl ApiClientBuilder {
    pub fn new(url: &str) -> Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            format!("rustac/{}", env!("CARGO_PKG_VERSION")).parse()?,
        );
        Ok(Self {
            url: url.to_string(),
            headers,
        })
    }

    pub fn with_headers(mut self, headers_str: &str) -> Result<Self> {
        for header_pair in headers_str.split(',').filter(|s| !s.is_empty()) {
            if let Some((key, val)) = header_pair.split_once(':') {
                self.headers.insert(
                    key.trim().parse::<HeaderName>()?,
                    HeaderValue::from_str(val.trim())?,
                );
            }
        }
        Ok(self)
    }

    pub fn build(self) -> Result<Client> {
        let client = ClientBuilder::new().default_headers(self.headers).build()?;
        Client::with_client(client, &self.url)
    }
}

impl Client {
    /// Creates a new API client.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac_io::api::Client;
    /// let client = Client::new("https://planetarycomputer.microsoft.com/api/stac/v1").unwrap();
    /// ```
    pub fn new(url: &str) -> Result<Client> {
        ApiClientBuilder::new(url)?.build()
    }

    /// Creates a new API client with the given [Client].
    ///
    /// Useful if you want to customize the behavior of the underlying `Client`,
    /// as documented in [Client::new].
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_io::api::Client;
    ///
    /// let client = reqwest::Client::new();
    /// let client = Client::with_client(client, "https://earth-search.aws.element84.com/v1/").unwrap();
    /// ```
    pub fn with_client(client: reqwest::Client, url: &str) -> Result<Client> {
        Ok(Client {
            client,
            channel_buffer: DEFAULT_CHANNEL_BUFFER,
            url_builder: UrlBuilder::new(url)?,
        })
    }

    /// Returns a single collection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use stac_io::api::Client;
    /// let client = Client::new("https://planetarycomputer.microsoft.com/api/stac/v1").unwrap();
    /// # tokio_test::block_on(async {
    /// let collection = client.collection("sentinel-2-l2a").await.unwrap().unwrap();
    /// # })
    /// ```
    pub async fn collection(&self, id: &str) -> Result<Option<Collection>> {
        let url = self.url_builder.collection(id)?;
        not_found_to_none(self.get(url).await)
    }

    /// Returns a stream of items belonging to a collection, using the [items
    /// endpoint](https://github.com/radiantearth/stac-api-spec/tree/main/ogcapi-features#collection-items-collectionscollectioniditems).
    ///
    /// The `items` argument can be used to filter, sort, and otherwise
    /// configure the request.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac_io::api::Client;
    /// use stac::api::Items;
    /// use futures::StreamExt;
    ///
    /// let client = Client::new("https://planetarycomputer.microsoft.com/api/stac/v1").unwrap();
    /// let items = Items {
    ///     limit: Some(1),
    ///     ..Default::default()
    /// };
    /// # tokio_test::block_on(async {
    /// let items: Vec<_> = client
    ///     .items("sentinel-2-l2a", items)
    ///     .await
    ///     .unwrap()
    ///     .map(|result| result.unwrap())
    ///     .collect()
    ///     .await;
    /// assert_eq!(items.len(), 1);
    /// # })
    /// ```
    pub async fn items(
        &self,
        id: &str,
        items: impl Into<Option<Items>>,
    ) -> Result<impl Stream<Item = Result<Item>>> {
        let url = self.url_builder.items(id)?; // TODO HATEOS
        let items = match items.into() {
            Some(items) => Some(GetItems::try_from(items)?),
            _ => None,
        };
        let page = self
            .request(Method::GET, url.clone(), items.as_ref(), None)
            .await?;
        Ok(stream_items(self.clone(), page, self.channel_buffer))
    }

    /// Searches an API, returning a stream of items.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac_io::api::Client;
    /// use stac::api::Search;
    /// use futures::StreamExt;
    ///
    /// let client = Client::new("https://planetarycomputer.microsoft.com/api/stac/v1").unwrap();
    /// let mut search = Search { collections: vec!["sentinel-2-l2a".to_string()], ..Default::default() };
    /// # tokio_test::block_on(async {
    /// let items: Vec<_> = client
    ///     .search(search)
    ///     .await
    ///     .unwrap()
    ///     .take(1)
    ///     .map(|result| result.unwrap())
    ///     .collect()
    ///     .await;
    /// assert_eq!(items.len(), 1);
    /// # })
    /// ```
    pub async fn search(&self, search: Search) -> Result<impl Stream<Item = Result<Item>> + use<>> {
        let url = self.url_builder.search().clone();
        tracing::debug!("searching {url}");
        // TODO support GET
        let page = self.post(url.clone(), &search).await?;
        Ok(stream_items(self.clone(), page, self.channel_buffer))
    }

    async fn get<V>(&self, url: impl IntoUrl) -> Result<V>
    where
        V: DeserializeOwned + SelfHref,
    {
        let url = url.into_url()?;
        let mut value = self
            .request::<(), V>(Method::GET, url.clone(), None, None)
            .await?;
        value.set_self_href(url);
        Ok(value)
    }

    async fn post<S, R>(&self, url: impl IntoUrl, data: &S) -> Result<R>
    where
        S: Serialize + 'static,
        R: DeserializeOwned,
    {
        self.request(Method::POST, url, Some(data), None).await
    }

    async fn request<S, R>(
        &self,
        method: Method,
        url: impl IntoUrl,
        params: impl Into<Option<&S>>,
        headers: impl Into<Option<HeaderMap>>,
    ) -> Result<R>
    where
        S: Serialize + 'static,
        R: DeserializeOwned,
    {
        let url = url.into_url()?;
        let mut request = match method {
            Method::GET => {
                let mut request = self.client.get(url);
                if let Some(query) = params.into() {
                    request = request.query(query);
                }
                request
            }
            Method::POST => {
                let mut request = self.client.post(url);
                if let Some(data) = params.into() {
                    request = request.json(&data);
                }
                request
            }
            _ => unimplemented!(),
        };
        if let Some(headers) = headers.into() {
            request = request.headers(headers);
        }
        let response = request.send().await?.error_for_status()?;
        response.json().await.map_err(Error::from)
    }

    async fn request_from_link<R>(&self, link: Link) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let method = if let Some(method) = link.method {
            method.parse()?
        } else {
            Method::GET
        };
        let headers = if let Some(headers) = link.headers {
            let mut header_map = HeaderMap::new();
            for (key, value) in headers.into_iter() {
                let header_name: HeaderName = key.parse()?;
                let _ = header_map.insert(header_name, value.to_string().parse()?);
            }
            Some(header_map)
        } else {
            None
        };
        self.request::<Map<String, Value>, R>(method, link.href.as_str(), &link.body, headers)
            .await
    }
}

impl BlockingClient {
    /// Creates a new blocking client.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_io::api::BlockingClient;
    ///
    /// let client = BlockingClient::new("https://planetarycomputer.microsoft.com/api/stac/vi").unwrap();
    /// ```
    pub fn new(url: &str) -> Result<BlockingClient> {
        Client::new(url).map(Self)
    }

    /// Searches an API, returning an iterable of items.
    ///
    /// To prevent fetching _all_ the items (which might be a lot), it is recommended to pass a `max_items`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac_io::api::BlockingClient;
    /// use stac::api::Search;
    ///
    /// let client = BlockingClient::new("https://planetarycomputer.microsoft.com/api/stac/v1").unwrap();
    /// let mut search = Search { collections: vec!["sentinel-2-l2a".to_string()], ..Default::default() };
    /// let items: Vec<_> = client
    ///     .search(search)
    ///     .unwrap()
    ///     .map(|result| result.unwrap())
    ///     .take(1)
    ///     .collect();
    /// assert_eq!(items.len(), 1);
    /// ```
    pub fn search(&self, search: Search) -> Result<BlockingIterator> {
        let runtime = Builder::new_current_thread().enable_all().build()?;
        let stream = runtime.block_on(async move { self.0.search(search).await })?;
        Ok(BlockingIterator {
            runtime,
            stream: Box::pin(stream),
        })
    }
}

impl Iterator for BlockingIterator {
    type Item = Result<Item>;

    fn next(&mut self) -> Option<Self::Item> {
        self.runtime.block_on(self.stream.next())
    }
}

fn stream_items(
    client: Client,
    page: ItemCollection,
    channel_buffer: usize,
) -> impl Stream<Item = Result<Item>> {
    let (tx, mut rx) = mpsc::channel(channel_buffer);
    let handle: JoinHandle<std::result::Result<(), SendError<_>>> = tokio::spawn(async move {
        let pages = stream_pages(client, page);
        pin_mut!(pages);
        while let Some(result) = pages.next().await {
            match result {
                Ok(page) => tx.send(Ok(page)).await?,
                Err(err) => {
                    tx.send(Err(err)).await?;
                    return Ok(());
                }
            }
        }
        Ok(())
    });
    try_stream! {
        while let Some(result) = rx.recv().await {
            let page = result?;
            for item in page.items {
                yield item;
            }
        }
        let _ = handle.await?;
    }
}

fn stream_pages(
    client: Client,
    mut page: ItemCollection,
) -> impl Stream<Item = Result<ItemCollection>> {
    try_stream! {
        loop {
            if page.items.is_empty() {
                break;
            }
            let next_link = page.link("next").cloned();
            yield page;
            if let Some(next_link) = next_link {
                if let Some(next_page) = client.request_from_link(next_link).await? {
                    page = next_page;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
    }
}

fn not_found_to_none<T>(result: Result<T>) -> Result<Option<T>> {
    let mut result = result.map(Some);
    if let Err(Error::Reqwest(ref err)) = result {
        if err
            .status()
            .map(|s| s == StatusCode::NOT_FOUND)
            .unwrap_or_default()
        {
            result = Ok(None);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use crate::api::ApiClientBuilder;

    use super::Client;
    use futures::StreamExt;
    use mockito::{Matcher, Server};
    use serde_json::json;
    use stac::Links;
    use stac::api::{ItemCollection, Items, Search};
    use url::Url;

    #[tokio::test]
    async fn collection_not_found() {
        let mut server = Server::new_async().await;
        let collection = server
            .mock("GET", "/collections/not-a-collection")
            .with_body(include_str!("../mocks/not-a-collection.json"))
            .with_header("content-type", "application/json")
            .with_status(404)
            .create_async()
            .await;

        let client = Client::new(&server.url()).unwrap();
        assert!(
            client
                .collection("not-a-collection")
                .await
                .unwrap()
                .is_none()
        );
        collection.assert_async().await;
    }

    #[tokio::test]
    async fn search_with_paging() {
        let mut server = Server::new_async().await;
        let mut page_1_body: ItemCollection =
            serde_json::from_str(include_str!("../mocks/search-page-1.json")).unwrap();
        let mut next_link = page_1_body.link("next").unwrap().clone();
        next_link.href = format!("{}/search", server.url());
        page_1_body.set_link(next_link);
        let page_1 = server
            .mock("POST", "/search")
            .match_body(Matcher::Json(json!({
                "collections": ["sentinel-2-l2a"],
                "limit": 1
            })))
            .with_body(serde_json::to_string(&page_1_body).unwrap())
            .with_header("content-type", "application/geo+json")
            .create_async()
            .await;
        let page_2 = server
            .mock("POST", "/search")
            .match_body(Matcher::Json(json!({
                "collections": ["sentinel-2-l2a"],
                "limit": 1,
                "token": "next:S2A_MSIL2A_20230216T150721_R082_T19PHS_20230217T082924"
            })))
            .with_body(include_str!("../mocks/search-page-2.json"))
            .with_header("content-type", "application/geo+json")
            .create_async()
            .await;

        let client = Client::new(&server.url()).unwrap();
        let mut search = Search {
            collections: vec!["sentinel-2-l2a".to_string()],
            ..Default::default()
        };
        search.items.limit = Some(1);
        let items: Vec<_> = client
            .search(search)
            .await
            .unwrap()
            .map(|result| result.unwrap())
            .take(2)
            .collect()
            .await;
        page_1.assert_async().await;
        page_2.assert_async().await;
        assert_eq!(items.len(), 2);
        assert!(items[0]["id"] != items[1]["id"]);
    }

    #[tokio::test]
    async fn items_with_paging() {
        let mut server = Server::new_async().await;
        let mut page_1_body: ItemCollection =
            serde_json::from_str(include_str!("../mocks/items-page-1.json")).unwrap();
        let mut next_link = page_1_body.link("next").unwrap().clone();
        let url: Url = next_link.href.as_str().parse().unwrap();
        let query = url.query().unwrap();
        next_link.href = format!(
            "{}/collections/sentinel-2-l2a/items?{}",
            server.url(),
            query
        );
        page_1_body.set_link(next_link);
        let page_1 = server
            .mock("GET", "/collections/sentinel-2-l2a/items?limit=1")
            .with_body(serde_json::to_string(&page_1_body).unwrap())
            .with_header("content-type", "application/geo+json")
            .create_async()
            .await;
        let page_2 = server
            .mock("GET", "/collections/sentinel-2-l2a/items?limit=1&token=next:S2A_MSIL2A_20230216T235751_R087_T52CEB_20230217T134604")
            .with_body(include_str!("../mocks/items-page-2.json"))
            .with_header("content-type", "application/geo+json")
            .create_async()
            .await;

        let client = Client::new(&server.url()).unwrap();
        let items = Items {
            limit: Some(1),
            ..Default::default()
        };
        let items: Vec<_> = client
            .items("sentinel-2-l2a", Some(items))
            .await
            .unwrap()
            .map(|result| result.unwrap())
            .take(2)
            .collect()
            .await;
        page_1.assert_async().await;
        page_2.assert_async().await;
        assert_eq!(items.len(), 2);
        assert!(items[0]["id"] != items[1]["id"]);
    }

    #[tokio::test]
    async fn stop_on_empty_page() {
        let mut server = Server::new_async().await;
        let mut page_body: ItemCollection =
            serde_json::from_str(include_str!("../mocks/items-page-1.json")).unwrap();
        let mut next_link = page_body.link("next").unwrap().clone();
        let url: Url = next_link.href.as_str().parse().unwrap();
        let query = url.query().unwrap();
        next_link.href = format!(
            "{}/collections/sentinel-2-l2a/items?{}",
            server.url(),
            query
        );
        page_body.set_link(next_link);
        page_body.items = vec![];
        let page = server
            .mock("GET", "/collections/sentinel-2-l2a/items?limit=1")
            .with_body(serde_json::to_string(&page_body).unwrap())
            .with_header("content-type", "application/geo+json")
            .create_async()
            .await;

        let client = Client::new(&server.url()).unwrap();
        let items = Items {
            limit: Some(1),
            ..Default::default()
        };
        let items: Vec<_> = client
            .items("sentinel-2-l2a", Some(items))
            .await
            .unwrap()
            .map(|result| result.unwrap())
            .collect()
            .await;
        page.assert_async().await;
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn user_agent() {
        let mut server = Server::new_async().await;
        let _ = server
            .mock("POST", "/search")
            .with_body_from_file("mocks/items-page-1.json")
            .match_header(
                "user-agent",
                format!("rustac/{}", env!("CARGO_PKG_VERSION")).as_str(),
            )
            .create_async()
            .await;
        let client = Client::new(&server.url()).unwrap();
        let _ = client.search(Default::default()).await.unwrap();
    }
    #[tokio::test]
    async fn custom_header() {
        let mut server = Server::new_async().await;
        let _ = server
            .mock("POST", "/search")
            .with_body_from_file("mocks/items-page-1.json")
            .match_header("x-my-header", "value")
            .match_header("x-my-other-header", "othervalue")
            .create_async()
            .await;
        let builder = ApiClientBuilder::new(&server.url())
            .unwrap()
            .with_headers("x-my-header: value,x-my-other-header: othervalue")
            .unwrap();
        let client = builder.build().unwrap();
        let _ = client.search(Default::default()).await.unwrap();
    }
}
