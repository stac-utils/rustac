use crate::{Backend, DEFAULT_DESCRIPTION, DEFAULT_ID, Error, Result};
use http::Method;
use serde::Serialize;
use serde_json::{Map, Value, json};
use stac::{Catalog, Collection, Fields, Item, Link, Links, mime::APPLICATION_OPENAPI_3_0};
use stac_api::{Collections, Conformance, ItemCollection, Items, Root, Search};
use url::Url;

/// A STAC server API.
#[derive(Clone, Debug)]
pub struct Api<B: Backend> {
    /// The backend storage for this API.
    pub backend: B,

    /// The text description of this API.
    pub description: String,

    /// The catalog id of this API.
    pub id: String,

    /// The root url of this API.
    pub root: Url,
}

impl<B: Backend> Api<B> {
    /// Creates a new API with the given backend.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{Api, MemoryBackend};
    ///
    /// let backend = MemoryBackend::new();
    /// let api = Api::new(backend, "http://stac.test").unwrap();
    /// ```
    pub fn new(backend: B, root: &str) -> Result<Api<B>> {
        Ok(Api {
            backend,
            id: DEFAULT_ID.to_string(),
            description: DEFAULT_DESCRIPTION.to_string(),
            root: root.parse()?,
        })
    }

    /// Sets this API's id.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{Api, MemoryBackend};
    ///
    /// let backend = MemoryBackend::new();
    /// let api = Api::new(backend, "http://stac.test").unwrap().id("an-id");
    /// ```
    pub fn id(mut self, id: impl ToString) -> Api<B> {
        self.id = id.to_string();
        self
    }

    /// Sets this API's description.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{Api, MemoryBackend};
    ///
    /// let backend = MemoryBackend::new();
    /// let api = Api::new(backend, "http://stac.test").unwrap().description("a description");
    /// ```
    pub fn description(mut self, description: impl ToString) -> Api<B> {
        self.description = description.to_string();
        self
    }

    fn url(&self, path: &str) -> Result<Url> {
        self.root.join(path).map_err(Error::from)
    }

    /// Returns the root of the API.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{Api, MemoryBackend};
    ///
    /// let api = Api::new(MemoryBackend::new(), "http://stac.test").unwrap();
    /// # tokio_test::block_on(async {
    /// let root = api.root().await.unwrap();
    /// # })
    /// ```
    pub async fn root(&self) -> Result<Root> {
        let mut catalog = Catalog::new(&self.id, &self.description);
        catalog.set_link(Link::root(self.root.clone()).json());
        catalog.set_link(Link::self_(self.root.clone()).json());
        catalog.set_link(
            Link::new(self.url("/api")?, "service-desc")
                .r#type(APPLICATION_OPENAPI_3_0.to_string()),
        );
        catalog.set_link(
            Link::new(self.url("/api.html")?, "service-doc").r#type("text/html".to_string()),
        );
        catalog.set_link(Link::new(self.url("/conformance")?, "conformance").json());
        catalog.set_link(Link::new(self.url("/collections")?, "data").json());
        for collection in self.backend.collections().await? {
            catalog
                .links
                .push(Link::child(self.url(&format!("/collections/{}", collection.id))?).json());
        }
        let search_url = self.url("/search")?;
        catalog.links.push(
            Link::new(search_url.clone(), "search")
                .geojson()
                .method("GET"),
        );
        catalog
            .links
            .push(Link::new(search_url, "search").geojson().method("POST"));
        if self.backend.has_filter() {
            catalog.links.push(
                Link::new(
                    self.url("/queryables")?,
                    "http://www.opengis.net/def/rel/ogc/1.0/queryables",
                )
                .r#type("application/schema+json".to_string()),
            );
        }
        Ok(Root {
            catalog,
            conformance: self.conformance(),
        })
    }

    /// Returns the conformance classes.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{Api, MemoryBackend};
    ///
    /// let api = Api::new(MemoryBackend::new(), "http://stac.test").unwrap();
    /// let conformance = api.conformance();
    /// ```
    pub fn conformance(&self) -> Conformance {
        let mut conformance = Conformance::new().ogcapi_features();
        if self.backend.has_item_search() {
            conformance = conformance.item_search();
        }
        if self.backend.has_filter() {
            conformance = conformance.filter();
        }
        conformance
    }

    /// Returns queryables.
    pub fn queryables(&self) -> Value {
        // This is a pure punt from https://github.com/stac-api-extensions/filter?tab=readme-ov-file#queryables
        json!({
          "$schema" : "https://json-schema.org/draft/2019-09/schema",
          "$id" : "https://stac-api.example.com/queryables",
          "type" : "object",
          "title" : "Queryables for Example STAC API",
          "description" : "Queryable names for the example STAC API Item Search filter.",
          "properties" : {
          },
          "additionalProperties": true
        })
    }

    /// Returns the collections from the backend.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{Api, MemoryBackend};
    ///
    /// let api = Api::new(MemoryBackend::new(), "http://stac.test").unwrap();
    /// # tokio_test::block_on(async {
    /// let collections = api.collections().await.unwrap();
    /// # })
    /// ```
    pub async fn collections(&self) -> Result<Collections> {
        let mut collections: Collections = self.backend.collections().await?.into();
        collections.set_link(Link::root(self.root.clone()).json());
        collections.set_link(Link::self_(self.url("/collections")?).json());
        for collection in collections.collections.iter_mut() {
            self.set_collection_links(collection)?;
        }
        Ok(collections)
    }

    /// Returns the collections from the backend.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{Api, MemoryBackend, Backend};
    /// use stac::Collection;
    ///
    /// let mut backend = MemoryBackend::new();
    /// # tokio_test::block_on(async {
    /// backend.add_collection(Collection::new("an-id", "a description")).await.unwrap();
    /// let api = Api::new(backend, "http://stac.test").unwrap();
    /// let collection = api.collection("an-id").await.unwrap().unwrap();
    /// # })
    /// ```
    pub async fn collection(&self, id: &str) -> Result<Option<Collection>> {
        match self.backend.collection(id).await? {
            Some(mut collection) => {
                self.set_collection_links(&mut collection)?;
                Ok(Some(collection))
            }
            _ => Ok(None),
        }
    }

    /// Returns all items for a given collection.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{Api, MemoryBackend, Backend};
    /// use stac::{Collection, Item};
    /// use stac_api::Items;
    ///
    /// let mut backend = MemoryBackend::new();
    /// # tokio_test::block_on(async {
    /// backend.add_collection(Collection::new("collection-id", "a description")).await.unwrap();
    /// backend.add_item(Item::new("item-id").collection("collection-id")).await.unwrap();
    /// let api = Api::new(backend, "http://stac.test").unwrap();
    /// let items = api.items("collection-id", Items::default()).await.unwrap().unwrap();
    /// assert_eq!(items.items.len(), 1);
    /// # })
    /// ```
    pub async fn items(&self, collection_id: &str, items: Items) -> Result<Option<ItemCollection>> {
        match self.backend.items(collection_id, items.clone()).await? {
            Some(mut item_collection) => {
                let collection_url = self.url(&format!("/collections/{collection_id}"))?;
                let items_url = self.url(&format!("/collections/{collection_id}/items"))?;
                item_collection.set_link(Link::root(self.root.clone()).json());
                item_collection.set_link(Link::self_(items_url.clone()).geojson());
                item_collection.set_link(Link::collection(collection_url).json());
                if let Some(next) = item_collection.next.take() {
                    item_collection.set_link(self.pagination_link(
                        items_url.clone(),
                        items.clone(),
                        next,
                        "next",
                        &Method::GET,
                    )?);
                }
                if let Some(prev) = item_collection.prev.take() {
                    item_collection.set_link(self.pagination_link(
                        items_url,
                        items,
                        prev,
                        "prev",
                        &Method::GET,
                    )?);
                }
                for item in item_collection.items.iter_mut() {
                    self.set_item_links(item)?;
                }
                Ok(Some(item_collection))
            }
            _ => Ok(None),
        }
    }

    /// Returns an item.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{Api, MemoryBackend, Backend};
    /// use stac::{Collection, Item};
    /// use stac_api::Items;
    ///
    /// let mut backend = MemoryBackend::new();
    /// # tokio_test::block_on(async {
    /// backend.add_collection(Collection::new("collection-id", "a description")).await.unwrap();
    /// backend.add_item(Item::new("item-id").collection("collection-id")).await.unwrap();
    /// let api = Api::new(backend, "http://stac.test").unwrap();
    /// let item = api.item("collection-id", "item-id").await.unwrap().unwrap();
    /// # })
    /// ```
    pub async fn item(&self, collection_id: &str, item_id: &str) -> Result<Option<Item>> {
        match self.backend.item(collection_id, item_id).await? {
            Some(mut item) => {
                item.set_link(Link::root(self.root.clone()).json());
                item.set_link(
                    Link::self_(
                        self.url(&format!("/collections/{collection_id}/items/{item_id}"))?,
                    )
                    .geojson(),
                );
                let collection_url = self.url(&format!("/collections/{collection_id}"))?;
                item.set_link(Link::collection(collection_url.clone()).json());
                item.set_link(Link::parent(collection_url).json());
                Ok(Some(item))
            }
            _ => Ok(None),
        }
    }

    /// Searches the API.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_api::Search;
    /// use stac_server::{Api, MemoryBackend, Backend};
    /// use http::Method;
    ///
    /// let api = Api::new(MemoryBackend::new(), "http://stac.test").unwrap();
    /// # tokio_test::block_on(async {
    /// let item_collection = api.search(Search::default(), Method::GET).await.unwrap();
    /// # })
    /// ```
    pub async fn search(&self, mut search: Search, method: Method) -> Result<ItemCollection> {
        let mut item_collection = self.backend.search(search.clone()).await?;
        if method == Method::GET {
            if let Some(filter) = search.filter.take() {
                search.filter = Some(filter.into_cql2_text()?);
            }
        }
        item_collection.set_link(Link::root(self.root.clone()).json());
        let search_url = self.url("/search")?;
        if let Some(next) = item_collection.next.take() {
            tracing::debug!("adding next pagination link");
            item_collection.set_link(self.pagination_link(
                search_url.clone(),
                search.clone(),
                next,
                "next",
                &method,
            )?);
        }
        if let Some(prev) = item_collection.prev.take() {
            tracing::debug!("adding prev pagination link");
            item_collection
                .set_link(self.pagination_link(search_url, search, prev, "prev", &method)?);
        }
        for item in item_collection.items.iter_mut() {
            self.set_item_links(item)?;
        }
        Ok(item_collection)
    }

    fn set_collection_links(&self, collection: &mut Collection) -> Result<()> {
        collection.set_link(Link::root(self.root.clone()).json());
        collection
            .set_link(Link::self_(self.url(&format!("/collections/{}", collection.id))?).json());
        collection.set_link(Link::parent(self.root.clone()).json());
        collection.set_link(
            Link::new(
                self.url(&format!("/collections/{}/items", collection.id))?,
                "items",
            )
            .geojson(),
        );
        Ok(())
    }

    fn pagination_link<D>(
        &self,
        mut url: Url,
        mut data: D,
        pagination: Map<String, Value>,
        rel: &str,
        method: &Method,
    ) -> Result<Link>
    where
        D: Fields + Serialize,
    {
        for (key, value) in pagination {
            let _ = data.set_field(key, value)?;
        }
        match *method {
            Method::GET => {
                url.set_query(Some(&serde_urlencoded::to_string(data)?));
                Ok(Link::new(url, rel).geojson().method("GET"))
            }
            Method::POST => Ok(Link::new(url, rel).geojson().method("POST").body(data)?),
            _ => unimplemented!(),
        }
    }

    fn set_item_links(&self, item: &mut stac_api::Item) -> Result<()> {
        let mut collection_url = None;
        let mut item_link = None;
        if let Some(item_id) = item.get("id").and_then(|id| id.as_str()) {
            if let Some(collection_id) = item.get("collection").and_then(|id| id.as_str()) {
                collection_url = Some(self.url(&format!("/collections/{collection_id}"))?);
                item_link = Some(serde_json::to_value(
                    Link::self_(
                        self.url(&format!("/collections/{collection_id}/items/{item_id}"))?,
                    )
                    .geojson(),
                )?);
            }
        }
        if item
            .get("links")
            .map(|links| !links.is_array())
            .unwrap_or(true)
        {
            let _ = item.insert("links".to_string(), Value::Array(Vec::new()));
        }
        let links = item.get_mut("links").unwrap().as_array_mut().unwrap();
        links.push(serde_json::to_value(Link::root(self.root.clone()).json())?);
        if let Some(item_link) = item_link {
            links.push(item_link);
        }
        if let Some(collection_url) = collection_url {
            links.push(serde_json::to_value(
                Link::collection(collection_url.clone()).json(),
            )?);
            links.push(serde_json::to_value(Link::parent(collection_url).json())?);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Api;
    use crate::{Backend, MemoryBackend};
    use http::Method;
    use stac::{Catalog, Collection, Item, Links};
    use stac_api::{ITEM_SEARCH_URI, Items, Search};
    use std::collections::HashSet;

    macro_rules! assert_link {
        ($link:expr_2021, $href:expr_2021, $media_type:expr_2021) => {
            let link = $link.unwrap();
            assert_eq!(link.href, $href);
            assert_eq!(link.r#type.as_ref().unwrap(), $media_type);
        };
    }

    fn test_api(backend: MemoryBackend) -> Api<MemoryBackend> {
        Api::new(backend, "http://stac.test/")
            .unwrap()
            .id("an-id")
            .description("a description")
    }

    #[tokio::test]
    async fn root() {
        let mut backend = MemoryBackend::new();
        backend
            .add_collection(Collection::new("a-collection", "A description"))
            .await
            .unwrap();
        let api = test_api(backend);
        let root = api.root().await.unwrap();
        assert!(!root.conformance.conforms_to.is_empty());
        let catalog: Catalog = serde_json::from_value(serde_json::to_value(root).unwrap()).unwrap();
        // catalog.validate().await.unwrap();
        assert_eq!(catalog.id, "an-id");
        assert_eq!(catalog.description, "a description");
        assert_link!(
            catalog.link("root"),
            "http://stac.test/",
            "application/json"
        );
        assert_link!(
            catalog.link("self"),
            "http://stac.test/",
            "application/json"
        );
        assert_link!(
            catalog.link("service-desc"),
            "http://stac.test/api",
            "application/vnd.oai.openapi+json;version=3.0"
        );
        assert_link!(
            catalog.link("service-doc"),
            "http://stac.test/api.html",
            "text/html"
        );
        assert_link!(
            catalog.link("conformance"),
            "http://stac.test/conformance",
            "application/json"
        );
        assert_link!(
            catalog.link("data"),
            "http://stac.test/collections",
            "application/json"
        );
        let mut methods = HashSet::new();
        let search_links = catalog.links.iter().filter(|link| link.rel == "search");
        for link in search_links {
            assert_eq!(link.href, "http://stac.test/search");
            assert_eq!(link.r#type.as_deref().unwrap(), "application/geo+json");
            let _ = methods.insert(link.method.as_deref().unwrap());
        }
        assert_eq!(methods.len(), 2);
        assert!(methods.contains("GET"));
        assert!(methods.contains("POST"));

        let children: Vec<_> = catalog.iter_child_links().collect();
        assert_eq!(children.len(), 1);
        let child = children[0];
        assert_eq!(child.href, "http://stac.test/collections/a-collection");
        assert_eq!(child.r#type.as_ref().unwrap(), "application/json");
    }

    #[tokio::test]
    async fn conformance() {
        let api = test_api(MemoryBackend::new());
        let conformance = api.conformance();
        for conformance_class in [
            "https://api.stacspec.org/v1.0.0/core",
            "https://api.stacspec.org/v1.0.0/ogcapi-features",
            "https://api.stacspec.org/v1.0.0/collections",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
        ] {
            assert!(
                conformance
                    .conforms_to
                    .contains(&conformance_class.to_string()),
                "{conformance_class} not in the conforms_to list"
            );
        }
    }

    #[tokio::test]
    async fn collections() {
        let mut backend = MemoryBackend::new();
        backend
            .add_collection(Collection::new("a-collection", "A description"))
            .await
            .unwrap();
        let api = test_api(backend);
        let collections = api.collections().await.unwrap();
        assert_link!(
            collections.link("root"),
            "http://stac.test/",
            "application/json"
        );
        assert_link!(
            collections.link("self"),
            "http://stac.test/collections",
            "application/json"
        );
        assert_eq!(collections.collections.len(), 1);
        let collection = &collections.collections[0];
        // collection.validate().await.unwrap();
        assert_link!(
            collection.link("root"),
            "http://stac.test/",
            "application/json"
        );
        assert_link!(
            collection.link("self"),
            "http://stac.test/collections/a-collection",
            "application/json"
        );
        assert_link!(
            collection.link("parent"),
            "http://stac.test/",
            "application/json"
        );
        assert_link!(
            collection.link("items"),
            "http://stac.test/collections/a-collection/items",
            "application/geo+json"
        );
    }

    #[tokio::test]
    async fn collection() {
        let mut backend = MemoryBackend::new();
        backend
            .add_collection(Collection::new("a-collection", "A description"))
            .await
            .unwrap();
        let api = test_api(backend);
        let collection = api.collection("a-collection").await.unwrap().unwrap();
        // collection.validate().await.unwrap();
        assert_link!(
            collection.link("root"),
            "http://stac.test/",
            "application/json"
        );
        assert_link!(
            collection.link("self"),
            "http://stac.test/collections/a-collection",
            "application/json"
        );
        assert_link!(
            collection.link("parent"),
            "http://stac.test/",
            "application/json"
        );
        assert_link!(
            collection.link("items"),
            "http://stac.test/collections/a-collection/items",
            "application/geo+json"
        );
    }

    #[tokio::test]
    async fn items() {
        let mut backend = MemoryBackend::new();
        let api = test_api(backend.clone());
        assert!(
            api.items("collection-id", Items::default())
                .await
                .unwrap()
                .is_none()
        );

        backend
            .add_collection(Collection::new("collection-id", "a description"))
            .await
            .unwrap();
        backend
            .add_item(Item::new("item-a").collection("collection-id"))
            .await
            .unwrap();
        let items = api
            .items("collection-id", Items::default())
            .await
            .unwrap()
            .unwrap();
        assert_link!(items.link("root"), "http://stac.test/", "application/json");
        assert_link!(
            items.link("self"),
            "http://stac.test/collections/collection-id/items",
            "application/geo+json"
        );
        assert_link!(
            items.link("collection"),
            "http://stac.test/collections/collection-id",
            "application/json"
        );
        assert_eq!(items.items.len(), 1);
        let item: Item = items.items[0].clone().try_into().unwrap();
        assert_link!(item.link("root"), "http://stac.test/", "application/json");
        assert_link!(
            item.link("self"),
            "http://stac.test/collections/collection-id/items/item-a",
            "application/geo+json"
        );
        assert_link!(
            item.link("collection"),
            "http://stac.test/collections/collection-id",
            "application/json"
        );
        assert_link!(
            item.link("parent"),
            "http://stac.test/collections/collection-id",
            "application/json"
        );
    }

    #[tokio::test]
    async fn items_pagination() {
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
        let api = test_api(backend);
        let items = Items {
            limit: Some(1),
            ..Default::default()
        };
        let items = api.items("collection-id", items).await.unwrap().unwrap();
        assert_eq!(items.items.len(), 1);
        assert_link!(
            items.link("next"),
            "http://stac.test/collections/collection-id/items?limit=1&skip=1",
            "application/geo+json"
        );

        let mut items = Items {
            limit: Some(1),
            ..Default::default()
        };
        let _ = items
            .additional_fields
            .insert("skip".to_string(), "1".into());
        let items = api.items("collection-id", items).await.unwrap().unwrap();
        assert_eq!(items.items.len(), 1);
        assert_link!(
            items.link("prev"),
            "http://stac.test/collections/collection-id/items?limit=1&skip=0",
            "application/geo+json"
        );
    }

    #[tokio::test]
    async fn item() {
        let mut backend = MemoryBackend::new();
        let api = test_api(backend.clone());
        assert!(
            api.item("collection-id", "item-id")
                .await
                .unwrap()
                .is_none()
        );

        backend
            .add_collection(Collection::new("collection-id", "a description"))
            .await
            .unwrap();
        backend
            .add_item(Item::new("item-id").collection("collection-id"))
            .await
            .unwrap();
        let item = api.item("collection-id", "item-id").await.unwrap().unwrap();
        assert_link!(item.link("root"), "http://stac.test/", "application/json");
        assert_link!(
            item.link("self"),
            "http://stac.test/collections/collection-id/items/item-id",
            "application/geo+json"
        );
        assert_link!(
            item.link("collection"),
            "http://stac.test/collections/collection-id",
            "application/json"
        );
        assert_link!(
            item.link("parent"),
            "http://stac.test/collections/collection-id",
            "application/json"
        );
    }

    #[tokio::test]
    async fn search() {
        let api = test_api(MemoryBackend::new());
        let item_collection = api.search(Search::default(), Method::GET).await.unwrap();
        assert!(item_collection.items.is_empty());
        assert_link!(
            item_collection.link("root"),
            "http://stac.test/",
            "application/json"
        );
    }

    #[test]
    fn memory_item_search_conformance() {
        let api = test_api(MemoryBackend::new());
        let conformance = api.conformance();
        assert!(
            conformance
                .conforms_to
                .contains(&ITEM_SEARCH_URI.to_string())
        );
    }
}
