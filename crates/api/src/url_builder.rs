use std::str::FromStr;
use url::{ParseError, Url};

/// Builds urls on a root url.
///
/// # Examples
///
/// ```
/// # use stac_api::UrlBuilder;
/// let url_builder = UrlBuilder::new("http://stac-api.test/api/v1").unwrap();
/// assert_eq!(
///     url_builder.items("my-great-collection").unwrap().as_str(),
///     "http://stac-api.test/api/v1/collections/my-great-collection/items"
/// );
/// ```
#[derive(Clone, Debug)]
pub struct UrlBuilder {
    root: Url,
    collections: Url,
    collections_with_slash: Url,
    conformance: Url,
    service_desc: Url,
    search: Url,
}

impl UrlBuilder {
    /// Creates a new url builder.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac_api::UrlBuilder;
    /// let url_builder = UrlBuilder::new("http://stac-api.test").unwrap();
    /// ```
    pub fn new(url: &str) -> Result<UrlBuilder, ParseError> {
        let root: Url = if url.ends_with('/') {
            url.parse()?
        } else {
            format!("{url}/").parse()?
        };
        Ok(UrlBuilder {
            collections: root.join("collections")?,
            collections_with_slash: root.join("collections/")?,
            conformance: root.join("conformance")?,
            service_desc: root.join("api")?,
            search: root.join("search")?,
            root,
        })
    }

    /// Returns the root url.
    ///
    /// The root url always has a trailing slash, even if the builder was
    /// created without one.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac_api::UrlBuilder;
    /// let url_builder = UrlBuilder::new("http://stac-api.test").unwrap();
    /// assert_eq!(url_builder.root().as_str(), "http://stac-api.test/");
    /// ```
    pub fn root(&self) -> &Url {
        &self.root
    }

    /// Returns the collections url.
    ///
    /// This url is created when the builder is created, so it can't error.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac_api::UrlBuilder;
    /// let url_builder = UrlBuilder::new("http://stac-api.test").unwrap();
    /// assert_eq!(url_builder.collections().as_str(), "http://stac-api.test/collections");
    /// ```
    pub fn collections(&self) -> &Url {
        &self.collections
    }

    /// Returns a collection url.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac_api::UrlBuilder;
    /// let url_builder = UrlBuilder::new("http://stac-api.test").unwrap();
    /// assert_eq!(
    ///     url_builder.collection("a-collection").unwrap().as_str(),
    ///     "http://stac-api.test/collections/a-collection"
    /// );
    /// ```
    pub fn collection(&self, id: &str) -> Result<Url, ParseError> {
        self.collections_with_slash.join(id)
    }

    /// Returns an items url.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac_api::UrlBuilder;
    /// let url_builder = UrlBuilder::new("http://stac-api.test").unwrap();
    /// assert_eq!(
    ///     url_builder.items("a-collection").unwrap().as_str(),
    ///     "http://stac-api.test/collections/a-collection/items"
    /// );
    /// ```
    pub fn items(&self, id: &str) -> Result<Url, ParseError> {
        self.collections_with_slash.join(&format!("{id}/items"))
    }

    /// Returns a item url.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac_api::UrlBuilder;
    /// let url_builder = UrlBuilder::new("http://stac-api.test").unwrap();
    /// assert_eq!(
    ///     url_builder.item("a-collection", "an-item").unwrap().as_str(),
    ///     "http://stac-api.test/collections/a-collection/items/an-item"
    /// );
    /// ```
    pub fn item(&self, collection_id: &str, id: &str) -> Result<Url, ParseError> {
        self.collections_with_slash
            .join(&format!("{collection_id}/items/{id}"))
    }

    /// Returns the conformance url.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac_api::UrlBuilder;
    /// let url_builder = UrlBuilder::new("http://stac-api.test").unwrap();
    /// assert_eq!(
    ///     url_builder.conformance().as_str(),
    ///     "http://stac-api.test/conformance"
    /// );
    /// ```
    pub fn conformance(&self) -> &Url {
        &self.conformance
    }

    /// Returns the service-desc url.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac_api::UrlBuilder;
    /// let url_builder = UrlBuilder::new("http://stac-api.test").unwrap();
    /// assert_eq!(
    ///     url_builder.service_desc().as_str(),
    ///     "http://stac-api.test/api"
    /// );
    /// ```
    pub fn service_desc(&self) -> &Url {
        &self.service_desc
    }

    /// Returns the search url.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac_api::UrlBuilder;
    /// let url_builder = UrlBuilder::new("http://stac-api.test").unwrap();
    /// assert_eq!(
    ///     url_builder.search().as_str(),
    ///     "http://stac-api.test/search"
    /// );
    /// ```
    pub fn search(&self) -> &Url {
        &self.search
    }
}

impl FromStr for UrlBuilder {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        UrlBuilder::new(s)
    }
}
