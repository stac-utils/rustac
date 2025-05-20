//! Links.

use crate::{Error, Href, Result, SelfHref, mime::APPLICATION_GEOJSON};
use mime::APPLICATION_JSON;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use stac_derive::Fields;

/// Child links.
pub const CHILD_REL: &str = "child";
/// Item link.
pub const ITEM_REL: &str = "item";
/// Parent link.
pub const PARENT_REL: &str = "parent";
/// Root link.
pub const ROOT_REL: &str = "root";
/// Self link.
pub const SELF_REL: &str = "self";
/// Collection link.
pub const COLLECTION_REL: &str = "collection";

/// This object describes a relationship with another entity.
///
/// Data providers are advised to be liberal with the links section, to describe
/// things like the `Catalog`` an `Item` is in,
/// related `Item`s, parent or child `Item`s (modeled in different ways, like an
/// 'acquisition' or derived data). It is allowed to add additional fields such
/// as a title and type.
///
/// This link structure includes a few fields from the [STAC API
/// specification](https://github.com/radiantearth/stac-api-spec/tree/main/item-search#pagination).
/// Generally we keep STAC API structures in the [stac-api
/// crate](https://github.com/stac-utils/rustac/stac-api), but in this case it
/// was simpler to include these attributes in the base [Link] rather to create a new one.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Fields)]
pub struct Link {
    /// The actual link in the format of an URL.
    ///
    /// Relative and absolute links are both allowed.
    pub href: Href,

    /// Relationship between the current document and the linked document.
    ///
    /// See the chapter on ["Relation
    /// types"](https://github.com/radiantearth/stac-spec/blob/master/item-spec/item-spec.md#relation-types)
    /// in the STAC spec for more information.
    pub rel: String,

    /// [Media type](crate::mime) of the referenced entity.
    #[serde(rename = "type")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,

    /// A human readable title to be used in rendered displays of the link.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    /// The HTTP method of the request, usually GET or POST. Defaults to GET.
    ///
    /// From the STAC API spec.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,

    /// A dictionary of header values that must be included in the next request
    ///
    /// From the STAC API spec.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<Map<String, Value>>,

    /// A JSON object containing fields/values that must be included in the body
    /// of the next request.
    ///
    /// From the STAC API spec.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<Map<String, Value>>,

    /// If true, the headers/body fields in the next link must be merged into
    /// the original request and be sent combined in the next request. Defaults
    /// to false
    ///
    /// From the STAC API spec.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merge: Option<bool>,

    /// Additional fields on the link.
    #[serde(flatten)]
    pub additional_fields: Map<String, Value>,
}

/// Implemented by any object that has links.
pub trait Links: SelfHref {
    /// Returns a reference to this object's links.
    ///
    /// # Examples
    ///
    /// `Value` implements Links:
    ///
    /// ```
    /// use stac::Links;
    /// let item: stac::Item = stac::read("examples/simple-item.json").unwrap();
    /// let links = item.links();
    /// ```
    fn links(&self) -> &[Link];

    /// Returns a mutable reference to this object's links.
    ///
    /// # Examples
    ///
    /// `Value`` implements Links:
    ///
    /// ```
    /// use stac::Links;
    /// let mut item: stac::Item = stac::read("examples/simple-item.json").unwrap();
    /// let links = item.links_mut();
    /// links.clear();
    /// ```
    fn links_mut(&mut self) -> &mut Vec<Link>;

    /// Returns the first link with the given rel type.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Links;
    /// let item: stac::Item = stac::read("examples/simple-item.json").unwrap();
    /// let link = item.link("root").unwrap();
    /// ```
    fn link(&self, rel: &str) -> Option<&Link> {
        self.links().iter().find(|link| link.rel == rel)
    }

    /// Sets a link of the given rel type.
    ///
    /// This will remove all other links of that rel type, so should only be
    /// used for e.g. "root", not e.g. "child".
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Links, Link};
    /// let mut item: stac::Item = stac::read("examples/simple-item.json").unwrap();
    /// item.set_link(Link::root("a/href"));
    /// ```
    fn set_link(&mut self, link: Link) {
        self.links_mut().retain(|l| l.rel != link.rel);
        self.links_mut().push(link)
    }

    /// Returns this object's root link.
    ///
    /// This is the first link with a rel="root".
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Links;
    /// let item: stac::Item = stac::read("examples/simple-item.json").unwrap();
    /// let link = item.root_link().unwrap();
    /// ```
    fn root_link(&self) -> Option<&Link> {
        self.links().iter().find(|link| link.is_root())
    }

    /// Returns this object's self link.
    ///
    /// This is the first link with a rel="self".
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Links;
    /// let item: stac::Item = stac::read("examples/simple-item.json").unwrap();
    /// let link = item.root_link().unwrap();
    /// ```
    fn self_link(&self) -> Option<&Link> {
        self.links().iter().find(|link| link.is_self())
    }

    /// Returns this object's parent link.
    ///
    /// This is the first link with a rel="parent".
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Links;
    /// let item: stac::Item = stac::read("examples/simple-item.json").unwrap();
    /// let link = item.parent_link().unwrap();
    /// ```
    fn parent_link(&self) -> Option<&Link> {
        self.links().iter().find(|link| link.is_parent())
    }

    /// Returns an iterator over this object's child links.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Links;
    /// let collection: stac::Collection = stac::read("examples/collection.json").unwrap();
    /// let links: Vec<_> = collection.iter_child_links().collect();
    /// ```
    fn iter_child_links(&self) -> Box<dyn Iterator<Item = &Link> + '_> {
        Box::new(self.links().iter().filter(|link| link.is_child()))
    }

    /// Returns an iterator over this object's item links.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Links;
    /// let collection: stac::Collection = stac::read("examples/collection.json").unwrap();
    /// let links: Vec<_> = collection.iter_item_links().collect();
    /// ```
    fn iter_item_links(&self) -> Box<dyn Iterator<Item = &Link> + '_> {
        Box::new(self.links().iter().filter(|link| link.is_item()))
    }

    /// Makes all relative links absolute with respect to this object's self href.
    fn make_links_absolute(&mut self) -> Result<()> {
        if let Some(href) = self.self_href().cloned() {
            for link in self.links_mut() {
                link.make_absolute(&href)?;
            }
            Ok(())
        } else {
            Err(Error::NoHref)
        }
    }

    /// Makes all links relative with respect to this object's self href.
    fn make_links_relative(&mut self) -> Result<()> {
        if let Some(href) = self.self_href().cloned() {
            for link in self.links_mut() {
                link.make_relative(&href)?;
            }
            Ok(())
        } else {
            Err(Error::NoHref)
        }
    }

    /// Removes all relative links.
    ///
    /// This can be useful e.g. if you're relocating a STAC object, but it
    /// doesn't have a href, so the relative links wouldn't make any sense.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Catalog, Links, Link};
    /// let mut catalog = Catalog::new("an-id", "a description");
    /// catalog.links.push(Link::new("./child.json", "child"));
    /// catalog.remove_relative_links();
    /// assert!(catalog.links.is_empty());
    /// ```
    fn remove_relative_links(&mut self) {
        self.links_mut().retain(|link| link.is_absolute())
    }

    /// Removes all structural links.
    ///
    /// Useful if you're, e.g., going to re-populate the structural links as a
    /// part of serving items with a STAC API.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Catalog, Links, Link};
    /// let mut catalog = Catalog::new("an-id", "a description");
    /// catalog.links.push(Link::self_("http://stac.test/catalog.json"));
    /// catalog.remove_structural_links();
    /// assert!(catalog.links.is_empty());
    /// ```
    fn remove_structural_links(&mut self) {
        self.links_mut().retain(|link| !link.is_structural())
    }
}

impl Link {
    /// Creates a new link with the provided href and rel type.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Link;
    /// let link = Link::new("an-href", "a-rel");
    /// assert_eq!(link.href, "an-href");
    /// assert_eq!(link.rel, "a-rel");
    /// ```
    pub fn new(href: impl Into<Href>, rel: impl ToString) -> Link {
        Link {
            href: href.into(),
            rel: rel.to_string(),
            r#type: None,
            title: None,
            method: None,
            headers: None,
            body: None,
            merge: None,
            additional_fields: Map::new(),
        }
    }

    /// Sets this link's media type to JSON.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Link;
    /// let link = Link::new("a/href", "rel-type").json();
    /// assert_eq!(link.r#type.unwrap(), ::mime::APPLICATION_JSON.as_ref());
    /// ```
    pub fn json(mut self) -> Link {
        self.r#type = Some(APPLICATION_JSON.to_string());
        self
    }

    /// Returns true if this link's media type is JSON.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Link;
    /// let link = Link::new("a/href", "rel-type").json();
    /// assert!(link.is_json());
    /// ```
    pub fn is_json(&self) -> bool {
        self.r#type
            .as_ref()
            .map(|t| t == APPLICATION_JSON.as_ref())
            .unwrap_or(false)
    }

    /// Sets this link's media type to GeoJSON.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Link, mime};
    /// let link = Link::new("a/href", "rel-type").geojson();
    /// assert_eq!(link.r#type.unwrap(), mime::GEOJSON);
    /// ```
    pub fn geojson(mut self) -> Link {
        self.r#type = Some(APPLICATION_GEOJSON.to_string());
        self
    }

    /// Returns true if this link's media type is GeoJSON.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Link;
    /// let link = Link::new("a/href", "rel-type").geojson();
    /// assert!(link.is_geojson());
    /// ```
    pub fn is_geojson(&self) -> bool {
        self.r#type
            .as_ref()
            .map(|t| t == APPLICATION_GEOJSON)
            .unwrap_or(false)
    }

    /// Sets this link's media type.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Link, mime};
    /// let link = Link::new("a/href", "rel-type").r#type(mime::GEOJSON.to_string());
    /// assert_eq!(link.r#type.unwrap(), mime::GEOJSON);
    /// ```
    pub fn r#type(mut self, r#type: impl Into<Option<String>>) -> Link {
        self.r#type = r#type.into();
        self
    }

    /// Sets this link's title.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Link;
    /// let link = Link::new("a/href", "rel-type").title("a title".to_string());
    /// assert_eq!(link.title.unwrap(), "a title");
    /// ```
    pub fn title(mut self, title: impl Into<Option<String>>) -> Link {
        self.title = title.into();
        self
    }

    /// Creates a new root link with JSON media type.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Link;
    /// let link = Link::root("an-href");
    /// assert!(link.is_root());
    /// assert_eq!(link.r#type.as_ref().unwrap(), ::mime::APPLICATION_JSON.as_ref());
    /// ```
    pub fn root(href: impl Into<Href>) -> Link {
        Link::new(href, ROOT_REL).json()
    }

    /// Creates a new self link with JSON media type.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Link;
    /// let link = Link::self_("an-href");
    /// assert!(link.is_self());
    /// assert_eq!(link.r#type.as_ref().unwrap(), ::mime::APPLICATION_JSON.as_ref());
    /// ```
    pub fn self_(href: impl Into<Href>) -> Link {
        Link::new(href, SELF_REL).json()
    }

    /// Creates a new child link with JSON media type.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Link;
    /// let link = Link::child("an-href");
    /// assert!(link.is_child());
    /// assert_eq!(link.r#type.as_ref().unwrap(), ::mime::APPLICATION_JSON.as_ref());
    /// ```
    pub fn child(href: impl Into<Href>) -> Link {
        Link::new(href, CHILD_REL).json()
    }

    /// Creates a new item link with JSON media type.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Link;
    /// let link = Link::item("an-href");
    /// assert!(link.is_item());
    /// assert_eq!(link.r#type.as_ref().unwrap(), ::mime::APPLICATION_JSON.as_ref());
    /// ```
    pub fn item(href: impl Into<Href>) -> Link {
        Link::new(href, ITEM_REL).json()
    }

    /// Creates a new parent link with JSON media type.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Link;
    /// let link = Link::parent("an-href");
    /// assert!(link.is_parent());
    /// assert_eq!(link.r#type.as_ref().unwrap(), ::mime::APPLICATION_JSON.as_ref());
    /// ```
    pub fn parent(href: impl Into<Href>) -> Link {
        Link::new(href, PARENT_REL).json()
    }

    /// Creates a new collection link with JSON media type.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Link;
    /// let link = Link::collection("an-href");
    /// assert!(link.is_collection());
    /// assert_eq!(link.r#type.as_ref().unwrap(), ::mime::APPLICATION_JSON.as_ref());
    /// ```
    pub fn collection(href: impl Into<Href>) -> Link {
        Link::new(href, COLLECTION_REL).json()
    }

    /// Returns true if this link's rel is `"item"`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Link;
    /// let link = Link::new("an-href", "item");
    /// assert!(link.is_item());
    /// let link = Link::new("an-href", "not-an-item");
    /// assert!(!link.is_item());
    /// ```
    pub fn is_item(&self) -> bool {
        self.rel == ITEM_REL
    }

    /// Returns true if this link's rel is `"child"`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Link;
    /// let link = Link::new("an-href", "child");
    /// assert!(link.is_child());
    /// let link = Link::new("an-href", "not-a-child");
    /// assert!(!link.is_child());
    /// ```
    pub fn is_child(&self) -> bool {
        self.rel == CHILD_REL
    }

    /// Returns true if this link's rel is `"parent"`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Link;
    /// let link = Link::new("an-href", "parent");
    /// assert!(link.is_parent());
    /// let link = Link::new("an-href", "not-a-parent");
    /// assert!(!link.is_parent());
    /// ```
    pub fn is_parent(&self) -> bool {
        self.rel == PARENT_REL
    }

    /// Returns true if this link's rel is `"root"`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Link;
    /// let link = Link::new("an-href", "root");
    /// assert!(link.is_root());
    /// let link = Link::new("an-href", "not-a-root");
    /// assert!(!link.is_root());
    /// ```
    pub fn is_root(&self) -> bool {
        self.rel == ROOT_REL
    }

    /// Returns true if this link's rel is `"self"`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Link;
    /// let link = Link::new("an-href", "self");
    /// assert!(link.is_self());
    /// let link = Link::new("an-href", "not-a-self");
    /// assert!(!link.is_self());
    /// ```
    pub fn is_self(&self) -> bool {
        self.rel == SELF_REL
    }

    /// Returns true if this link's rel is `"collection"`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Link;
    /// let link = Link::new("an-href", "collection");
    /// assert!(link.is_collection());
    /// let link = Link::new("an-href", "not-a-collection");
    /// assert!(!link.is_collection());
    /// ```
    pub fn is_collection(&self) -> bool {
        self.rel == COLLECTION_REL
    }

    /// Returns true if this link is structural (i.e. not child, parent, item,
    /// root, or self).
    ///
    /// Also includes some API structural link types such as "data",
    /// "conformance", "items", and "search".
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Link;
    /// let link = Link::new("an-href", "self");
    /// assert!(link.is_structural());
    /// let link = Link::new("an-href", "child");
    /// assert!(link.is_structural());
    /// let link = Link::new("an-href", "not-a-root");
    /// assert!(!link.is_structural());
    pub fn is_structural(&self) -> bool {
        self.is_child()
            || self.is_item()
            || self.is_parent()
            || self.is_root()
            || self.is_self()
            || self.is_collection()
            || self.rel == "data"
            || self.rel == "conformance"
            || self.rel == "items"
            || self.rel == "search"
            || self.rel == "service-desc"
            || self.rel == "service-doc"
            || self.rel == "next"
            || self.rel == "prev"
    }

    /// Returns true if this link's href is an absolute path or url.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Link;
    ///
    /// assert!(Link::new("/a/local/path/item.json", "rel").is_absolute());
    /// assert!(Link::new("http://rustac.test/item.json", "rel").is_absolute());
    /// assert!(!Link::new("./not/an/absolute/path", "rel").is_absolute());
    /// ```
    pub fn is_absolute(&self) -> bool {
        self.href.is_absolute()
    }

    /// Returns true if this link's href is a relative path.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Link;
    ///
    /// assert!(!Link::new("/a/local/path/item.json", "rel").is_relative());
    /// assert!(!Link::new("http://rustac.test/item.json", "rel").is_relative());
    /// assert!(Link::new("./not/an/absolute/path", "rel").is_relative());
    /// ```
    pub fn is_relative(&self) -> bool {
        !self.href.is_absolute()
    }

    /// Sets the method attribute on this link.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Link;
    /// let link = Link::new("href", "rel").method("GET");
    /// ```
    pub fn method(mut self, method: impl ToString) -> Link {
        self.method = Some(method.to_string());
        self
    }

    /// Sets the body attribute on this link.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Link;
    /// use serde_json::json;
    ///
    /// let link = Link::new("href", "rel").body(json!({"foo": "bar"})).unwrap();
    /// ```
    pub fn body<T: Serialize>(mut self, body: T) -> Result<Link> {
        match serde_json::to_value(body)? {
            Value::Object(body) => {
                self.body = Some(body);
                Ok(self)
            }
            value => Err(Error::IncorrectType {
                actual: value.to_string(),
                expected: "object".to_string(),
            }),
        }
    }

    /// Makes this link absolute.
    ///
    /// If the href is relative, use the passed in value as a base.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Link, Href};
    ///
    /// let mut link = Link::new("./b/item.json", "rel");
    /// link.make_absolute(Href::from("/a/base/catalog.json")).unwrap();
    /// assert_eq!(link.href, "/a/base/b/item.json")
    /// ```
    pub fn make_absolute(&mut self, base: impl AsRef<Href>) -> Result<()> {
        self.href = self.href.into_absolute(base)?;
        Ok(())
    }

    /// Makes this link relative
    pub fn make_relative(&mut self, base: impl AsRef<Href>) -> Result<()> {
        self.href = self.href.into_relative(base)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Link;

    #[test]
    fn new() {
        let link = Link::new("an-href", "a-rel");
        assert_eq!(link.href, "an-href");
        assert_eq!(link.rel, "a-rel");
        assert!(link.r#type.is_none());
        assert!(link.title.is_none());
    }

    #[test]
    fn skip_serializing() {
        let link = Link::new("an-href", "a-rel");
        let value = serde_json::to_value(link).unwrap();
        assert!(value.get("type").is_none());
        assert!(value.get("title").is_none());
    }

    mod links {
        use crate::{Catalog, Item, Link, Links};

        #[test]
        fn link() {
            let mut item = Item::new("an-item");
            assert!(item.link("root").is_none());
            item.links.push(Link::new("an-href", "root"));
            assert!(item.link("root").is_some());
        }

        #[test]
        fn root() {
            let mut item = Item::new("an-item");
            assert!(item.root_link().is_none());
            item.links.push(Link::new("an-href", "root"));
            assert!(item.root_link().is_some());
        }

        #[test]
        fn self_() {
            let mut item = Item::new("an-item");
            assert!(item.self_link().is_none());
            item.links.push(Link::new("an-href", "self"));
            assert!(item.self_link().is_some());
        }

        #[test]
        fn remove_relative_links() {
            let mut catalog = Catalog::new("an-id", "a description");
            catalog.links.push(Link::new("./child.json", "child"));
            catalog.links.push(Link::new("/child.json", "child"));
            catalog
                .links
                .push(Link::new("http://rustac.test/child.json", "child"));
            catalog.remove_relative_links();
            assert_eq!(catalog.links.len(), 2);
        }
    }
}
