use std::collections::HashMap;

use crate::{
    Asset, Catalog, Collection, Error, Href, Item, Link, Result, CATALOG_TYPE, COLLECTION_TYPE,
    ITEM_TYPE,
};

/// A type used to pass either an [Object] or an [HrefObject] into functions.
pub type ObjectHrefTuple = (Object, Option<Href>);
const TYPE_FIELD: &str = "type";

/// A wrapper around any of the three main STAC entities: [Item], [Catalog], and [Collection].
#[derive(Debug, PartialEq, Clone)]
pub enum Object {
    /// An [Item].
    Item(Item),

    /// A [Catalog].
    Catalog(Catalog),

    /// A [Collection].
    Collection(Collection),
}

/// An [Object] and an [Href], together.
#[derive(Debug, PartialEq, Clone)]
pub struct HrefObject {
    /// An href to where the object was read from or will be written to.
    pub href: Href,

    /// The actual STAC object.
    pub object: Object,
}

impl Object {
    /// Creates a STAC Object from a JSON value.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Object;
    /// let file = std::fs::File::open("data/catalog.json").unwrap();
    /// let reader = std::io::BufReader::new(file);
    /// let value: serde_json::Value = serde_json::from_reader(reader).unwrap();
    /// let object = Object::from_value(value).unwrap();
    /// ```
    pub fn from_value(value: serde_json::Value) -> Result<Object> {
        if let Some(type_) = value.get(TYPE_FIELD) {
            if let Some(type_) = type_.as_str() {
                match type_ {
                    ITEM_TYPE => Ok(Object::Item(serde_json::from_value(value)?)),
                    CATALOG_TYPE => Ok(Object::Catalog(serde_json::from_value(value)?)),
                    COLLECTION_TYPE => Ok(Object::Collection(serde_json::from_value(value)?)),
                    _ => Err(Error::InvalidTypeValue(type_.to_string())),
                }
            } else {
                Err(Error::InvalidTypeField(type_.clone()))
            }
        } else {
            Err(Error::MissingType)
        }
    }

    /// Returns true if this object is a [Catalog].
    pub fn is_catalog(&self) -> bool {
        matches!(self, Object::Catalog(_))
    }

    /// Returns a reference to this object as a [Catalog], or None if it is not a catalog.
    pub fn as_catalog(&self) -> Option<&Catalog> {
        match &self {
            Object::Catalog(catalog) => Some(catalog),
            _ => None,
        }
    }

    /// Returns a mutable reference to this object as a [Catalog], or None if it is not a Catalog.
    pub fn as_mut_catalog(&mut self) -> Option<&mut Catalog> {
        match self {
            Object::Catalog(catalog) => Some(catalog),
            _ => None,
        }
    }

    /// Returns true if this object is a [Collection].
    pub fn is_collection(&self) -> bool {
        matches!(self, Object::Collection(_))
    }

    /// Returns a reference to this object as a [Collection], or None if it is not a collection.
    pub fn as_collection(&self) -> Option<&Collection> {
        match &self {
            Object::Collection(collection) => Some(collection),
            _ => None,
        }
    }

    /// Returns a reference to this object as a [Collection], or None if it is not a collection.
    pub fn as_mut_collection(&mut self) -> Option<&mut Collection> {
        match self {
            Object::Collection(collection) => Some(collection),
            _ => None,
        }
    }

    /// Returns true if this object is an [Item].
    pub fn is_item(&self) -> bool {
        matches!(self, Object::Item(_))
    }

    /// Returns a reference to this object as an [Item], or None if it is not an item.
    pub fn as_item(&self) -> Option<&Item> {
        match &self {
            Object::Item(item) => Some(item),
            _ => None,
        }
    }

    /// Returns a mutable reference to this object as an [Item], or None if it is not an item.
    pub fn as_mut_item(&mut self) -> Option<&mut Item> {
        match self {
            Object::Item(item) => Some(item),
            _ => None,
        }
    }
    /// Returns this object's type field.
    pub fn r#type(&self) -> &str {
        match &self {
            Object::Item(item) => &item.r#type,
            Object::Catalog(catalog) => &catalog.r#type,
            Object::Collection(collection) => &collection.r#type,
        }
    }

    /// Returns a reference to this object's id.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::{Catalog, Object};
    /// let mut catalog = Catalog::new("id");
    /// assert_eq!(Object::from(catalog).id(), "id");
    /// ```
    pub fn id(&self) -> &str {
        match &self {
            Object::Item(item) => &item.id,
            Object::Catalog(catalog) => &catalog.id,
            Object::Collection(collection) => &collection.id,
        }
    }

    /// Returns a reference to this object's title.
    ///
    /// For [Items](Item), this checks for a `title` field in the
    /// `additional_fields` attribute and returns it as a string if possible.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::{Catalog, Object};
    /// let mut catalog = Catalog::new("id");
    /// catalog.title = Some("Example Catalog".to_string());
    /// assert_eq!(Object::from(catalog).title().unwrap(), "Example Catalog");
    /// ```
    pub fn title(&self) -> Option<&str> {
        match &self {
            Object::Item(item) => item
                .additional_fields
                .get("title")
                .and_then(|value| value.as_str()),
            Object::Catalog(catalog) => catalog.title.as_deref(),
            Object::Collection(collection) => collection.title.as_deref(),
        }
    }

    /// Returns a reference to this object's assets.
    ///
    /// # Examples
    ///
    /// ```
    /// let href_object = stac::read("data/simple-item.json").unwrap();
    /// assert!(href_object.object.assets().is_some());
    /// ```
    pub fn assets(&self) -> Option<&HashMap<String, Asset>> {
        match &self {
            Object::Item(item) => Some(&item.assets),
            Object::Collection(collection) => collection.assets.as_ref(),
            Object::Catalog(_) => None,
        }
    }

    /// Returns a reference to this object's links.
    ///
    /// # Examples
    ///
    /// ```
    /// let href_object = stac::read("data/catalog.json").unwrap();
    /// let links = href_object.object.links();
    /// assert_eq!(links.len(), 6);
    /// ```
    pub fn links(&self) -> &[Link] {
        match &self {
            Object::Item(item) => &item.links,
            Object::Catalog(catalog) => &catalog.links,
            Object::Collection(collection) => &collection.links,
        }
    }

    /// Returns the root link if there is one.
    ///
    /// # Examples
    ///
    /// ```
    /// let object = stac::read("data/catalog.json").unwrap().object;
    /// let root_link = object.root_link().unwrap();
    /// ```
    pub fn root_link(&self) -> Option<&Link> {
        self.links().iter().find(|link| link.is_root())
    }

    /// Returns the parent link if there is one.
    ///
    /// # Examples
    ///
    /// ```
    /// let object = stac::read("data/catalog.json").unwrap().object;
    /// assert!(object.parent_link().is_none());
    /// ```
    pub fn parent_link(&self) -> Option<&Link> {
        self.links().iter().find(|link| link.is_parent())
    }

    /// Returns the self link if there is one.
    ///
    /// # Examples
    ///
    /// ```
    /// let object = stac::read("data/catalog.json").unwrap().object;
    /// assert_eq!(
    ///     object.self_link().unwrap().href,
    ///     "https://raw.githubusercontent.com/radiantearth/stac-spec/v1.0.0/examples/catalog.json"
    /// );
    /// ```
    pub fn self_link(&self) -> Option<&Link> {
        self.links().iter().find(|link| link.is_self())
    }

    /// Iterates over the child links.
    ///
    /// # Examples
    ///
    /// ```
    /// let object = stac::read("data/catalog.json").unwrap().object;
    /// let child_links: Vec<_> = object.child_links().collect();
    /// ```
    pub fn child_links(&self) -> impl Iterator<Item = &Link> {
        self.links().iter().filter(|link| link.is_child())
    }

    /// Adds a link to this object.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::{Link, Object, Item};
    /// let link = Link::new("an-href", "a-rel");
    /// let mut object = Object::from(Item::new("an-id"));
    /// object.add_link(link);
    /// ```
    pub fn add_link(&mut self, link: Link) {
        match self {
            Object::Item(item) => item.links.push(link),
            Object::Catalog(catalog) => catalog.links.push(link),
            Object::Collection(collection) => collection.links.push(link),
        }
    }

    /// Converts this object into a [serde_json::Value].
    ///
    /// TODO can we use serde::serialize w/ untagged?
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::{Object, Item};
    /// let object = Object::from(Item::new("an-id"));
    /// let value = object.into_value().unwrap();
    /// ```
    pub fn into_value(self) -> Result<serde_json::Value> {
        match self {
            Object::Item(item) => serde_json::to_value(item).map_err(Error::from),
            Object::Catalog(catalog) => serde_json::to_value(catalog).map_err(Error::from),
            Object::Collection(collection) => serde_json::to_value(collection).map_err(Error::from),
        }
    }

    pub(crate) fn links_mut(&mut self) -> &mut Vec<Link> {
        match self {
            Object::Item(item) => &mut item.links,
            Object::Catalog(catalog) => &mut catalog.links,
            Object::Collection(collection) => &mut collection.links,
        }
    }
}

impl HrefObject {
    /// Creates a new object with an href.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::{HrefObject, Item, Href};
    /// let item = Item::new("an-id");
    /// let object = HrefObject::new(item, "an-href");
    /// assert_eq!(object.href.as_str(), "an-href");
    /// assert!(object.object.is_item());
    /// ```
    pub fn new(object: impl Into<Object>, href: impl Into<Href>) -> HrefObject {
        HrefObject {
            href: href.into(),
            object: object.into(),
        }
    }
}

impl From<Catalog> for Object {
    fn from(catalog: Catalog) -> Object {
        Object::Catalog(catalog)
    }
}

impl From<Collection> for Object {
    fn from(collection: Collection) -> Object {
        Object::Collection(collection)
    }
}

impl From<Item> for Object {
    fn from(item: Item) -> Object {
        Object::Item(item)
    }
}

impl From<Object> for ObjectHrefTuple {
    fn from(object: Object) -> ObjectHrefTuple {
        (object, None)
    }
}

impl From<HrefObject> for ObjectHrefTuple {
    fn from(href_object: HrefObject) -> ObjectHrefTuple {
        (href_object.object, Some(href_object.href))
    }
}

impl From<Item> for ObjectHrefTuple {
    fn from(item: Item) -> ObjectHrefTuple {
        (Object::Item(item), None)
    }
}

impl From<Collection> for ObjectHrefTuple {
    fn from(collection: Collection) -> ObjectHrefTuple {
        (Object::Collection(collection), None)
    }
}

impl From<Catalog> for ObjectHrefTuple {
    fn from(catalog: Catalog) -> ObjectHrefTuple {
        (Object::Catalog(catalog), None)
    }
}

impl TryFrom<Object> for Catalog {
    type Error = Error;

    fn try_from(object: Object) -> Result<Catalog> {
        match object {
            Object::Catalog(catalog) => Ok(catalog),
            _ => Err(Error::TypeMismatch {
                expected: CATALOG_TYPE.to_string(),
                actual: object.r#type().to_string(),
            }),
        }
    }
}

impl TryFrom<Object> for Collection {
    type Error = Error;

    fn try_from(object: Object) -> Result<Collection> {
        match object {
            Object::Collection(collection) => Ok(collection),
            _ => Err(Error::TypeMismatch {
                expected: COLLECTION_TYPE.to_string(),
                actual: object.r#type().to_string(),
            }),
        }
    }
}

impl TryFrom<Object> for Item {
    type Error = Error;

    fn try_from(object: Object) -> Result<Item> {
        match object {
            Object::Item(item) => Ok(item),
            _ => Err(Error::TypeMismatch {
                expected: ITEM_TYPE.to_string(),
                actual: object.r#type().to_string(),
            }),
        }
    }
}
