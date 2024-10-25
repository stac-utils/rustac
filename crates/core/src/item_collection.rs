use crate::{Error, Href, Item, Link, Links, Migrate};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{ops::Deref, vec::IntoIter};

/// A [GeoJSON FeatureCollection](https://www.rfc-editor.org/rfc/rfc7946#page-12) of items.
///
/// While not part of the STAC specification, ItemCollections are often used to store many items in a single file.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(tag = "type", rename = "FeatureCollection")]
pub struct ItemCollection {
    /// The list of [Items](Item).
    ///
    /// The attribute is actually "features", but we rename to "items".
    #[serde(rename = "features")]
    pub items: Vec<Item>,

    /// List of link objects to resources and related URLs.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub links: Vec<Link>,

    /// Additional fields.
    #[serde(flatten)]
    pub additional_fields: Map<String, Value>,

    #[serde(skip)]
    href: Option<String>,
}

impl From<Vec<Item>> for ItemCollection {
    fn from(items: Vec<Item>) -> Self {
        ItemCollection {
            items,
            links: Vec::new(),
            additional_fields: Map::new(),
            href: None,
        }
    }
}

impl FromIterator<Item> for ItemCollection {
    fn from_iter<I: IntoIterator<Item = Item>>(iter: I) -> Self {
        iter.into_iter().collect::<Vec<_>>().into()
    }
}

impl IntoIterator for ItemCollection {
    type IntoIter = IntoIter<Item>;
    type Item = Item;
    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl Deref for ItemCollection {
    type Target = Vec<Item>;
    fn deref(&self) -> &Self::Target {
        &self.items
    }
}

impl Href for ItemCollection {
    fn href(&self) -> Option<&str> {
        self.href.as_deref()
    }

    fn set_href(&mut self, href: impl ToString) {
        self.href = Some(href.to_string())
    }

    fn clear_href(&mut self) {
        self.href = None;
    }
}

impl Links for ItemCollection {
    fn links(&self) -> &[Link] {
        &self.links
    }
    fn links_mut(&mut self) -> &mut Vec<Link> {
        &mut self.links
    }
}

impl Migrate for ItemCollection {
    fn migrate(mut self, version: &crate::Version) -> crate::Result<Self> {
        let mut items = Vec::with_capacity(self.items.len());
        for item in self.items {
            items.push(item.migrate(version)?);
        }
        self.items = items;
        Ok(self)
    }
}

impl TryFrom<Value> for ItemCollection {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match serde_json::from_value::<ItemCollection>(value.clone()) {
            Ok(item_collection) => Ok(item_collection),
            Err(err) => {
                if let Value::Array(array) = value {
                    let mut items = Vec::new();
                    for item in array {
                        items.push(serde_json::from_value(item)?);
                    }
                    Ok(items.into())
                } else {
                    Err(Error::from(err))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ItemCollection;
    use crate::Item;

    #[test]
    fn item_collection_from_vec() {
        let items = vec![Item::new("a"), Item::new("b")];
        let _ = ItemCollection::from(items);
    }

    #[test]
    fn item_collection_from_iter() {
        let items = vec![Item::new("a"), Item::new("b")];
        let _ = ItemCollection::from_iter(items.into_iter());
    }
}
