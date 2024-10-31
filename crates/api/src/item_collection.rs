use crate::{Item, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use stac::Link;
use stac_derive::{Href, Links};

/// The return value of the `/items` and `/search` endpoints.
///
/// This might be a [stac::ItemCollection], but if the [fields
/// extension](https://github.com/stac-api-extensions/fields) is used, it might
/// not be. Defined by the [itemcollection
/// fragment](https://github.com/radiantearth/stac-api-spec/blob/main/fragments/itemcollection/README.md).
#[derive(Debug, Serialize, Deserialize, Default, Links, Href)]
#[serde(tag = "type", rename = "FeatureCollection")]
pub struct ItemCollection {
    /// A possibly-empty array of Item objects.
    #[serde(rename = "features")]
    pub items: Vec<Item>,

    /// An array of Links related to this ItemCollection.
    pub links: Vec<Link>,

    /// The number of Items that meet the selection parameters, possibly estimated.
    #[serde(skip_serializing_if = "Option::is_none", rename = "numberMatched")]
    pub number_matched: Option<u64>,

    /// The number of Items in the features array.
    #[serde(skip_serializing_if = "Option::is_none", rename = "numberReturned")]
    pub number_returned: Option<u64>,

    /// The search-related metadata for the [ItemCollection].
    ///
    /// Part of the [context extension](https://github.com/stac-api-extensions/context).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Context>,

    /// Additional fields.
    #[serde(flatten)]
    pub additional_fields: Map<String, Value>,

    /// Optional pagination information for the next page.
    ///
    /// This is not part of the specification, but can be used to hold arbitrary
    /// pagination information (tokens) to later be turned into links.
    #[serde(skip)]
    pub next: Option<Map<String, Value>>,

    /// Optional pagination information for the previous page.
    ///
    /// This is not part of the specification, but can be used to hold arbitrary
    /// pagination information (tokens) to later be turned into links.
    #[serde(skip)]
    pub prev: Option<Map<String, Value>>,

    /// Optional pagination information for the first page.
    ///
    /// This is not part of the specification, but can be used to hold arbitrary
    /// pagination information (tokens) to later be turned into links.
    #[serde(skip)]
    pub first: Option<Map<String, Value>>,

    /// Optional pagination information for the last page.
    ///
    /// This is not part of the specification, but can be used to hold arbitrary
    /// pagination information (tokens) to later be turned into links.
    #[serde(skip)]
    pub last: Option<Map<String, Value>>,

    #[serde(skip)]
    href: Option<String>,
}

/// The search-related metadata for the [ItemCollection].
///
/// Part of the [context extension](https://github.com/stac-api-extensions/context).
#[derive(Debug, Serialize, Deserialize)]
pub struct Context {
    /// The count of results returned by this response. Equal to the cardinality
    /// of features array.
    pub returned: u64,

    /// The maximum number of results to which the result was limited.
    pub limit: Option<u64>,

    /// The count of total number of results that match for this query, possibly
    /// estimated, particularly in the context of NoSQL data stores.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched: Option<u64>,

    /// Additional fields.
    #[serde(flatten)]
    pub additional_fields: Map<String, Value>,
}

impl ItemCollection {
    /// Creates a new [ItemCollection] from a vector of items.
    ///
    /// # Examples
    ///
    /// ```
    /// let item: stac_api::Item = stac::Item::new("an-id").try_into().unwrap();
    /// let item_collection = stac_api::ItemCollection::new(vec![item]).unwrap();
    /// ```
    pub fn new(items: Vec<Item>) -> Result<ItemCollection> {
        let number_returned = items.len();
        Ok(ItemCollection {
            items,
            links: Vec::new(),
            number_matched: None,
            number_returned: Some(number_returned.try_into()?),
            context: None,
            additional_fields: Map::new(),
            next: None,
            prev: None,
            first: None,
            last: None,
            href: None,
        })
    }
}

impl From<Vec<Item>> for ItemCollection {
    fn from(items: Vec<Item>) -> Self {
        ItemCollection {
            items,
            ..Default::default()
        }
    }
}
