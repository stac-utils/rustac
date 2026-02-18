use crate::{
    Asset, Assets, Bbox, Error, Item, ItemAsset, Link, Links, Migrate, Result, STAC_VERSION,
    SelfHref, Version,
};
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use stac_derive::{Fields, Links, SelfHref};

const DEFAULT_LICENSE: &str = "other";

const COLLECTION_TYPE: &str = "Collection";

fn collection_type() -> String {
    COLLECTION_TYPE.to_string()
}

fn deserialize_collection_type<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let r#type = String::deserialize(deserializer)?;
    if r#type != COLLECTION_TYPE {
        Err(serde::de::Error::invalid_value(
            serde::de::Unexpected::Str(&r#type),
            &COLLECTION_TYPE,
        ))
    } else {
        Ok(r#type)
    }
}

/// The STAC `Collection` Specification defines a set of common fields to describe
/// a group of [Items](crate::Item) that share properties and metadata.
///
/// The `Collection` Specification shares all fields with the STAC
/// [Catalog](crate::Catalog) Specification (with different allowed values for
/// `type` and `extensions`) and adds fields to describe the whole dataset and
/// the included set of `Item`s.  `Collection`s can have both parent `Catalogs` and
/// `Collection`s and child `Item`s, `Catalog`s and `Collection`s.
///
/// A STAC `Collection` is represented in JSON format. Any JSON object that
/// contains all the required fields is a valid STAC `Collection` and also a valid
/// STAC `Catalog`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, SelfHref, Links, Fields)]
pub struct Collection {
    #[serde(
        default = "collection_type",
        deserialize_with = "deserialize_collection_type"
    )]
    r#type: String,

    /// The STAC version the `Collection` implements.
    #[serde(rename = "stac_version", default)]
    pub version: Version,

    /// A list of extension identifiers the `Collection` implements.
    #[serde(rename = "stac_extensions")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub extensions: Vec<String>,

    /// Identifier for the `Collection` that is unique across the provider.
    #[serde(default)]
    pub id: String,

    /// A short descriptive one-line title for the `Collection`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    /// Detailed multi-line description to fully explain the `Collection`.
    ///
    /// [CommonMark 0.29](http://commonmark.org/) syntax MAY be used for rich text representation.
    #[serde(default)]
    pub description: String,

    /// List of keywords describing the `Collection`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keywords: Option<Vec<String>>,

    /// `Collection`'s license(s), either a SPDX [License
    /// identifier](https://spdx.org/licenses/), `"various"` if multiple licenses
    /// apply or `"proprietary"` for all other cases.
    #[serde(default)]
    pub license: String,

    /// A list of [providers](Provider), which may include all organizations capturing or
    /// processing the data or the hosting provider.
    ///
    /// Providers should be listed in chronological order with the most recent
    /// provider being the last element of the list.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub providers: Option<Vec<Provider>>,

    /// Spatial and temporal extents.
    #[serde(default)]
    pub extent: Extent,

    /// A map of property summaries, either a set of values, a range of values
    /// or a [JSON Schema](https://json-schema.org).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summaries: Option<Map<String, Value>>,

    /// A list of references to other documents.
    #[serde(default)]
    pub links: Vec<Link>,

    /// Dictionary of asset objects that can be downloaded, each with a unique key.
    #[serde(skip_serializing_if = "IndexMap::is_empty", default)]
    pub assets: IndexMap<String, Asset>,

    /// A dictionary of assets that can be found in member Items.
    #[serde(skip_serializing_if = "IndexMap::is_empty", default)]
    pub item_assets: IndexMap<String, ItemAsset>,

    /// Additional fields not part of the `Collection` specification.
    #[serde(flatten)]
    pub additional_fields: Map<String, Value>,

    #[serde(skip)]
    self_href: Option<String>,
}

/// This object provides information about a provider.
///
/// A provider is any of the organizations that captures or processes the
/// content of the [Collection](crate::Collection) and therefore influences the
/// data offered by this `Collection`. May also include information about the
/// final storage provider hosting the data.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Provider {
    /// The name of the organization or the individual.
    pub name: String,

    /// Multi-line description to add further provider information such as
    /// processing details for processors and producers, hosting details for
    /// hosts or basic contact information.
    ///
    /// [CommonMark 0.29](http://commonmark.org/) syntax MAY be used for rich text representation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Roles of the provider.
    ///
    /// Any of `"licensor"`, `"producer"`, `"processor"`, or `"host"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub roles: Option<Vec<String>>,

    /// Homepage on which the provider describes the dataset and publishes contact information.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    /// Additional fields on the provider.
    #[serde(flatten)]
    pub additional_fields: Map<String, Value>,
}

/// The object describes the spatio-temporal extents of the [Collection](crate::Collection).
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Clone)]
pub struct Extent {
    /// Spatial extents covered by the `Collection`.
    pub spatial: SpatialExtent,
    /// Temporal extents covered by the `Collection`.
    pub temporal: TemporalExtent,

    /// Additional fields on the extent.
    #[serde(flatten)]
    pub additional_fields: Map<String, Value>,
}

/// The object describes the spatial extents of the Collection.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct SpatialExtent {
    /// Potential spatial extents covered by the Collection.
    pub bbox: Vec<Bbox>,
}

/// The object describes the temporal extents of the Collection.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TemporalExtent {
    /// Potential temporal extents covered by the Collection.
    pub interval: Vec<[Option<DateTime<Utc>>; 2]>,
}

impl Collection {
    /// Creates a new `Collection` with the given `id`.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Collection;
    /// let collection = Collection::new("an-id", "a description");
    /// assert_eq!(collection.id, "an-id");
    /// assert_eq!(collection.description, "a description");
    /// ```
    pub fn new(id: impl ToString, description: impl ToString) -> Collection {
        Collection {
            r#type: collection_type(),
            version: STAC_VERSION,
            extensions: Vec::new(),
            id: id.to_string(),
            title: None,
            description: description.to_string(),
            keywords: None,
            license: DEFAULT_LICENSE.to_string(),
            providers: None,
            extent: Extent::default(),
            summaries: None,
            links: Vec::new(),
            assets: IndexMap::new(),
            item_assets: IndexMap::new(),
            additional_fields: Map::new(),
            self_href: None,
        }
    }

    /// Creates a new collection with the given id and values populated from the
    /// provided items.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, Collection};
    ///
    /// let simple_item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let extended_item: Item = stac::read("examples/extended-item.json").unwrap();
    /// let collection = Collection::from_id_and_items("an-id", &[simple_item, extended_item]);
    /// ```
    pub fn from_id_and_items(id: impl ToString, items: &[Item]) -> Collection {
        let description = format!(
            "This collection was generated by rustac v{} from {} items",
            env!("CARGO_PKG_VERSION"),
            items.len()
        );
        if items.is_empty() {
            Collection::new(id, description)
        } else {
            let mut collection = Collection::new_from_item(id, description, &items[0]);
            for item in items.iter().skip(1) {
                let _ = collection.add_item(item);
            }
            collection
        }
    }

    /// Creates a new collection with its extents set to match the item's.
    ///
    /// Also, adds an `item` link if the item has a href or a `item`.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, Collection};
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let collection = Collection::new_from_item("an-id", "a description", &item);
    /// ```
    pub fn new_from_item(id: impl ToString, description: impl ToString, item: &Item) -> Collection {
        let mut collection = Collection::new(id, description);
        if let Some(bbox) = item.bbox {
            collection.extent.spatial.bbox[0] = bbox;
        }
        let (start, end) = item.datetimes();
        collection.extent.temporal.update(start, end);
        let _ = collection.maybe_add_item_link(item);
        collection
    }

    fn update_extents(&mut self, item: &Item) {
        if let Some(bbox) = item.bbox {
            self.extent.spatial.update(bbox);
        }
        let (start, end) = item.datetimes();
        self.extent.temporal.update(start, end);
    }

    fn maybe_add_item_link(&mut self, item: &Item) -> Option<&Link> {
        if let Some(href) = item
            .self_href()
            .or(item.self_link().map(|link| link.href.as_str()))
        {
            self.links.push(Link::item(href));
            self.links.last()
        } else {
            None
        }
    }

    /// Adds an item to this collection.
    ///
    /// This method does a couple of things:
    ///
    /// 1. Updates this collection's extents to contain the item's spatial and temporal bounds
    /// 2. If the item has a href or a `self` link, adds a `item` link
    ///
    /// Note that collections are created, by default, with global bounds and no
    /// temporal extent, so you'll want to set those (e.g. with
    /// [Collection::new_from_item]) before adding other items.
    ///
    /// This function returns a reference to the `item`` link, if one was created.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, Collection};
    ///
    /// let item_0 = stac::read("examples/simple-item.json").unwrap();
    /// let item_1 = stac::read("examples/extended-item.json").unwrap();
    /// let mut collection = Collection::new_from_item("an-id", "a description", &item_0);
    /// collection.add_item(&item_1);
    /// ```
    pub fn add_item(&mut self, item: &Item) -> Option<&Link> {
        self.update_extents(item);
        self.maybe_add_item_link(item)
    }

    /// Creates a [Hasher](crate::hash::Hasher) from this collection's temporal extent.
    ///
    /// The `spatial_extent` and `temporal_extent` parameters control the
    /// resolution (minimum cell size) of the hasher. The time range is derived
    /// from the first interval in this collection's temporal extent.
    ///
    /// Returns `None` if the collection's first temporal interval does not have
    /// both a start and end datetime.
    ///
    /// # Examples
    ///
    /// ```
    /// use chrono::{TimeDelta, TimeZone, Utc};
    /// use stac::Collection;
    ///
    /// let mut collection = Collection::new("an-id", "a description");
    /// collection.extent.temporal.interval =
    ///     vec![[
    ///         Some(Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()),
    ///         Some(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()),
    ///     ]];
    ///
    /// let hasher = collection.hasher(1.0, TimeDelta::days(1)).unwrap();
    /// assert!(hasher.is_some());
    /// ```
    pub fn hasher(
        &self,
        spatial_extent: f64,
        temporal_extent: chrono::TimeDelta,
    ) -> std::result::Result<Option<crate::hash::Hasher>, crate::hash::Error> {
        let interval = self.extent.temporal.interval.first();
        let (start, end) = match interval {
            Some([Some(start), Some(end)]) => (*start, *end),
            _ => return Ok(None),
        };
        crate::hash::Hasher::new(spatial_extent, temporal_extent, start..end).map(Some)
    }
}

impl Provider {
    /// Creates a new provider with the given name.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Provider;
    /// let provider = Provider::new("a-name");
    /// assert_eq!(provider.name, "a-name");
    /// ```
    pub fn new(name: impl ToString) -> Provider {
        Provider {
            name: name.to_string(),
            description: None,
            roles: None,
            url: None,
            additional_fields: Map::new(),
        }
    }
}

impl Default for SpatialExtent {
    fn default() -> SpatialExtent {
        SpatialExtent {
            bbox: vec![Default::default()],
        }
    }
}

impl SpatialExtent {
    fn update(&mut self, other: Bbox) {
        if self.bbox.is_empty() {
            self.bbox.push(other);
        } else {
            self.bbox[0].update(other);
        }
    }
}

impl TemporalExtent {
    fn update(&mut self, start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) {
        if self.interval.is_empty() {
            self.interval.push([start, end]);
        } else {
            if let Some(start) = start
                && self.interval[0][0].map(|dt| dt > start).unwrap_or(true)
            {
                self.interval[0][0] = Some(start);
            }
            if let Some(end) = end
                && self.interval[0][1].map(|dt| dt < end).unwrap_or(true)
            {
                self.interval[0][1] = Some(end);
            }
        }
    }
}

impl Default for TemporalExtent {
    fn default() -> TemporalExtent {
        TemporalExtent {
            interval: vec![[None, None]],
        }
    }
}

impl Assets for Collection {
    fn assets(&self) -> &IndexMap<String, Asset> {
        &self.assets
    }
    fn assets_mut(&mut self) -> &mut IndexMap<String, Asset> {
        &mut self.assets
    }
}

impl TryFrom<Collection> for Map<String, Value> {
    type Error = Error;
    fn try_from(collection: Collection) -> Result<Self> {
        match serde_json::to_value(collection)? {
            Value::Object(object) => Ok(object),
            _ => {
                panic!("all STAC collections should serialize to a serde_json::Value::Object")
            }
        }
    }
}

impl TryFrom<Map<String, Value>> for Collection {
    type Error = serde_json::Error;
    fn try_from(map: Map<String, Value>) -> std::result::Result<Self, Self::Error> {
        serde_json::from_value(Value::Object(map))
    }
}

impl Migrate for Collection {}

#[cfg(test)]
mod tests {
    use super::{Collection, Extent, Provider};
    use serde_json::json;

    mod collection {
        use super::Collection;
        use crate::{Bbox, Extent, Links, STAC_VERSION};
        use chrono::{DateTime, Utc};

        #[test]
        fn new() {
            let collection = Collection::new("an-id", "a description");
            assert!(collection.title.is_none());
            assert_eq!(collection.description, "a description");
            assert_eq!(collection.license, "other");
            assert!(collection.providers.is_none());
            assert_eq!(collection.extent, Extent::default());
            assert!(collection.summaries.is_none());
            assert!(collection.assets.is_empty());
            assert_eq!(collection.version, STAC_VERSION);
            assert!(collection.extensions.is_empty());
            assert_eq!(collection.id, "an-id");
            assert!(collection.links.is_empty());
        }

        #[test]
        fn skip_serializing() {
            let collection = Collection::new("an-id", "a description");
            let value = serde_json::to_value(collection).unwrap();
            assert!(value.get("stac_extensions").is_none());
            assert!(value.get("title").is_none());
            assert!(value.get("keywords").is_none());
            assert!(value.get("providers").is_none());
            assert!(value.get("summaries").is_none());
            assert!(value.get("assets").is_none());
        }

        #[test]
        fn new_from_item() {
            let item = crate::read("examples/simple-item.json").unwrap();
            let collection = Collection::new_from_item("an-id", "a description", &item);
            assert_eq!(
                collection.extent.spatial.bbox[0],
                Bbox::TwoDimensional([
                    172.91173669923782,
                    1.3438851951615003,
                    172.95469614953714,
                    1.3690476620161975
                ])
            );
            assert_eq!(
                collection.extent.temporal.interval[0][0].unwrap(),
                "2020-12-11T22:38:32.125000Z"
                    .parse::<DateTime<Utc>>()
                    .unwrap()
            );
            assert_eq!(
                collection.extent.temporal.interval[0][1].unwrap(),
                "2020-12-11T22:38:32.125000Z"
                    .parse::<DateTime<Utc>>()
                    .unwrap()
            );
            let link = collection.link("item").unwrap();
            assert!(link.href.to_string().ends_with("simple-item.json"));
        }
    }

    mod provider {
        use super::Provider;

        #[test]
        fn new() {
            let provider = Provider::new("a-name");
            assert_eq!(provider.name, "a-name");
            assert!(provider.description.is_none());
            assert!(provider.roles.is_none());
            assert!(provider.url.is_none());
            assert!(provider.additional_fields.is_empty());
        }

        #[test]
        fn skip_serializing() {
            let provider = Provider::new("an-id");
            let value = serde_json::to_value(provider).unwrap();
            assert!(value.get("description").is_none());
            assert!(value.get("roles").is_none());
            assert!(value.get("url").is_none());
        }
    }

    mod extent {
        use super::Extent;
        use crate::Bbox;

        #[test]
        fn default() {
            let extent = Extent::default();
            assert_eq!(
                extent.spatial.bbox[0],
                Bbox::TwoDimensional([-180.0, -90.0, 180.0, 90.0])
            );
            assert_eq!(extent.temporal.interval, [[None, None]]);
            assert!(extent.additional_fields.is_empty());
        }
    }

    mod roundtrip {
        use super::Collection;
        use crate::tests::roundtrip;

        roundtrip!(collection, "examples/collection.json", Collection);
        roundtrip!(
            collection_with_schemas,
            "examples/collection-only/collection-with-schemas.json",
            Collection
        );
        roundtrip!(
            collection_only,
            "examples/collection-only/collection.json",
            Collection
        );
        roundtrip!(
            extensions_collection,
            "examples/extensions-collection/collection.json",
            Collection
        );
    }

    #[test]
    fn permissive_deserialization() {
        let _: Collection = serde_json::from_value(json!({})).unwrap();
    }

    #[test]
    fn has_type() {
        let value: serde_json::Value =
            serde_json::to_value(Collection::new("an-id", "a description")).unwrap();
        assert_eq!(value.as_object().unwrap()["type"], "Collection");
    }
}
