use crate::{Band, DataType, Statistics};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use stac_derive::Fields;

/// An Asset is an object that contains a URI to data associated with the [Item](crate::Item) that can be downloaded or streamed.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Fields)]
pub struct Asset {
    /// URI to the asset object.
    ///
    /// Relative and absolute URIs are both allowed.
    pub href: String,

    /// The displayed title for clients and users.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    /// A description of the Asset providing additional details, such as how it was processed or created.
    ///
    /// [CommonMark 0.29](http://commonmark.org/) syntax MAY be used for rich text representation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// [Media type](crate::mime) of the asset.
    ///
    /// See the [common media types](https://github.com/radiantearth/stac-spec/blob/master/best-practices.md#common-media-types-in-stac) in the best practice doc for commonly used asset types.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,

    /// The semantic roles of the asset, similar to the use of rel in [Links](crate::Link).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub roles: Vec<String>,

    /// Creation date and time of the corresponding data, in UTC.
    ///
    /// This identifies the creation time of the data.
    ///
    /// This is a [common
    /// metadata](https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md)
    /// field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<String>,

    /// Date and time the metadata was updated last, in UTC.
    ///
    /// This identifies the updated time of the data.
    ///
    /// This is a [common
    /// metadata](https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md)
    /// field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated: Option<String>,

    /// An array of available bands where each object is a [Band].
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub bands: Vec<Band>,

    /// Value used to identify no-data.
    ///
    /// The extension specifies that this can be a number or a string, but we
    /// just use a f64 with a custom (de)serializer.
    ///
    /// TODO write custom (de)serializer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodata: Option<f64>,

    /// The data type of the values.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_type: Option<DataType>,

    /// Statistics of all the values.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statistics: Option<Statistics>,

    /// Unit of measurement of the value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,

    /// Additional fields on the asset.
    #[serde(flatten)]
    pub additional_fields: Map<String, Value>,
}

/// Trait implemented by anything that has assets.
///
/// As of STAC v1.0.0, this is [Collection](crate::Collection) and [Item](crate::Item).
pub trait Assets {
    /// Returns a reference to this object's assets.
    ///
    /// # Examples
    ///
    /// [Item](crate::Item) has assets:
    ///
    /// ```
    /// use stac::{Item, Assets};
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// assert!(!item.assets().is_empty());
    /// ```
    fn assets(&self) -> &IndexMap<String, Asset>;

    /// Returns a mut reference to this object's assets.
    ///
    /// # Examples
    ///
    /// [Item](crate::Item) has assets:
    ///
    /// ```
    /// use stac::{Item, Asset, Assets};
    /// let mut item: Item = stac::read("examples/simple-item.json").unwrap();
    /// item.assets_mut().insert("foo".to_string(), Asset::new("./asset.tif"));
    /// ```
    fn assets_mut(&mut self) -> &mut IndexMap<String, Asset>;
}

impl Asset {
    /// Creates a new asset with the provided href.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::Asset;
    /// let asset = Asset::new("an-href");
    /// assert_eq!(asset.href, "an-href");
    /// ```
    pub fn new(href: impl ToString) -> Asset {
        Asset {
            href: href.to_string(),
            title: None,
            description: None,
            r#type: None,
            roles: Vec::new(),
            created: None,
            updated: None,
            bands: Vec::new(),
            data_type: None,
            nodata: None,
            statistics: None,
            unit: None,
            additional_fields: Map::new(),
        }
    }

    /// Adds a role to this asset, returning the modified asset.
    ///
    /// Useful for builder patterns.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Asset;
    /// let asset = Asset::new("asset/dataset.tif").role("data");
    /// assert_eq!(asset.roles, vec!["data"]);
    /// ```
    pub fn role(mut self, role: impl ToString) -> Asset {
        self.roles.push(role.to_string());
        self.roles.dedup();
        self
    }
}

impl From<String> for Asset {
    fn from(value: String) -> Self {
        Asset::new(value)
    }
}

impl<'a> From<&'a str> for Asset {
    fn from(value: &'a str) -> Self {
        Asset::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::Asset;

    #[test]
    fn new() {
        let asset = Asset::new("an-href");
        assert_eq!(asset.href, "an-href");
        assert!(asset.title.is_none());
        assert!(asset.description.is_none());
        assert!(asset.r#type.is_none());
        assert!(asset.roles.is_empty());
    }

    #[test]
    fn skip_serializing() {
        let asset = Asset::new("an-href");
        let value = serde_json::to_value(asset).unwrap();
        assert!(value.get("title").is_none());
        assert!(value.get("description").is_none());
        assert!(value.get("type").is_none());
        assert!(value.get("roles").is_none());
    }
}
