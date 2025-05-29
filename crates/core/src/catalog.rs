use crate::{Error, Link, Result, STAC_VERSION, Version};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use stac_derive::{Fields, Links, Migrate, SelfHref};

const CATALOG_TYPE: &str = "Catalog";

fn catalog_type() -> String {
    CATALOG_TYPE.to_string()
}

fn deserialize_catalog_type<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let r#type = String::deserialize(deserializer)?;
    if r#type != CATALOG_TYPE {
        Err(serde::de::Error::invalid_value(
            serde::de::Unexpected::Str(&r#type),
            &CATALOG_TYPE,
        ))
    } else {
        Ok(r#type)
    }
}

/// A STAC Catalog object represents a logical group of other `Catalog`,
/// [Collection](crate::Collection), and [Item](crate::Item) objects.
///
/// These `Item`s can be linked to directly from a `Catalog`, or the `Catalog`
/// can link to other Catalogs (often called sub-catalogs) that contain links to
/// `Collection`s and `Item`s. The division of sub-catalogs is up to the
/// implementor, but is generally done to aid the ease of online browsing by
/// people.
///
/// A `Catalog` object will typically be the entry point into a STAC catalog.
/// Their purpose is discovery: to be browsed by people or be crawled by clients
/// to build a searchable index.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, SelfHref, Migrate, Links, Fields)]
pub struct Catalog {
    #[serde(
        default = "catalog_type",
        deserialize_with = "deserialize_catalog_type"
    )]
    r#type: String,

    /// The STAC version the `Catalog` implements.
    #[serde(rename = "stac_version", default)]
    pub version: Version,

    /// A list of extension identifiers the `Catalog` implements.
    #[serde(rename = "stac_extensions")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub extensions: Vec<String>,

    /// Identifier for the `Catalog`.
    #[serde(default)]
    pub id: String,

    /// A short descriptive one-line title for the `Catalog`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    /// Detailed multi-line description to fully explain the `Catalog`.
    ///
    /// [CommonMark 0.29](http://commonmark.org/) syntax MAY be used for rich text representation.
    #[serde(default)]
    pub description: String,

    /// A list of references to other documents.
    #[serde(default)]
    pub links: Vec<Link>,

    /// Additional fields not part of the Catalog specification.
    #[serde(flatten)]
    pub additional_fields: Map<String, Value>,

    #[serde(skip)]
    self_href: Option<String>,
}

impl Catalog {
    /// Creates a new `Catalog` with the given `id`.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Catalog;
    /// let catalog = Catalog::new("an-id", "a description");
    /// assert_eq!(catalog.id, "an-id");
    /// assert_eq!(catalog.description, "a description");
    /// ```
    pub fn new(id: impl ToString, description: impl ToString) -> Catalog {
        Catalog {
            r#type: catalog_type(),
            version: STAC_VERSION,
            extensions: Vec::new(),
            id: id.to_string(),
            title: None,
            description: description.to_string(),
            links: Vec::new(),
            additional_fields: Map::new(),
            self_href: None,
        }
    }
}

impl TryFrom<Catalog> for Map<String, Value> {
    type Error = Error;
    fn try_from(catalog: Catalog) -> Result<Self> {
        match serde_json::to_value(catalog)? {
            Value::Object(object) => Ok(object),
            _ => {
                panic!("all STAC catalogs should serialize to a serde_json::Value::Object")
            }
        }
    }
}

impl TryFrom<Map<String, Value>> for Catalog {
    type Error = serde_json::Error;
    fn try_from(map: Map<String, Value>) -> std::result::Result<Self, Self::Error> {
        serde_json::from_value(Value::Object(map))
    }
}

#[cfg(test)]
mod tests {
    use super::Catalog;
    use crate::STAC_VERSION;
    use serde_json::json;

    #[test]
    fn new() {
        let catalog = Catalog::new("an-id", "a description");
        assert!(catalog.title.is_none());
        assert_eq!(catalog.description, "a description");
        assert_eq!(catalog.version, STAC_VERSION);
        assert!(catalog.extensions.is_empty());
        assert_eq!(catalog.id, "an-id");
        assert!(catalog.links.is_empty());
    }

    #[test]
    fn skip_serializing() {
        let catalog = Catalog::new("an-id", "a description");
        let value = serde_json::to_value(catalog).unwrap();
        assert!(value.get("stac_extensions").is_none());
        assert!(value.get("title").is_none());
    }

    mod roundtrip {
        use super::Catalog;
        use crate::tests::roundtrip;

        roundtrip!(catalog, "examples/catalog.json", Catalog);
    }

    #[test]
    fn permissive_deserialization() {
        let _: Catalog = serde_json::from_value(json!({})).unwrap();
    }

    #[test]
    fn has_type() {
        let value: serde_json::Value =
            serde_json::to_value(Catalog::new("an-id", "a description")).unwrap();
        assert_eq!(value.as_object().unwrap()["type"], "Catalog");
    }
}
