use serde::{Deserialize, Serialize};

/// The core conformance uri.
pub const CORE_URI: &str = "https://api.stacspec.org/v1.0.0/core";

/// The features conformance uri.
pub const FEATURES_URI: &str = "https://api.stacspec.org/v1.0.0/ogcapi-features";

/// The collections conformance uri.
pub const COLLECTIONS_URI: &str = "https://api.stacspec.org/v1.0.0/collections";

/// The OGC API - Features - Part 1 Requirements Class Core uri
pub const OGC_API_FEATURES_URI: &str =
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core";

/// The GeoJSON spec conformance uri.
pub const GEOJSON_URI: &str = "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson";

/// The item search conformance uri.
pub const ITEM_SEARCH_URI: &str = "https://api.stacspec.org/v1.0.0/item-search";

/// The filter conformance uris.
pub const FILTER_URIS: [&str; 5] = [
    "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/filter",
    "http://www.opengis.net/spec/cql2/1.0/conf/basic-cql2",
    "https://api.stacspec.org/v1.0.0-rc.3/item-search#filter",
    "http://www.opengis.net/spec/cql2/1.0/conf/cql2-text",
    "http://www.opengis.net/spec/cql2/1.0/conf/cql2-json",
];

/// To support "generic" clients that want to access multiple OGC API Features
/// implementations - and not "just" a specific API / server, the server has to
/// declare the conformance classes it implements and conforms to.
#[derive(Debug, Serialize, Deserialize)]
pub struct Conformance {
    /// The conformance classes it implements and conforms to.
    #[serde(rename = "conformsTo")]
    pub conforms_to: Vec<String>,
}

impl Conformance {
    /// Creates a new conformance structure with only the core conformance class.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Conformance;
    /// let conformance = Conformance::new();
    /// ```
    pub fn new() -> Conformance {
        Conformance {
            conforms_to: vec![CORE_URI.to_string()],
        }
    }

    /// Adds
    /// [ogcapi-features](https://github.com/radiantearth/stac-api-spec/tree/release/v1.0.0/ogcapi-features)
    /// conformance classes.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Conformance;
    /// let conformance = Conformance::new().ogcapi_features();
    /// ```
    pub fn ogcapi_features(mut self) -> Conformance {
        self.conforms_to.push(FEATURES_URI.to_string());
        self.conforms_to.push(COLLECTIONS_URI.to_string());
        self.conforms_to.push(OGC_API_FEATURES_URI.to_string());
        self.conforms_to.push(GEOJSON_URI.to_string());
        self
    }

    /// Adds
    /// [item search](https://github.com/radiantearth/stac-api-spec/tree/release/v1.0.0/item-search)
    /// conformance class.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Conformance;
    /// let conformance = Conformance::new().item_search();
    /// ```
    pub fn item_search(mut self) -> Conformance {
        self.conforms_to.push(ITEM_SEARCH_URI.to_string());
        self
    }

    /// Adds [filter](https://github.com/stac-api-extensions/filter) conformance
    /// class.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Conformance;
    /// let conformance = Conformance::new().item_search();
    /// ```
    pub fn filter(mut self) -> Conformance {
        self.conforms_to
            .extend(FILTER_URIS.iter().map(|s| s.to_string()));
        self
    }
}

impl Default for Conformance {
    fn default() -> Self {
        Self::new()
    }
}
