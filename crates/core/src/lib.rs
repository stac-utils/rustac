//! Rust implementation of the [SpatioTemporal Asset Catalog (STAC)](https://stacspec.org/) specification.
//!
//! The SpatioTemporal Asset Catalog (STAC) specification provides a common language to describe a range of geospatial information, so it can more easily be indexed and discovered.
//! A 'spatiotemporal asset' is any file that represents information about the earth captured in a certain space and time.
//!
//! This is a Rust implementation of the specification.
//! Similar projects in other languages include:
//!
//! - Python: [PySTAC](https://pystac.readthedocs.io/en/1.0/)
//! - Go: [go-stac](https://github.com/planetlabs/go-stac)
//! - .NET: [DotNetStac](https://github.com/Terradue/DotNetStac)
//! - PHP: [resto](https://github.com/jjrom/resto)
//!
//! # Data structures
//!
//! STAC has three core data structures:
//!
//! - [Item] is a [GeoJSON](http://geojson.org/) [Feature](https://tools.ietf.org/html/rfc7946#section-3.2) augmented with [foreign members](https://tools.ietf.org/html/rfc7946#section-6)
//! - [Catalog] represents a logical group of other [Catalogs](Catalog), [Collections](Collection), and [Items](Item)
//! - [Collection] shares all fields with the `Catalog` (with different allowed values for `type` and `stac_extensions`) and adds fields to describe the whole dataset and the included set of `Items`
//!
//! All three are provided as [serde](https://serde.rs/) (de)serializable structures with public attributes.
//! Each structure provides a `new` method that fills most of the object's attributes with sensible defaults:
//!
//! ```
//! use stac::{Item, Catalog, Collection};
//! let item = Item::new("id");
//! let catalog = Catalog::new("id", "description");
//! let collection = Catalog::new("id", "description");
//! ```
//!
//! All attributes of STAC objects are accessible as public members:
//!
//! ```
//! use stac::{Item, Link};
//! let mut item = Item::new("id");
//! assert_eq!(item.id, "id");
//! assert!(item.geometry.is_none());
//! assert!(item.links.is_empty());
//! item.links.push(Link::new("an/href", "a-rel-type"));
//! ```
//!
//! # [Value]
//!
//! A [Value] can represent any of the three core data structures or an [ItemCollection].
//! It's the [serde_json::Value] for **stac-rs**:
//!
//! ```
//! use stac::{Value, Item};
//!
//! let value = Value::Item(Item::new("an-id"));
//! ```
//!
//! [Value] implements most traits that are shared between the data structures, so users of this library can do work (e.g. [migration](Migrate)) without needing to know what type of object the value represents:
//!
//! ```
//! use stac::{Value, Migrate, Version};
//!
//! let value: Value = stac::read("examples/simple-item.json").unwrap();
//! let value = value.migrate(&Version::v1_1_0).unwrap();
//! ```
//!
//! # Input and output
//!
//! Synchronous reads from the filesystem are supported via [read]:
//!
//! ```
//! let value: stac::Item = stac::read("examples/simple-item.json").unwrap();
//! ```
//!
//! If the [reqwest](https://docs.rs/reqwest/latest/reqwest/) feature is enabled, synchronous reads from urls are also supported:
//!
//! ```
//! #[cfg(feature = "reqwest")]
//! {
//!     let url = "https://raw.githubusercontent.com/radiantearth/stac-spec/master/examples/simple-item.json";
//!     let item: stac::Item = stac::read(url).unwrap();
//! }
//! ```
//!
//! To write, use [write()]:
//!
//! ```no_run
//! use stac::Format;
//!
//! stac::write("an-id.json", stac::Item::new("an-id")).unwrap();
//! ```
//!
//! Enable the `object-store` feature to get and put objects from cloud storage, e.g. s3 (with the `object-store-aws` feature) or from other backends (see [features](#features) for a complete listing):
//!
//! ```no_run
//! use stac::Item;
//!
//! #[cfg(feature = "object-store")]
//! {
//!     # tokio_test::block_on(async {
//!     stac::io::put_opts("s3://bucket/item.json", Item::new("an-id"), [("foo", "bar")]).await.unwrap();
//!     let item: Item = stac::io::get_opts("s3://bucket/item.json", [("foo", "bar")]).await.unwrap();
//!     # })
//! }
//! ```
//!
//! For more, see the documentation in the [io] module.
//!
//! # Features
//!
//! - `geo`: add some geo-enabled methods, see [geo]
//! - `geoarrow`: read and write [geoarrow](https://geoarrow.org/), see [geoarrow]
//! - `geoparquet`: read and write [geoparquet](https://geoparquet.org/), see [geoparquet]
//!     - `geoparquet-compression`: enable parquet compression
//! - `object-store`: get and put from object stores. Sub-features enable specific protocols:
//!     - `object-store-aws`
//!     - `object-store-azure`
//!     - `object-store-gcp`
//!     - `object-store-http`
//!     - `object-store-all` (enable them all)
//! - `reqwest`: get from `http` and `https` urls when using [read]
//!     - `reqwest-rustls`: enable the `rustls-tls` feature in reqwest

#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![deny(
    elided_lifetimes_in_paths,
    explicit_outlives_requirements,
    keyword_idents,
    macro_use_extern_crate,
    meta_variable_misuse,
    missing_abi,
    missing_debug_implementations,
    missing_docs,
    non_ascii_idents,
    noop_method_call,
    rust_2021_incompatible_closure_captures,
    rust_2021_incompatible_or_patterns,
    rust_2021_prefixes_incompatible_syntax,
    rust_2021_prelude_collisions,
    single_use_lifetimes,
    trivial_casts,
    trivial_numeric_casts,
    unreachable_pub,
    unsafe_code,
    unsafe_op_in_unsafe_fn,
    unused_crate_dependencies,
    unused_extern_crates,
    unused_import_braces,
    unused_lifetimes,
    unused_qualifications,
    unused_results
)]

mod asset;
mod band;
mod bbox;
mod catalog;
mod collection;
mod data_type;
pub mod datetime;
mod error;
mod format;
#[cfg(feature = "geo")]
pub mod geo;
#[cfg(feature = "geoarrow")]
pub mod geoarrow;
pub mod geoparquet;
pub mod io;
pub mod item;
mod item_asset;
mod item_collection;
mod json;
mod ndjson;
mod node;
#[cfg(feature = "object-store")]
mod resolver;
mod statistics;
#[cfg(feature = "validate")]
mod validate;
mod value;

use std::fmt::Display;

#[cfg(feature = "object-store")]
pub use resolver::Resolver;
pub use stac_types::{mime, Fields, Href, Link, Links, Migrate, SelfHref, Version, STAC_VERSION};
#[cfg(feature = "validate-blocking")]
pub use validate::ValidateBlocking;
#[cfg(feature = "validate")]
pub use validate::{Validate, Validator};
pub use {
    asset::{Asset, Assets},
    band::Band,
    bbox::Bbox,
    catalog::Catalog,
    collection::{Collection, Extent, Provider, SpatialExtent, TemporalExtent},
    data_type::DataType,
    error::Error,
    format::Format,
    geoparquet::{FromGeoparquet, IntoGeoparquet},
    io::{read, write},
    item::{FlatItem, Item, Properties},
    item_asset::ItemAsset,
    item_collection::ItemCollection,
    json::{FromJson, ToJson},
    ndjson::{FromNdjson, ToNdjson},
    node::{Container, Node},
    statistics::Statistics,
    value::Value,
};

/// Custom [Result](std::result::Result) type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Enum for the four "types" of STAC values.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Type {
    /// An item.
    Item,

    /// A collection.
    Collection,

    /// A catalog.
    Catalog,

    /// An item collection.
    ///
    /// While not technically part of the STAC specification, it's used all over the place.
    ItemCollection,
}

impl Type {
    /// Returns this type as a str.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Type;
    ///
    /// assert_eq!(Type::Item.as_str(), "Feature");
    /// ```
    pub fn as_str(&self) -> &'static str {
        match self {
            Type::Item => "Feature",
            Type::Catalog => "Catalog",
            Type::Collection => "Collection",
            Type::ItemCollection => "FeatureCollection",
        }
    }

    /// Returns the schema path for this type.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Type, Version};
    ///
    /// assert_eq!(Type::Item.spec_path(&Version::v1_0_0).unwrap(), "/v1.0.0/item-spec/json-schema/item.json");
    /// ```
    pub fn spec_path(&self, version: &Version) -> Option<String> {
        match self {
            Type::Item => Some(format!("/v{}/item-spec/json-schema/item.json", version)),
            Type::Catalog => Some(format!(
                "/v{}/catalog-spec/json-schema/catalog.json",
                version
            )),
            Type::Collection => Some(format!(
                "/v{}/collection-spec/json-schema/collection.json",
                version
            )),
            Type::ItemCollection => None,
        }
    }
}

impl std::str::FromStr for Type {
    type Err = Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "Feature" => Ok(Type::Item),
            "Catalog" => Ok(Type::Catalog),
            "Collection" => Ok(Type::Collection),
            "FeatureCollection" => Ok(Type::ItemCollection),
            _ => Err(Error::UnknownType(s.to_string())),
        }
    }
}

impl<T> PartialEq<T> for Type
where
    T: AsRef<str>,
{
    fn eq(&self, other: &T) -> bool {
        self.as_str() == other.as_ref()
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Type::Item => "Item",
                Type::Catalog => "Catalog",
                Type::Collection => "Collection",
                Type::ItemCollection => "ItemCollection",
            }
        )
    }
}

/// Return this crate's version.
///
/// # Examples
///
/// ```
/// println!("{}", stac::version());
/// ```
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use rstest as _;
    use tokio as _;
    use tokio_test as _;

    macro_rules! roundtrip {
        ($function:ident, $filename:expr, $object:ident) => {
            #[test]
            fn $function() {
                use assert_json_diff::{assert_json_matches, CompareMode, Config, NumericMode};
                use chrono::{DateTime, Utc};
                use serde_json::Value;
                use std::{fs::File, io::BufReader};

                let file = File::open($filename).unwrap();
                let buf_reader = BufReader::new(file);
                let mut before: Value = serde_json::from_reader(buf_reader).unwrap();
                if let Some(object) = before.as_object_mut() {
                    if object
                        .get("stac_extensions")
                        .and_then(|value| value.as_array())
                        .map(|array| array.is_empty())
                        .unwrap_or_default()
                    {
                        let _ = object.remove("stac_extensions");
                    }
                    if let Some(properties) =
                        object.get_mut("properties").and_then(|v| v.as_object_mut())
                    {
                        if let Some(datetime) = properties.get("datetime") {
                            if !datetime.is_null() {
                                let datetime: DateTime<Utc> =
                                    serde_json::from_value(datetime.clone()).unwrap();
                                let _ = properties.insert(
                                    "datetime".to_string(),
                                    serde_json::to_value(datetime).unwrap(),
                                );
                            }
                        }
                    }
                    if let Some(intervals) = object
                        .get_mut("extent")
                        .and_then(|v| v.as_object_mut())
                        .and_then(|o| o.get_mut("temporal"))
                        .and_then(|v| v.as_object_mut())
                        .and_then(|o| o.get_mut("interval"))
                        .and_then(|v| v.as_array_mut())
                    {
                        for interval in intervals {
                            if let Some(interval) = interval.as_array_mut() {
                                for datetime in interval {
                                    if !datetime.is_null() {
                                        let dt: DateTime<Utc> =
                                            serde_json::from_value(datetime.clone()).unwrap();
                                        *datetime = serde_json::to_value(dt).unwrap();
                                    }
                                }
                            }
                        }
                    }
                }
                let object: $object = serde_json::from_value(before.clone()).unwrap();
                let after = serde_json::to_value(object).unwrap();
                assert_json_matches!(
                    before,
                    after,
                    Config::new(CompareMode::Strict).numeric_mode(NumericMode::AssumeFloat)
                );
            }
        };
    }
    pub(crate) use roundtrip;
}

// From https://github.com/rust-lang/cargo/issues/383#issuecomment-720873790,
// may they be forever blessed.
#[cfg(doctest)]
mod readme {
    macro_rules! external_doc_test {
        ($x:expr) => {
            #[doc = $x]
            extern "C" {}
        };
    }

    external_doc_test!(include_str!("../README.md"));
}

// For now, we only use tracing in the validate module.
#[cfg(not(feature = "validate"))]
use tracing as _;
