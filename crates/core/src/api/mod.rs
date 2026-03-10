//! Rust implementation of the [STAC API](https://github.com/radiantearth/stac-api-spec) specification.
//!
//! This module **is**:
//!
//! - Data structures
//!
//! This module **is not**:
//!
//! - A server implementation
//!
//! For a STAC API server written in Rust based on this crate, see our
//! [stac-server](https://github.com/stac-utils/rustac/tree/main/stac-server).
//!
//! # Data structures
//!
//! Each API endpoint has its own data structure. In some cases, these are
//! light wrappers around [stac] data structures. In other cases, they can be
//! different -- e.g. the `/search` endpoint may not return [Items](stac::Item)
//! if the [fields](https://github.com/stac-api-extensions/fields) extension is
//! used, so the return type is a crate-specific [Item] struct.
//!
//! For example, here's the root structure (a.k.a the landing page):
//!
//! ```
//! use stac::Catalog;
//! use stac::api::{Root, Conformance, CORE_URI};
//! let root = Root {
//!     catalog: Catalog::new("an-id", "a description"),
//!     conformance: Conformance {
//!         conforms_to: vec![CORE_URI.to_string()]
//!     },
//! };
//! ```

#![warn(missing_docs, unused_qualifications)]

mod client;
mod collections;
mod conformance;
mod fields;
mod filter;
mod item_collection;
mod items;
mod root;
mod search;
mod sort;
mod url_builder;

#[cfg(feature = "geoarrow")]
pub use client::ArrowSearchClient;
pub use client::{CollectionSearchClient, SearchClient, TransactionClient};
pub use collections::Collections;
pub use conformance::{
    COLLECTIONS_URI, CORE_URI, Conformance, FEATURES_URI, FILTER_URIS, GEOJSON_URI,
    ITEM_SEARCH_URI, OGC_API_FEATURES_URI,
};
pub use fields::Fields;
pub use filter::Filter;
pub use item_collection::{Context, ItemCollection};
pub use items::{GetItems, Items};
pub use root::Root;
pub use search::{GetSearch, Search};
pub use sort::{Direction, Sortby};
pub use url_builder::UrlBuilder;

/// Crate-specific result type.
pub type Result<T> = std::result::Result<T, crate::Error>;

/// A STAC API Item type definition.
///
/// By default, STAC API endpoints that return [stac::Item] objects return every
/// field of those Items. However, Item objects can have hundreds of fields, or
/// large geometries, and even smaller Item objects can add up when large
/// numbers of them are in results. Frequently, not all fields in an Item are
/// used, so this specification provides a mechanism for clients to request that
/// servers to explicitly include or exclude certain fields.
pub type Item = serde_json::Map<String, serde_json::Value>;

/// Return this crate's version.
///
/// # Examples
///
/// ```
/// println!("{}", stac::api::version());
/// ```
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
use geojson as _;
