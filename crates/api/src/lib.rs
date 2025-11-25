//! Rust implementation of the [STAC API](https://github.com/radiantearth/stac-api-spec) specification.
//!
//! This crate **is**:
//!
//! - Data structures
//!
//! This crate **is not**:
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
//! use stac_api::{Root, Conformance, CORE_URI};
//! let root = Root {
//!     catalog: Catalog::new("an-id", "a description"),
//!     conformance: Conformance {
//!         conforms_to: vec![CORE_URI.to_string()]
//!     },
//! };
//! ```

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
    unused_results,
    warnings
)]

mod collections;
mod conformance;
mod error;
mod fields;
mod filter;
mod item_collection;
mod items;
mod root;
mod search;
mod sort;
mod url_builder;

pub use collections::Collections;
pub use conformance::{
    COLLECTIONS_URI, CORE_URI, Conformance, FEATURES_URI, FILTER_URIS, GEOJSON_URI,
    ITEM_SEARCH_URI, OGC_API_FEATURES_URI,
};
pub use error::Error;
pub use fields::Fields;
pub use filter::Filter;
pub use item_collection::{Context, ItemCollection};
pub use items::{GetItems, Items};
pub use root::Root;
pub use search::{GetSearch, Search};
pub use sort::{Direction, Sortby};
pub use url_builder::UrlBuilder;

/// Crate-specific result type.
pub type Result<T> = std::result::Result<T, Error>;

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
/// println!("{}", stac_api::version());
/// ```
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
use geojson as _;

// From https://github.com/rust-lang/cargo/issues/383#issuecomment-720873790,
// may they be forever blessed.
#[cfg(doctest)]
mod readme {
    macro_rules! external_doc_test {
        ($x:expr) => {
            #[doc = $x]
            unsafe extern "C" {}
        };
    }

    external_doc_test!(include_str!("../README.md"));
}
