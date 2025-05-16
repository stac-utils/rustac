//! Use [duckdb](https://duckdb.org/) with [STAC](https://stacspec.org).

#![warn(unused_crate_dependencies)]

mod client;
mod error;
mod extension;

pub use {client::Client, error::Error, extension::Extension};

use getrandom as _;

/// Searches a stac-geoparquet file.
///
/// # Examples
///
/// ```
/// let item_collection = stac_duckdb::search("data/100-sentinel-2-items.parquet", Default::default(), None).unwrap();
/// ```
pub fn search(
    href: &str,
    mut search: stac_api::Search,
    max_items: Option<usize>,
) -> Result<stac_api::ItemCollection> {
    if let Some(max_items) = max_items {
        search.limit = Some(max_items.try_into()?);
    } else {
        search.limit = None;
    };
    let client = Client::new()?;
    client.search(href, search)
}

/// A crate-specific result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Return this crate's version.
///
/// # Examples
///
/// ```
/// println!("{}", stac_duckdb::version());
/// ```
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
