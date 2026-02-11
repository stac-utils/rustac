#[cfg(feature = "duckdb")]
mod duckdb;
mod memory;
#[cfg(feature = "pgstac")]
mod pgstac;

use crate::Error;
#[cfg(feature = "duckdb")]
pub use duckdb::DuckdbBackend;
pub use memory::MemoryBackend;
#[cfg(feature = "pgstac")]
pub use pgstac::PgstacBackend;
use stac::api::{CollectionSearchClient, SearchClient, TransactionClient};

/// Storage backend for a STAC API.
///
/// This trait combines [`SearchClient`], [`CollectionSearchClient`], and
/// [`TransactionClient`] with backend-specific capability flags.
pub trait Backend:
    SearchClient<Error = Error>
    + CollectionSearchClient<Error = Error>
    + TransactionClient<Error = Error>
    + Clone
    + Sync
    + Send
    + 'static
{
    /// Returns true if this backend has item search capabilities.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{MemoryBackend, Backend};
    ///
    /// assert!(MemoryBackend::new().has_item_search());
    /// ```
    fn has_item_search(&self) -> bool;

    /// Returns true if this backend has [filter](https://github.com/stac-api-extensions/filter) capabilities.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_server::{MemoryBackend, Backend};
    ///
    /// assert!(!MemoryBackend::new().has_filter());
    /// ```
    fn has_filter(&self) -> bool;
}
