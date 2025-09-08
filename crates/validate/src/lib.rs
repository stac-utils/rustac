//! Validate STAC objects with [json-schema](https://json-schema.org/).
//!
//! # Examples
//!
//! Validation is provided via the [Validate] trait:
//!
//! ```
//! use stac::Item;
//! use stac_validate::Validate;
//!
//! #[tokio::main]
//! async fn main() {
//!     Item::new("an-id").validate().await.unwrap();
//! }
//! ```
//!
//! All fetched schemas are cached, so if you're you're doing multiple
//! validations, you should re-use the same [Validator]:
//!
//! ```
//! use stac::Item;
//! use stac_validate::Validator;
//!
//! #[tokio::main]
//! async fn main() {
//!     let mut items: Vec<_> = (0..10).map(|n| Item::new(format!("item-{}", n))).collect();
//!     let mut validator = Validator::new().await.unwrap();
//!     for item in items {
//!         validator.validate(&item).await.unwrap();
//!     }
//! }
//! ```
//!
//! [Validator] is cheap to clone, so you are encouraged to validate a large
//! number of objects at the same time if that's your use-case.

use serde::Serialize;

mod error;
mod validator;
use async_trait::async_trait;

pub use {error::Error, validator::Validator};

/// Public result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Validate any serializable object with [json-schema](https://json-schema.org/)
#[async_trait]
pub trait Validate: Serialize + Sized {
    /// Validates this object.
    ///
    /// If the object fails validation, this will return an [Error::Validation]
    /// which contains a vector of all of the validation errors.
    ///
    /// If you're doing multiple validations, use [Validator::validate], which
    /// will re-use cached schemas.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_validate::Validate;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut item = Item::new("an-id");
    ///     item.validate().await.unwrap();
    /// }
    /// ```
    async fn validate(&self) -> Result<()> {
        let mut validator = Validator::new().await?;
        validator.validate(self).await
    }
}

impl<T: Serialize + Send + Sync> Validate for T {}

/// Returns a string suitable for use as a HTTP user agent.
pub fn user_agent() -> &'static str {
    concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"))
}
