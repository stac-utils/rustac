//! Validate STAC objects with [json-schema](https://json-schema.org/).
//!
//! # Examples
//!
//! Validation is provided via the [Validate] trait:
//!
//! ```
//! use stac::Item;
//! use stac_io::Validate;
//!
//! Item::new("an-id").validate().unwrap();
//! ```
//!
//! All fetched schemas are cached, so if you're you're doing multiple
//! validations, you should re-use the same [Validator]:
//!
//! ```
//! # use stac::Item;
//! use stac_io::Validator;
//! let mut items: Vec<_> = (0..10).map(|n| Item::new(format!("item-{}", n))).collect();
//! let mut validator = Validator::new().unwrap();
//! for item in items {
//!     validator.validate(&item).unwrap();
//! }
//! ```
//!
//! [Validator] is cheap to clone, so you are encouraged to validate a large
//! number of objects at the same time if that's your use-case.

use crate::Result;
use serde::Serialize;

mod validator;

pub use validator::Validator;

/// Validate any serializable object with [json-schema](https://json-schema.org/)
pub trait Validate: Serialize + Sized {
    /// Validates this object.
    ///
    /// If the object fails validation, this will return an
    /// [Error::Validation](crate::Error::Validation) which contains a vector of
    /// all of the validation errors.
    ///
    /// If you're doing multiple validations, use [Validator::validate], which
    /// will re-use cached schemas.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_io::Validate;
    ///
    /// let mut item = Item::new("an-id");
    /// item.validate().unwrap();
    /// ```
    fn validate(&self) -> Result<()> {
        let mut validator = Validator::new()?;
        validator.validate(self)
    }
}

impl<T: Serialize> Validate for T {}
