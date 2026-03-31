//! Extensions describe how STAC can use extensions that extend the
//! functionality of the core spec or add fields for specific domains.
//!
//! Extensions can be published anywhere, although the preferred location for
//! public extensions is in the GitHub
//! [stac-extensions](https://github.com/stac-extensions/) organization.
//! This crate currently supports only a few extensions, though we plan to add more as we find the time.
//! See <https://stac-extensions.github.io/> for the latest table of community extensions.
//! This table below lists all [stable](https://github.com/radiantearth/stac-spec/tree/master/extensions#extension-maturity) extensions, as well as any other extensions that are supported by **rustac**:
//!
//! | Extension | Maturity | **rustac** supported version |
//! | -- | -- | -- |
//! | [Authentication](https://github.com/stac-extensions/authentication) | Proposal | v1.1.0 |
//! | [Electro-Optical](https://github.com/stac-extensions/eo) | Stable | v1.1.0 |
//! | [File Info](https://github.com/stac-extensions/file) | Stable | v2.1.0 |
//! | [Landsat](https://github.com/stac-extensions/landsat) | Stable | n/a |
//! | [Projection](https://github.com/stac-extensions/projection) | Stable | v1.1.0 |
//! | [Raster](https://github.com/stac-extensions/raster) | Candidate | v1.1.0 |
//! | [Scientific Citation](https://github.com/stac-extensions/scientific) | Stable | n/a |
//! | [View Geometry](https://github.com/stac-extensions/view) | Stable | n/a |
//!
//! ## Usage
//!
//! [Item], [Collection], and [Catalog] all implement the [Extensions] trait,
//! which provides methods to get, set, and remove extension information:
//!
//! ```
//! use stac::Item;
//! use stac_extensions::{Extensions, Projection, projection::Centroid};
//! let mut item: Item = stac::read("examples/extensions-collection/proj-example/proj-example.json").unwrap();
//! assert!(item.has_extension::<Projection>());
//!
//! // Get extension information
//! let mut projection: Projection = item.extension().unwrap();
//! println!("code: {}", projection.code.as_ref().unwrap());
//!
//! // Set extension information
//! projection.centroid = Some(Centroid { lat: 34.595302, lon: -101.344483 });
//! Extensions::set_extension(&mut item, projection).unwrap();
//!
//! // Remove an extension
//! Extensions::remove_extension::<Projection>(&mut item);
//! assert!(!item.has_extension::<Projection>());
//! ```

pub mod authentication;
pub mod electro_optical;
pub mod file;
pub mod projection;
pub mod raster;

pub use projection::Projection;
pub use raster::Raster;
use serde::{Serialize, de::DeserializeOwned};
use stac::{Asset, Catalog, Collection, Fields, Item, Link, Result};

/// A trait implemented by extensions.
///
/// So far, all extensions are assumed to live in under
/// <https://stac-extensions.github.io> domain.
pub trait Extension: Serialize + DeserializeOwned {
    /// The schema URI.
    const IDENTIFIER: &'static str;

    /// The fiend name prefix.
    const PREFIX: &'static str;

    /// Returns everything from the identifier up until the version.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_extensions::{Raster, Extension};
    /// assert_eq!(Raster::identifier_prefix(), "https://stac-extensions.github.io/raster/");
    /// ```
    fn identifier_prefix() -> &'static str {
        assert!(Self::IDENTIFIER.starts_with("https://stac-extensions.github.io/"));
        let index = Self::IDENTIFIER["https://stac-extensions.github.io/".len()..]
            .find('/')
            .expect("all identifiers should have a first path segment");
        &Self::IDENTIFIER[0.."https://stac-extensions.github.io/".len() + index + 1]
    }
}

/// A trait for objects that may have STAC extensions.
pub trait Extensions: Fields {
    /// Returns a reference to this object's extensions.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_extensions::Extensions;
    ///
    /// let item = Item::new("an-id");
    /// assert!(item.extensions().is_empty());
    /// ```
    fn extensions(&self) -> &Vec<String>;

    /// Returns a mutable reference to this object's extensions.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_extensions::Extensions;
    ///
    /// let mut item = Item::new("an-id");
    /// item.extensions_mut().push("https://stac-extensions.github.io/raster/v1.1.0/schema.json".to_string());
    /// ```
    fn extensions_mut(&mut self) -> &mut Vec<String>;

    /// Returns true if this object has the given extension.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_extensions::{Projection, Extensions};
    ///
    /// let mut item = Item::new("an-id");
    /// assert!(!item.has_extension::<Projection>());
    /// let projection = Projection { code: Some("EPSG:4326".to_string()), ..Default::default() };
    /// item.set_extension(projection).unwrap();
    /// assert!(item.has_extension::<Projection>());
    /// ```
    fn has_extension<E: Extension>(&self) -> bool {
        self.extensions()
            .iter()
            .any(|extension| extension.starts_with(E::identifier_prefix()))
    }

    /// Returns an extension's data.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_extensions::{Projection, Extensions};
    ///
    /// let item: Item = stac::read("examples/extensions-collection/proj-example/proj-example.json").unwrap();
    /// let projection: Projection = item.extension().unwrap();
    /// assert_eq!(projection.code.unwrap(), "EPSG:32614");
    /// ```
    fn extension<E: Extension>(&self) -> Result<E> {
        self.fields_with_prefix(E::PREFIX)
    }

    /// Adds an extension's identifier to this object.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_extensions::{Projection, Extensions};
    ///
    /// let mut item = Item::new("an-id");
    /// item.add_extension::<Projection>();
    /// ```
    fn add_extension<E: Extension>(&mut self) {
        self.extensions_mut().push(E::IDENTIFIER.to_string());
        self.extensions_mut().dedup();
    }

    /// Sets an extension's data and adds its schema to this object's `extensions`.
    ///
    /// This will remove any previous versions of this extension.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_extensions::{Projection, Extensions};
    ///
    /// let mut item = Item::new("an-id");
    /// let projection = Projection { code: Some("EPSG:4326".to_string()), ..Default::default() };
    /// item.set_extension(projection).unwrap();
    /// ```
    fn set_extension<E: Extension>(&mut self, extension: E) -> Result<()> {
        self.extensions_mut().push(E::IDENTIFIER.to_string());
        self.extensions_mut().dedup();
        self.remove_fields_with_prefix(E::PREFIX);
        self.set_fields_with_prefix(E::PREFIX, extension)
    }

    /// Removes this extension and all of its fields from this object.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_extensions::{Projection, Extensions};
    ///
    /// let mut item: Item = stac::read("examples/extensions-collection/proj-example/proj-example.json").unwrap();
    /// assert!(item.has_extension::<Projection>());
    /// item.remove_extension::<Projection>();
    /// assert!(!item.has_extension::<Projection>());
    /// ```
    fn remove_extension<E: Extension>(&mut self) {
        self.remove_fields_with_prefix(E::PREFIX);
        self.extensions_mut()
            .retain(|extension| !extension.starts_with(E::identifier_prefix()))
    }
}

macro_rules! impl_extensions {
    ($name:ident) => {
        impl Extensions for $name {
            fn extensions(&self) -> &Vec<String> {
                &self.extensions
            }
            fn extensions_mut(&mut self) -> &mut Vec<String> {
                &mut self.extensions
            }
        }
    };
}

impl_extensions!(Item);
impl_extensions!(Catalog);
impl_extensions!(Collection);
impl_extensions!(Asset);
impl_extensions!(Link);

#[cfg(test)]
mod tests {
    use crate::{Extension, Extensions, Projection, raster::Raster};
    use serde_json::json;
    use stac::Item;

    #[test]
    fn identifer_prefix() {
        assert_eq!(
            Raster::identifier_prefix(),
            "https://stac-extensions.github.io/raster/"
        );
        assert_eq!(
            Projection::identifier_prefix(),
            "https://stac-extensions.github.io/projection/"
        );
    }

    #[test]
    fn remove_extension() {
        let mut item = Item::new("an-id");
        item.extensions
            .push("https://stac-extensions.github.io/projection/v2.0.0/schema.json".to_string());
        let _ = item
            .properties
            .additional_fields
            .insert("proj:code".to_string(), json!("EPSG:4326"));
        assert!(item.has_extension::<Projection>());
        item.remove_extension::<Projection>();
        assert!(!item.has_extension::<Projection>());
        assert!(item.extensions.is_empty());
        assert!(item.properties.additional_fields.is_empty());
    }
}
