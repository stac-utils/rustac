//! Geometry utilities, enabled by the `geo` feature.

use crate::{Error, Result};
use geo::{Rect, coord};

/// Creates a two-dimensional rectangle from four coordinates.
///
/// # Examples
///
/// ```
/// let bbox = stac::geo::bbox(&vec![-106.0, 41.0, -105.0, 42.0]).unwrap();
/// ```
pub fn bbox(coordinates: &[f64]) -> Result<Rect> {
    if coordinates.len() == 4 {
        Ok(Rect::new(
            coord! { x: coordinates[0], y: coordinates[1] },
            coord! { x: coordinates[2], y: coordinates[3] },
        ))
    } else {
        // TODO support three dimensional
        Err(Error::InvalidBbox(
            coordinates.to_vec(),
            "unsupported 3D bbox",
        ))
    }
}
