use crate::{Error, Result};
use geojson::{Geometry, Value};
use serde::{Deserialize, Serialize};

/// A bounding box.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Bbox {
    /// A two-dimensional bounding box.
    TwoDimensional([f64; 4]),

    /// A three-dimensional bounding box.
    ThreeDimensional([f64; 6]),
}

impl Bbox {
    /// Creates a new 2D bbox.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Bbox;
    /// let bbox = Bbox::new(1., 2., 3., 4.);
    /// ```
    pub fn new(xmin: f64, ymin: f64, xmax: f64, ymax: f64) -> Bbox {
        Bbox::TwoDimensional([xmin, ymin, xmax, ymax])
    }

    /// Returns true if the minimum bbox values are smaller than the maximum.
    ///
    /// This doesn't currently handle antimeridian-crossing bbox.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Bbox;
    /// let bbox = Bbox::default();
    /// assert!(bbox.is_valid());
    /// let bbox = Bbox::new(4., 3., 2., 1.);
    /// assert!(!bbox.is_valid());
    /// ```
    pub fn is_valid(&self) -> bool {
        // TODO handle antimeridian
        match self {
            Bbox::TwoDimensional([xmin, ymin, xmax, ymax]) => xmin <= xmax && ymin <= ymax,
            Bbox::ThreeDimensional([xmin, ymin, zmin, xmax, ymax, zmax]) => {
                xmin <= xmax && ymin <= ymax && zmin <= zmax
            }
        }
    }

    /// Updates this bbox with another bbox's values.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Bbox;
    /// let mut bbox = Bbox::new(1., 1., 2., 2.);
    /// bbox.update(Bbox::new(0., 0., 1.5, 1.5));
    /// assert_eq!(bbox, Bbox::new(0., 0., 2., 2.));
    /// ```
    pub fn update(&mut self, other: Bbox) {
        let new = match self {
            Bbox::TwoDimensional([xmin, ymin, xmax, ymax]) => match other {
                Bbox::TwoDimensional([oxmin, oymin, oxmax, oymax]) => {
                    *xmin = xmin.min(oxmin);
                    *ymin = ymin.min(oymin);
                    *xmax = xmax.max(oxmax);
                    *ymax = ymax.max(oymax);
                    None
                }
                Bbox::ThreeDimensional([oxmin, oymin, ozmin, oxmax, oymax, ozmax]) => {
                    Some(Bbox::ThreeDimensional([
                        xmin.min(oxmin),
                        ymin.min(oymin),
                        ozmin,
                        xmax.max(oxmax),
                        ymax.max(oymax),
                        ozmax,
                    ]))
                }
            },
            Bbox::ThreeDimensional([xmin, ymin, zmin, xmax, ymax, zmax]) => match other {
                Bbox::TwoDimensional([oxmin, oymin, oxmax, oymax]) => {
                    *xmin = xmin.min(oxmin);
                    *ymin = ymin.min(oymin);
                    *xmax = xmax.max(oxmax);
                    *ymax = ymax.max(oymax);
                    None
                }
                Bbox::ThreeDimensional([oxmin, oymin, ozmin, oxmax, oymax, ozmax]) => {
                    *xmin = xmin.min(oxmin);
                    *ymin = ymin.min(oymin);
                    *zmin = zmin.min(ozmin);
                    *xmax = xmax.max(oxmax);
                    *ymax = ymax.max(oymax);
                    *zmax = zmax.max(ozmax);
                    None
                }
            },
        };
        if let Some(new) = new {
            let _ = std::mem::replace(self, new);
        }
    }

    /// Returns this bbox's minimum x value.
    pub fn xmin(&self) -> f64 {
        match self {
            Bbox::TwoDimensional([v, _, _, _]) => *v,
            Bbox::ThreeDimensional([v, _, _, _, _, _]) => *v,
        }
    }

    /// Returns this bbox's minimum y value.
    pub fn ymin(&self) -> f64 {
        match self {
            Bbox::TwoDimensional([_, v, _, _]) => *v,
            Bbox::ThreeDimensional([_, v, _, _, _, _]) => *v,
        }
    }

    /// Returns this bbox's minimum z value.
    pub fn zmin(&self) -> Option<f64> {
        match self {
            Bbox::TwoDimensional(_) => None,
            Bbox::ThreeDimensional([_, _, v, _, _, _]) => Some(*v),
        }
    }

    /// Returns this bbox's maximum x value.
    pub fn xmax(&self) -> f64 {
        match self {
            Bbox::TwoDimensional([_, _, v, _]) => *v,
            Bbox::ThreeDimensional([_, _, _, v, _, _]) => *v,
        }
    }

    /// Returns this bbox's maximum y value.
    pub fn ymax(&self) -> f64 {
        match self {
            Bbox::TwoDimensional([_, _, _, v]) => *v,
            Bbox::ThreeDimensional([_, _, _, _, v, _]) => *v,
        }
    }

    /// Returns this bbox's maximum z value.
    pub fn zmax(&self) -> Option<f64> {
        match self {
            Bbox::TwoDimensional(_) => None,
            Bbox::ThreeDimensional([_, _, _, _, _, v]) => Some(*v),
        }
    }

    /// Converts this bbox to a [Geometry](geojson::Geometry).
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Bbox;
    /// let bbox = Bbox::new(1., 1., 2., 2.);
    /// let geometry = bbox.to_geometry();
    /// ```
    pub fn to_geometry(&self) -> Geometry {
        let bbox = Some((*self).into());
        let coordinates = match self {
            Bbox::TwoDimensional([xmin, ymin, xmax, ymax]) => vec![
                vec![*xmin, *ymin],
                vec![*xmax, *ymin],
                vec![*xmax, *ymax],
                vec![*xmin, *ymax],
                vec![*xmin, *ymin],
            ],
            Bbox::ThreeDimensional([xmin, ymin, zmin, xmax, ymax, _]) => vec![
                vec![*xmin, *ymin, *zmin],
                vec![*xmax, *ymin, *zmin],
                vec![*xmax, *ymax, *zmin],
                vec![*xmin, *ymax, *zmin],
                vec![*xmin, *ymin, *zmin],
            ],
        };
        Geometry {
            bbox,
            value: Value::Polygon(vec![coordinates]),
            foreign_members: None,
        }
    }
}

impl TryFrom<Vec<f64>> for Bbox {
    type Error = Error;

    fn try_from(bbox: Vec<f64>) -> Result<Bbox> {
        if bbox.len() == 4 {
            Ok(Bbox::TwoDimensional([bbox[0], bbox[1], bbox[2], bbox[3]]))
        } else if bbox.len() == 6 {
            Ok(Bbox::ThreeDimensional([
                bbox[0], bbox[1], bbox[2], bbox[3], bbox[4], bbox[5],
            ]))
        } else {
            Err(Error::InvalidBbox(bbox, "must have 4 or 6 values"))
        }
    }
}

impl From<Bbox> for Vec<f64> {
    fn from(bbox: Bbox) -> Vec<f64> {
        match bbox {
            Bbox::TwoDimensional(coordinates) => coordinates.to_vec(),
            Bbox::ThreeDimensional(coordinates) => coordinates.to_vec(),
        }
    }
}

impl Default for Bbox {
    fn default() -> Self {
        Bbox::TwoDimensional([-180., -90., 180., 90.])
    }
}

#[cfg(feature = "geo")]
impl From<geo::Rect> for Bbox {
    fn from(rect: geo::Rect) -> Bbox {
        Bbox::TwoDimensional([rect.min().x, rect.min().y, rect.max().x, rect.max().y])
    }
}

#[cfg(feature = "geo")]
impl From<Bbox> for geo::Rect {
    fn from(bbox: Bbox) -> geo::Rect {
        geo::Rect::new(
            geo::coord! {x: bbox.xmin(), y: bbox.ymin()},
            geo::coord! {x: bbox.xmax(), y: bbox.ymax()},
        )
    }
}

#[cfg(test)]
mod tests {
    use super::Bbox;
    use geojson::Value;

    #[test]
    fn to_geometry() {
        let bbox = Bbox::new(1., 2., 3., 4.);
        let geometry = bbox.to_geometry();
        assert_eq!(
            geometry.value,
            Value::Polygon(vec![vec![
                vec![1., 2.],
                vec![3., 2.],
                vec![3., 4.],
                vec![1., 4.],
                vec![1., 2.],
            ]])
        )
    }
}
