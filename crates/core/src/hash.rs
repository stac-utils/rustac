//! Sortable spatio-temporal hashing.
//!
//! Computes a sortable hash from a datetime, latitude, and longitude by
//! quantizing each dimension and interleaving their bits into a [Z-order
//! curve](https://en.wikipedia.org/wiki/Z-order_curve) (Morton code). Points
//! that are nearby in space and time tend to have numerically close hash values,
//! making the hashes useful for range queries and spatial indexing.
//!
//! Precision is controlled by providing spatial and temporal extents that define
//! the minimum cell size in each dimension. The number of bits per dimension is
//! derived from these extents automatically.
//!
//! # Example
//!
//! ```
//! use chrono::{TimeDelta, TimeZone, Utc};
//! use stac::hash::Hasher;
//!
//! let hasher = Hasher::new(
//!     1.0,                                           // 1 degree spatial cells
//!     TimeDelta::days(1),                            // 1 day temporal cells
//!     Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()
//!         ..Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
//! )
//! .unwrap();
//!
//! let hash = hasher.hash(
//!     Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap(),
//!     40.0,
//!     -105.0,
//! );
//! ```

use chrono::{DateTime, TimeDelta, Utc};
use std::ops::Range;

const MAX_BITS_PER_DIMENSION: u8 = 21;

/// A spatio-temporal hasher that produces sortable 64-bit hashes.
///
/// The hasher quantizes latitude, longitude, and time into discrete bins, then
/// interleaves their bits to form a Z-order curve index. The number of bits per
/// dimension is determined by the spatial and temporal extents provided at
/// construction time.
#[derive(Debug, Clone)]
pub struct Hasher {
    time_start_ms: i64,
    time_total_ms: f64,
    bits: u8,
}

impl Hasher {
    /// Creates a new hasher.
    ///
    /// # Arguments
    ///
    /// * `spatial_extent` — Minimum spatial cell size in degrees. Both latitude
    ///   and longitude are quantized at this resolution.
    /// * `temporal_extent` — Minimum temporal cell size.
    /// * `time_range` — The full time range that hashes must cover. Datetimes
    ///   outside this range are clamped to the boundaries.
    ///
    /// # Errors
    ///
    /// Returns an error if the spatial extent is not positive and finite, the
    /// temporal extent is not positive, the time range is empty, or the
    /// required bits per dimension exceeds the maximum (21).
    pub fn new(
        spatial_extent: f64,
        temporal_extent: TimeDelta,
        time_range: Range<DateTime<Utc>>,
    ) -> Result<Self, Error> {
        if spatial_extent <= 0.0 || !spatial_extent.is_finite() {
            return Err(Error::InvalidSpatialExtent);
        }
        if temporal_extent <= TimeDelta::zero() {
            return Err(Error::InvalidTemporalExtent);
        }
        if time_range.start >= time_range.end {
            return Err(Error::InvalidTimeRange);
        }

        let lat_bits = bits_needed(180.0 / spatial_extent);
        let lon_bits = bits_needed(360.0 / spatial_extent);

        let total_ms = (time_range.end - time_range.start).num_milliseconds() as f64;
        let extent_ms = temporal_extent.num_milliseconds() as f64;
        let time_bits = bits_needed(total_ms / extent_ms);

        let bits = lat_bits.max(lon_bits).max(time_bits);
        if bits > MAX_BITS_PER_DIMENSION {
            return Err(Error::TooManyBits { bits });
        }

        Ok(Self {
            time_start_ms: time_range.start.timestamp_millis(),
            time_total_ms: total_ms,
            bits,
        })
    }

    /// Creates a hasher by deriving the time range from a slice of items.
    ///
    /// Scans all items to find the earliest and latest datetimes, then uses
    /// that as the time range. Each item's datetime is determined by its
    /// `datetime` property, or by its `start_datetime` and `end_datetime`
    /// properties.
    ///
    /// Returns `None` if no items have datetimes. If all items share the same
    /// datetime, the time range is extended by `temporal_extent` to ensure a
    /// non-empty range.
    ///
    /// # Examples
    ///
    /// ```
    /// use chrono::TimeDelta;
    /// use stac::Item;
    /// use stac::hash::Hasher;
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let hasher = Hasher::from_items(&[item], 1.0, TimeDelta::days(1)).unwrap();
    /// assert!(hasher.is_some());
    /// ```
    pub fn from_items(
        items: &[crate::Item],
        spatial_extent: f64,
        temporal_extent: TimeDelta,
    ) -> Result<Option<Self>, Error> {
        let mut min: Option<DateTime<Utc>> = None;
        let mut max: Option<DateTime<Utc>> = None;
        for item in items {
            let dt = item.properties.datetime;
            let start = item.properties.start_datetime.or(dt);
            let end = item.properties.end_datetime.or(dt);
            if let Some(start) = start {
                min = Some(min.map_or(start, |m: DateTime<Utc>| m.min(start)));
            }
            if let Some(end) = end {
                max = Some(max.map_or(end, |m: DateTime<Utc>| m.max(end)));
            }
        }
        match (min, max) {
            (Some(start), Some(mut end)) => {
                if start == end {
                    end = end + temporal_extent;
                }
                Self::new(spatial_extent, temporal_extent, start..end).map(Some)
            }
            _ => Ok(None),
        }
    }

    /// Creates a hasher that uses the maximum precision for the given time range.
    ///
    /// The spatial and temporal extents are chosen so that each dimension uses
    /// the full 21 bits available, yielding 63-bit hashes with the finest
    /// possible resolution.
    ///
    /// # Examples
    ///
    /// ```
    /// use chrono::{TimeZone, Utc};
    /// use stac::hash::Hasher;
    ///
    /// let hasher = Hasher::from_time_range(
    ///     Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()
    ///         ..Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
    /// )
    /// .unwrap();
    /// assert_eq!(hasher.bits_per_dimension(), 21);
    /// ```
    pub fn from_time_range(time_range: Range<DateTime<Utc>>) -> Result<Self, Error> {
        if time_range.start >= time_range.end {
            return Err(Error::InvalidTimeRange);
        }
        let bins = (1u64 << MAX_BITS_PER_DIMENSION) as f64;
        let spatial_extent = 360.0 / bins;
        let total_ms = (time_range.end - time_range.start).num_milliseconds() as f64;
        let extent_ms = (total_ms / bins).ceil().max(1.0);
        let temporal_extent =
            TimeDelta::try_milliseconds(extent_ms as i64).unwrap_or(TimeDelta::milliseconds(1));
        Self::new(spatial_extent, temporal_extent, time_range)
    }

    /// Creates a maximum-precision hasher by deriving the time range from items.
    ///
    /// Combines the behavior of [`Hasher::from_items`] and
    /// [`Hasher::from_time_range`]: scans items to find the time range, then
    /// creates a hasher that uses the full 21 bits per dimension.
    ///
    /// Returns `None` if no items have datetimes.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac::hash::Hasher;
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// let hasher = Hasher::from_items_auto(&[item]).unwrap();
    /// assert!(hasher.is_some());
    /// ```
    pub fn from_items_auto(items: &[crate::Item]) -> Result<Option<Self>, Error> {
        let mut min: Option<DateTime<Utc>> = None;
        let mut max: Option<DateTime<Utc>> = None;
        for item in items {
            let dt = item.properties.datetime;
            let start = item.properties.start_datetime.or(dt);
            let end = item.properties.end_datetime.or(dt);
            if let Some(start) = start {
                min = Some(min.map_or(start, |m: DateTime<Utc>| m.min(start)));
            }
            if let Some(end) = end {
                max = Some(max.map_or(end, |m: DateTime<Utc>| m.max(end)));
            }
        }
        match (min, max) {
            (Some(start), Some(mut end)) => {
                if start == end {
                    end = end + TimeDelta::milliseconds(1);
                }
                Self::from_time_range(start..end).map(Some)
            }
            _ => Ok(None),
        }
    }

    /// Returns the number of bits used per dimension.
    pub fn bits_per_dimension(&self) -> u8 {
        self.bits
    }

    /// Returns the total number of bits in the hash.
    pub fn total_bits(&self) -> u8 {
        self.bits * 3
    }

    /// Computes a sortable hash for the given spatio-temporal point.
    ///
    /// Datetimes outside the configured time range are clamped. Latitudes are
    /// clamped to \[-90, 90\] and longitudes to \[-180, 180\].
    pub fn hash(&self, datetime: DateTime<Utc>, lat: f64, lon: f64) -> u64 {
        let lat_norm = ((lat + 90.0) / 180.0).clamp(0.0, 1.0);
        let lon_norm = ((lon + 180.0) / 360.0).clamp(0.0, 1.0);

        let time_offset_ms = (datetime.timestamp_millis() - self.time_start_ms).max(0) as f64;
        let time_norm = (time_offset_ms / self.time_total_ms).clamp(0.0, 1.0);

        let max_val = ((1u64 << self.bits) - 1) as f64;
        let time_q = (time_norm * max_val) as u64;
        let lat_q = (lat_norm * max_val) as u64;
        let lon_q = (lon_norm * max_val) as u64;

        interleave3(time_q, lat_q, lon_q, self.bits)
    }
}

/// Error enum for hash-related errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The required bits per dimension exceeds the maximum.
    #[error(
        "required bits per dimension ({bits}) exceeds maximum ({MAX_BITS_PER_DIMENSION}), \
         use coarser extents"
    )]
    TooManyBits {
        /// The number of bits that were required.
        bits: u8,
    },

    /// The spatial extent must be positive and finite.
    #[error("spatial extent must be positive")]
    InvalidSpatialExtent,

    /// The temporal extent must be positive.
    #[error("temporal extent must be positive")]
    InvalidTemporalExtent,

    /// The time range must be non-empty (start < end).
    #[error("time range must be non-empty")]
    InvalidTimeRange,
}

fn bits_needed(count: f64) -> u8 {
    if count <= 1.0 {
        return 0;
    }
    let bits = count.ceil().log2().ceil() as u8;
    bits.max(1)
}

fn interleave3(a: u64, b: u64, c: u64, bits: u8) -> u64 {
    let mut result = 0u64;
    for i in 0..bits {
        let src = i as u64;
        let dst = (i as u64) * 3;
        result |= ((c >> src) & 1) << dst;
        result |= ((b >> src) & 1) << (dst + 1);
        result |= ((a >> src) & 1) << (dst + 2);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn test_time_range() -> Range<DateTime<Utc>> {
        Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap()
            ..Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()
    }

    fn test_hasher() -> Hasher {
        Hasher::new(1.0, TimeDelta::days(1), test_time_range()).unwrap()
    }

    #[test]
    fn deterministic() {
        let hasher = test_hasher();
        let dt = Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap();
        let a = hasher.hash(dt, 40.0, -105.0);
        let b = hasher.hash(dt, 40.0, -105.0);
        assert_eq!(a, b);
    }

    #[test]
    fn nearby_points_have_close_hashes() {
        let hasher = test_hasher();
        let dt = Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap();

        let h1 = hasher.hash(dt, 40.0, -105.0);
        let h2 = hasher.hash(dt, 40.1, -105.1);
        let h_far = hasher.hash(dt, -40.0, 105.0);

        let diff_near = (h1 as i64 - h2 as i64).unsigned_abs();
        let diff_far = (h1 as i64 - h_far as i64).unsigned_abs();
        assert!(diff_near < diff_far);
    }

    #[test]
    fn origin_and_extremes() {
        let hasher = test_hasher();
        let dt_start = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let dt_end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

        let h_min = hasher.hash(dt_start, -90.0, -180.0);
        let h_max = hasher.hash(dt_end, 90.0, 180.0);

        assert_eq!(h_min, 0);
        assert!(h_max > 0);
    }

    #[test]
    fn clamping() {
        let hasher = test_hasher();
        let dt = Utc.with_ymd_and_hms(2023, 6, 15, 12, 0, 0).unwrap();
        let dt_before = Utc.with_ymd_and_hms(2019, 1, 1, 0, 0, 0).unwrap();
        let dt_after = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();

        let _ = hasher.hash(dt, -91.0, -181.0);
        let _ = hasher.hash(dt, 91.0, 181.0);
        let _ = hasher.hash(dt_before, 0.0, 0.0);
        let _ = hasher.hash(dt_after, 0.0, 0.0);
    }

    #[test]
    fn time_ordering() {
        let hasher = test_hasher();
        let dt1 = Utc.with_ymd_and_hms(2021, 1, 1, 0, 0, 0).unwrap();
        let dt2 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();

        let h1 = hasher.hash(dt1, 0.0, 0.0);
        let h2 = hasher.hash(dt2, 0.0, 0.0);
        assert!(h1 < h2);
    }

    #[test]
    fn spatial_ordering_lat() {
        let hasher = test_hasher();
        let dt = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        let h1 = hasher.hash(dt, -45.0, 0.0);
        let h2 = hasher.hash(dt, 45.0, 0.0);
        assert!(h1 < h2);
    }

    #[test]
    fn spatial_ordering_lon() {
        let hasher = test_hasher();
        let dt = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();

        let h1 = hasher.hash(dt, 0.0, -90.0);
        let h2 = hasher.hash(dt, 0.0, 90.0);
        assert!(h1 < h2);
    }

    #[test]
    fn invalid_spatial_extent() {
        assert!(Hasher::new(0.0, TimeDelta::days(1), test_time_range()).is_err());
        assert!(Hasher::new(-1.0, TimeDelta::days(1), test_time_range()).is_err());
        assert!(Hasher::new(f64::NAN, TimeDelta::days(1), test_time_range()).is_err());
        assert!(Hasher::new(f64::INFINITY, TimeDelta::days(1), test_time_range()).is_err());
    }

    #[test]
    fn invalid_temporal_extent() {
        assert!(Hasher::new(1.0, TimeDelta::zero(), test_time_range()).is_err());
        assert!(Hasher::new(1.0, TimeDelta::days(-1), test_time_range()).is_err());
    }

    #[test]
    fn invalid_time_range() {
        let dt = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        assert!(Hasher::new(1.0, TimeDelta::days(1), dt..dt).is_err());
    }

    #[test]
    fn fine_precision() {
        let hasher = Hasher::new(0.01, TimeDelta::hours(1), test_time_range()).unwrap();
        assert!(hasher.bits_per_dimension() > 10);
    }

    #[test]
    fn coarse_precision() {
        let hasher = Hasher::new(90.0, TimeDelta::days(365), test_time_range()).unwrap();
        assert!(hasher.bits_per_dimension() <= 5);
    }

    #[test]
    fn bits_needed_cases() {
        assert_eq!(bits_needed(0.5), 0);
        assert_eq!(bits_needed(1.0), 0);
        assert_eq!(bits_needed(2.0), 1);
        assert_eq!(bits_needed(3.0), 2);
        assert_eq!(bits_needed(4.0), 2);
        assert_eq!(bits_needed(5.0), 3);
        assert_eq!(bits_needed(256.0), 8);
        assert_eq!(bits_needed(257.0), 9);
    }

    #[test]
    fn interleave_simple() {
        assert_eq!(interleave3(1, 0, 1, 1), 0b101);
        assert_eq!(interleave3(1, 1, 1, 1), 0b111);
    }

    #[test]
    fn too_many_bits() {
        let range = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap()
            ..Utc.with_ymd_and_hms(2100, 1, 1, 0, 0, 0).unwrap();
        let result = Hasher::new(0.0001, TimeDelta::seconds(1), range);
        assert!(result.is_err());
    }
}
