use crate::Version;
use thiserror::Error;

/// Error enum for crate-specific errors.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// [chrono::ParseError]
    #[error(transparent)]
    ChronoParse(#[from] chrono::ParseError),

    /// A required feature is not enabled.
    #[error("{0} is not enabled")]
    FeatureNotEnabled(&'static str),

    /// [geojson::Error]
    #[error(transparent)]
    Geojson(#[from] Box<geojson::Error>),

    /// Returned when a STAC object has the wrong type field.
    #[error("incorrect type: expected={expected}, actual={actual}")]
    IncorrectType {
        /// The actual type field on the object.
        actual: String,

        /// The expected value.
        expected: String,
    },

    /// Returned when a property name conflicts with a top-level STAC field, or
    /// it's an invalid top-level field name.
    #[error("invalid attribute name: {0}")]
    InvalidAttribute(String),

    /// This vector is not a valid bounding box.
    #[error("invalid bbox: {0:?}")]
    InvalidBbox(Vec<f64>),

    /// This string is not a valid datetime interval.
    #[error("invalid datetime: {0}")]
    InvalidDatetime(String),

    /// [std::io::Error]
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// Returned when there is not a required field on a STAC object
    #[error("no \"{0}\" field in the JSON object")]
    MissingField(&'static str),

    /// There is not an href, when an href is required.
    #[error("no href")]
    NoHref,

    /// There are no items, when items are required.
    #[error("no items")]
    NoItems,

    /// This is not a JSON object.
    #[error("json value is not an object")]
    NotAnObject(serde_json::Value),

    /// [serde_json::Error]
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    /// [std::num::TryFromIntError]
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),

    /// Returned when the `type` field of a STAC object does not equal `"Feature"`, `"Catalog"`, or `"Collection"`.
    #[error("unknown \"type\": {0}")]
    UnknownType(String),

    /// Unsupported geoparquet type
    #[error("unsupported geoparquet type")]
    UnsupportedGeoparquetType,

    /// Unsupported migration.
    #[error("unsupported migration: {0} to {1}")]
    UnsupportedMigration(Version, Version),

    /// [url::ParseError]
    #[error(transparent)]
    UrlParse(#[from] url::ParseError),

    /// [arrow_schema::ArrowError]
    #[error(transparent)]
    #[cfg(feature = "geoarrow")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// The schema of two sets of items don't match.
    #[cfg(feature = "geoarrow")]
    #[error("Arrow schema mismatch")]
    ArrowSchemaMismatch,

    /// The arrow table is empty
    #[cfg(feature = "geoarrow")]
    #[error("Empty arrow table")]
    EmptyArrowTable,

    /// [geoarrow_schema::error::GeoArrowError]
    #[error(transparent)]
    #[cfg(feature = "geoarrow")]
    GeoArrow(#[from] geoarrow_schema::error::GeoArrowError),

    /// [wkb::error::WkbError]
    #[error(transparent)]
    #[cfg(feature = "geoarrow")]
    Wkb(#[from] wkb::error::WkbError),

    /// The geoparquet writer has been closed.
    #[error("The geoparquet writer has already been closed")]
    #[cfg(feature = "geoparquet")]
    ClosedGeoparquetWriter,

    /// No geoparquet metadata in a stac-geoparquet file.
    #[error("no geoparquet metadata")]
    #[cfg(feature = "geoparquet")]
    MissingGeoparquetMetadata,

    /// [parquet::errors::ParquetError]
    #[error(transparent)]
    #[cfg(feature = "geoparquet")]
    Parquet(#[from] parquet::errors::ParquetError),
}
