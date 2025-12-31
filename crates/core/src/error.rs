use crate::Version;
use thiserror::Error;

/// Error enum for crate-specific errors.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Queries cannot be converted to strings.
    #[error("cannot convert queries to strings")]
    CannotConvertQueryToString(serde_json::Map<String, serde_json::Value>),

    /// CQL2 JSON cannot (currently) be converted to strings.
    ///
    /// TODO support conversion
    #[error("cannot convert cql2-json to strings")]
    CannotConvertCql2JsonToString(serde_json::Map<String, serde_json::Value>),

    /// [chrono::ParseError]
    #[error(transparent)]
    ChronoParse(#[from] chrono::ParseError),

    /// [cql2::Error]
    #[error(transparent)]
    Cql2(#[from] Box<cql2::Error>),

    /// [geojson::Error]
    #[error(transparent)]
    Geojson(#[from] Box<geojson::Error>),

    /// An empty datetime interval.
    #[error("empty datetime interval")]
    EmptyDatetimeInterval,

    /// Some functionality requires a certain optional feature to be enabled.
    #[error("feature not enabled: {0}")]
    FeatureNotEnabled(&'static str),

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
    #[error("invalid bbox ({0:?}): {1}")]
    InvalidBbox(Vec<f64>, &'static str),

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

    /// [std::num::ParseIntError]
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    /// [std::num::ParseFloatError]
    #[error(transparent)]
    ParseFloatError(#[from] std::num::ParseFloatError),

    /// A search has both bbox and intersects.
    #[error("search has bbox and intersects")]
    SearchHasBboxAndIntersects(Box<crate::api::Search>),

    /// [serde_json::Error]
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    /// [serde_urlencoded::ser::Error]
    #[error(transparent)]
    SerdeUrlencodedSer(#[from] serde_urlencoded::ser::Error),

    /// The start time is after the end time.
    #[error("start ({0}) is after end ({1})")]
    StartIsAfterEnd(
        chrono::DateTime<chrono::FixedOffset>,
        chrono::DateTime<chrono::FixedOffset>,
    ),

    /// [std::num::TryFromIntError]
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),

    /// Returned when the `type` field of a STAC object does not equal `"Feature"`, `"Catalog"`, or `"Collection"`.
    #[error("unknown \"type\": {0}")]
    UnknownType(String),

    /// This functionality is not yet implemented.
    #[error("this functionality is not yet implemented: {0}")]
    Unimplemented(&'static str),

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

    /// [geoarrow_schema::error::GeoArrowError]
    #[error(transparent)]
    #[cfg(feature = "geoarrow")]
    GeoArrow(#[from] geoarrow_schema::error::GeoArrowError),

    /// [wkb::error::WkbError]
    #[error(transparent)]
    #[cfg(feature = "geoarrow")]
    Wkb(#[from] wkb::error::WkbError),

    /// No geoparquet metadata in a stac-geoparquet file.
    #[error("no geoparquet metadata")]
    #[cfg(feature = "geoparquet")]
    MissingGeoparquetMetadata,

    /// [parquet::errors::ParquetError]
    #[error(transparent)]
    #[cfg(feature = "geoparquet")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Invalid year value.
    #[error("invalid year: {0}")]
    InvalidYear(i32),

    /// Unrecognized date format.
    #[error("unrecognized date format: {0}")]
    UnrecognizedDateFormat(String),
}
