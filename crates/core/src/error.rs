use thiserror::Error;

/// Error enum for crate-specific errors.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// [arrow_schema::ArrowError]
    #[error(transparent)]
    #[cfg(feature = "geoarrow")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// [chrono::ParseError]
    #[error(transparent)]
    ChronoParse(#[from] chrono::ParseError),

    /// A required feature is not enabled.
    #[error("{0} is not enabled")]
    FeatureNotEnabled(&'static str),

    /// [fluent_uri::error::ParseError]
    #[error(transparent)]
    #[cfg(feature = "validate")]
    FluentUriParse(#[from] fluent_uri::error::ParseError),

    /// Returned when unable to read a STAC value from a path.
    #[error("{io}: {path}")]
    FromPath {
        /// The [std::io::Error]
        #[source]
        io: std::io::Error,

        /// The path.
        path: String,
    },

    /// [geoarrow::error::GeoArrowError]
    #[error(transparent)]
    #[cfg(feature = "geoarrow")]
    GeoArrow(#[from] geoarrow::error::GeoArrowError),

    /// [geojson::Error]
    #[error(transparent)]
    Geojson(#[from] Box<geojson::Error>),

    /// [std::io::Error]
    #[error(transparent)]
    Io(#[from] std::io::Error),

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

    /// Returned when there is not a required field on a STAC object
    #[error("no \"{0}\" field in the JSON object")]
    MissingField(&'static str),

    /// There are no items, when items are required.
    #[error("no items")]
    NoItems,

    /// [object_store::Error]
    #[error(transparent)]
    #[cfg(feature = "object-store")]
    ObjectStore(#[from] object_store::Error),

    /// [object_store::path::Error]
    #[error(transparent)]
    #[cfg(feature = "object-store")]
    ObjectStorePath(#[from] object_store::path::Error),

    /// [parquet::errors::ParquetError]
    #[error(transparent)]
    #[cfg(feature = "geoparquet")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// [reqwest::Error]
    #[cfg(any(feature = "reqwest", feature = "validate"))]
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    /// JSON is a scalar when an array or object was expected
    #[error("json value is not an object or an array")]
    ScalarJson(serde_json::Value),

    /// [serde_json::Error]
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    /// [stac_types::Error]
    #[error(transparent)]
    StacTypes(#[from] stac_types::Error),

    /// [tokio::task::JoinError]
    #[error(transparent)]
    #[cfg(any(feature = "validate", feature = "object-store"))]
    TokioJoin(#[from] tokio::task::JoinError),

    /// [std::num::TryFromIntError]
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),

    /// Returned when the `type` field of a STAC object does not equal `"Feature"`, `"Catalog"`, or `"Collection"`.
    #[error("unknown \"type\": {0}")]
    UnknownType(String),

    /// Unsupported file format.
    #[error("unsupported format: {0}")]
    UnsupportedFormat(String),

    /// Unsupported geoparquet type
    #[error("unsupported geoparquet type")]
    UnsupportedGeoparquetType,

    /// [url::ParseError]
    #[error(transparent)]
    UrlParse(#[from] url::ParseError),

    /// A list of validation errors.
    #[error("{} validation error(s)", .0.len())]
    #[cfg(feature = "validate")]
    Validation(Vec<Validation>),

    /// [jsonschema::ValidationError]
    #[cfg(feature = "validate")]
    #[error(transparent)]
    JsonschemaValidation(#[from] jsonschema::ValidationError<'static>),
}

/// A validation error
#[cfg(feature = "validate")]
#[derive(Debug)]
pub struct Validation {
    /// The ID of the STAC object that failed to validate.
    id: Option<String>,

    /// The type of the STAC object that failed to validate.
    r#type: Option<crate::Type>,

    /// The validation error.
    error: jsonschema::ValidationError<'static>,
}

#[cfg(feature = "validate")]
impl Validation {
    pub(crate) fn new(
        error: jsonschema::ValidationError<'_>,
        value: Option<&serde_json::Value>,
    ) -> Validation {
        use std::borrow::Cow;

        // Cribbed from https://docs.rs/jsonschema/latest/src/jsonschema/error.rs.html#21-30
        let error = jsonschema::ValidationError {
            instance_path: error.instance_path.clone(),
            instance: Cow::Owned(error.instance.into_owned()),
            kind: error.kind,
            schema_path: error.schema_path,
        };
        let mut id = None;
        let mut r#type = None;
        if let Some(value) = value.and_then(|v| v.as_object()) {
            id = value.get("id").and_then(|v| v.as_str()).map(String::from);
            r#type = value
                .get("type")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<crate::Type>().ok());
        }
        Validation { id, r#type, error }
    }
}

#[cfg(feature = "validate")]
impl Error {
    pub(crate) fn from_validation_errors<'a, I>(
        errors: I,
        value: Option<&serde_json::Value>,
    ) -> Error
    where
        I: Iterator<Item = jsonschema::ValidationError<'a>>,
    {
        Error::Validation(errors.map(|error| Validation::new(error, value)).collect())
    }
}

#[cfg(feature = "validate")]
impl std::fmt::Display for Validation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(r#type) = self.r#type {
            if let Some(id) = self.id.as_ref() {
                write!(f, "{}[id={id}]: {}", r#type, self.error)
            } else {
                write!(f, "{}: {}", r#type, self.error)
            }
        } else if let Some(id) = self.id.as_ref() {
            write!(f, "[id={id}]: {}", self.error)
        } else {
            write!(f, "{}", self.error)
        }
    }
}
