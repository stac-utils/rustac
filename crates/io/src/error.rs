use thiserror::Error;

/// Crate-specific error enum
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// An error occurred when getting an href.
    #[error("error when getting href={href}: {message}")]
    Get {
        /// The href that we were trying to get.
        href: String,

        /// The underling error message.
        message: String,
    },

    /// A required feature is not enabled.
    #[error("{0} is not enabled")]
    FeatureNotEnabled(&'static str),

    /// [fluent_uri::error::ParseError]
    #[error(transparent)]
    #[cfg(feature = "validate")]
    FluentUriParse(#[from] fluent_uri::error::ParseError<String>),

    /// Returned when unable to read a STAC value from a path.
    #[error("{io}: {path}")]
    FromPath {
        /// The [std::io::Error]
        #[source]
        io: std::io::Error,

        /// The path.
        path: String,
    },

    /// Returned when there is not a required field on a STAC object
    #[error("no \"{0}\" field in the JSON object")]
    MissingField(&'static str),

    /// [std::io::Error]
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[cfg(feature = "store")]
    #[error(transparent)]
    /// [object_store::Error]
    ObjectStore(#[from] object_store::Error),

    #[cfg(feature = "geoparquet")]
    #[error(transparent)]
    /// [parquet::errors::ParquetError]
    Parquet(#[from] parquet::errors::ParquetError),

    #[cfg(feature = "reqwest")]
    #[error(transparent)]
    /// [reqwest::Error]
    Reqwest(#[from] reqwest::Error),

    /// JSON is a scalar when an array or object was expected
    #[error("json value is not an object or an array")]
    ScalarJson(serde_json::Value),

    #[error(transparent)]
    /// [serde_json::Error]
    SerdeJson(#[from] serde_json::Error),

    #[error(transparent)]
    /// [stac::Error]
    Stac(#[from] stac::Error),

    /// Unsupported file format.
    #[error("unsupported format: {0}")]
    UnsupportedFormat(String),

    /// A list of validation errors.
    #[error("{} validation error(s)", .0.len())]
    #[cfg(feature = "validate")]
    Validation(Vec<validation::Validation>),

    /// [jsonschema::ValidationError]
    #[cfg(feature = "validate")]
    #[error(transparent)]
    JsonschemaValidation(#[from] jsonschema::ValidationError<'static>),

    /// [url::ParseError]
    #[error(transparent)]
    UrlParse(#[from] url::ParseError),
}

#[cfg(feature = "validate")]
mod validation {

    /// A validation error
    #[derive(Debug)]
    pub struct Validation {
        /// The ID of the STAC object that failed to validate.
        id: Option<String>,

        /// The type of the STAC object that failed to validate.
        r#type: Option<stac::Type>,

        /// The validation error.
        error: jsonschema::ValidationError<'static>,
    }

    impl Validation {
        pub(crate) fn new(
            error: jsonschema::ValidationError<'_>,
            value: Option<&serde_json::Value>,
        ) -> Validation {
            let mut id = None;
            let mut r#type = None;
            if let Some(value) = value.and_then(|v| v.as_object()) {
                id = value.get("id").and_then(|v| v.as_str()).map(String::from);
                r#type = value
                    .get("type")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<stac::Type>().ok());
            }
            Validation {
                id,
                r#type,
                error: error.to_owned(),
            }
        }

        /// Converts this validation error into a [serde_json::Value].
        pub fn into_json(self) -> serde_json::Value {
            let error_description = jsonschema::output::ErrorDescription::from(self.error);
            serde_json::json!({
                "id": self.id,
                "type": self.r#type,
                "error": error_description,
            })
        }
    }

    impl super::Error {
        pub(crate) fn from_validation_errors<'a, I>(
            errors: I,
            value: Option<&serde_json::Value>,
        ) -> super::Error
        where
            I: Iterator<Item = jsonschema::ValidationError<'a>>,
        {
            super::Error::Validation(errors.map(|error| Validation::new(error, value)).collect())
        }
    }

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
}
