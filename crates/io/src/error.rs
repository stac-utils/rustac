use thiserror::Error;

/// Crate-specific error enum
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Returned when unable to read a STAC value from a path.
    #[error("{io}: {path}")]
    FromPath {
        /// The [std::io::Error]
        #[source]
        io: std::io::Error,

        /// The path.
        path: String,
    },

    /// [http::header::InvalidHeaderName]
    #[error(transparent)]
    InvalidHeaderName(#[from] http::header::InvalidHeaderName),

    /// [http::header::InvalidHeaderValue]
    #[error(transparent)]
    InvalidHeaderValue(#[from] http::header::InvalidHeaderValue),

    /// [http::method::InvalidMethod]
    #[error(transparent)]
    InvalidMethod(#[from] http::method::InvalidMethod),

    /// [tokio::task::JoinError]
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),

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

    #[error(transparent)]
    /// [reqwest::Error]
    Reqwest(#[from] reqwest::Error),

    #[error(transparent)]
    /// [serde_json::Error]
    SerdeJson(#[from] serde_json::Error),

    #[error(transparent)]
    /// [stac::Error]
    Stac(#[from] stac::Error),

    /// [std::num::TryFromIntError]
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),

    /// Unsupported file format.
    #[error("unsupported format: {0}")]
    UnsupportedFormat(String),

    /// [url::ParseError]
    #[error(transparent)]
    UrlParse(#[from] url::ParseError),
}
