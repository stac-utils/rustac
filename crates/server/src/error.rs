use thiserror::Error;

/// A crate-specific error type.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// [bb8::RunError]
    #[cfg(feature = "duckdb")]
    #[error(transparent)]
    Bb8DuckdbRun(#[from] Box<bb8::RunError<Error>>),

    /// [stac_duckdb::Error]
    #[cfg(feature = "duckdb")]
    #[error(transparent)]
    StacDuckdb(#[from] stac_duckdb::Error),

    /// A memory backend error.
    #[error("memory backend error: {0}")]
    MemoryBackend(String),

    /// [pgstac::Error]
    #[cfg(feature = "pgstac")]
    #[error(transparent)]
    Pgstac(#[from] pgstac::Error),

    /// [serde_json::Error]
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    /// [serde_urlencoded::ser::Error]
    #[error(transparent)]
    SerdeUrlencodedSer(#[from] serde_urlencoded::ser::Error),

    /// [stac::Error]
    #[error(transparent)]
    Stac(#[from] stac::Error),

    /// The backend is read-only.
    #[error("this backend is read-only")]
    ReadOnly,

    /// [std::num::TryFromIntError]
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),

    /// [url::ParseError]
    #[error(transparent)]
    UrlParse(#[from] url::ParseError),
}
