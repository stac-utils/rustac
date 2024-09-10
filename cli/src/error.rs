use crate::Value;
use thiserror::Error;

/// Crate specific error type.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// [std::io::Error]
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// [parquet::errors::ParquetError]
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),

    /// [reqwest::Error]
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    /// [serde_json::Error]
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    /// [stac_api::Error]
    #[error(transparent)]
    StacApi(#[from] stac_api::Error),

    /// [stac::Error]
    #[error(transparent)]
    Stac(#[from] stac::Error),

    /// [stac_async::Error]
    #[error(transparent)]
    StacAsync(#[from] stac_async::Error),

    /// [stac_duckdb::Error]
    #[cfg(feature = "duckdb")]
    #[error(transparent)]
    StacDuckdb(#[from] stac_duckdb::Error),

    /// [stac_server::Error]
    #[error(transparent)]
    StacServer(#[from] stac_server::Error),

    /// [stac_validate::Error]
    #[error(transparent)]
    StacValidate(#[from] stac_validate::Error),

    /// [tokio::sync::mpsc::error::SendError]
    #[error(transparent)]
    TokioSend(#[from] tokio::sync::mpsc::error::SendError<Value>),

    /// [tokio::task::JoinError]
    #[error(transparent)]
    TokioJoinError(#[from] tokio::task::JoinError),

    /// [std::num::TryFromIntError]
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),

    /// Unsupported format.
    #[error("unsupported (or unknown) format: {0}")]
    UnsupportedFormat(String),
}

impl Error {
    /// Returns this error return code.
    pub fn code(&self) -> i32 {
        // TODO make these codes more meaningful
        1
    }
}
