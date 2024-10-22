use crate::Search;
use chrono::{DateTime, FixedOffset};
use serde_json::{Map, Value};
use thiserror::Error;

/// Crate-specific error enum.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Queries cannot be converted to strings.
    #[error("cannot convert queries to strings")]
    CannotConvertQueryToString(Map<String, Value>),

    /// CQL2 JSON cannot (currently) be converted to strings.
    ///
    /// TODO support conversion
    #[error("cannot convert cql2-json to strings")]
    CannotConvertCql2JsonToString(Map<String, Value>),

    /// [chrono::ParseError]
    #[error(transparent)]
    ChronoParse(#[from] chrono::ParseError),

    /// [geojson::Error]
    #[error(transparent)]
    GeoJson(#[from] Box<geojson::Error>),

    /// An empty datetime interval.
    #[error("empty datetime interval")]
    EmptyDatetimeInterval,

    /// Some functionality requires a certain optional feature to be enabled.
    #[error("feature not enabled: {0}")]
    FeatureNotEnabled(&'static str),

    /// Invalid bounding box.
    #[error("invalid bbox ({0:?}): {1}")]
    InvalidBbox(Vec<f64>, &'static str),

    /// [http::header::InvalidHeaderName]
    #[error(transparent)]
    #[cfg(feature = "client")]
    InvalidHeaderName(#[from] http::header::InvalidHeaderName),

    /// [http::header::InvalidHeaderValue]
    #[error(transparent)]
    #[cfg(feature = "client")]
    InvalidHeaderValue(#[from] http::header::InvalidHeaderValue),

    /// [http::method::InvalidMethod]
    #[error(transparent)]
    #[cfg(feature = "client")]
    InvalidMethod(#[from] http::method::InvalidMethod),

    /// [std::io::Error]
    #[error(transparent)]
    #[cfg(feature = "client")]
    Io(#[from] std::io::Error),

    /// [tokio::task::JoinError]
    #[error(transparent)]
    #[cfg(feature = "client")]
    Join(#[from] tokio::task::JoinError),

    /// [std::num::ParseIntError]
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    /// [std::num::ParseFloatError]
    #[error(transparent)]
    ParseFloatError(#[from] std::num::ParseFloatError),

    /// [reqwest::Error]
    #[error(transparent)]
    #[cfg(feature = "client")]
    Reqwest(#[from] reqwest::Error),

    /// A search has both bbox and intersects.
    #[error("search has bbox and intersects")]
    SearchHasBboxAndIntersects(Box<Search>),

    /// [serde_json::Error]
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    /// [serde_urlencoded::ser::Error]
    #[error(transparent)]
    SerdeUrlencodedSer(#[from] serde_urlencoded::ser::Error),

    /// [stac::Error]
    #[error(transparent)]
    Stac(#[from] stac::Error),

    /// The start time is after the end time.
    #[error("start ({0}) is after end ({1})")]
    StartIsAfterEnd(DateTime<FixedOffset>, DateTime<FixedOffset>),

    /// [std::num::TryFromIntError]
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),

    /// [url::ParseError]
    #[error(transparent)]
    UrlParse(#[from] url::ParseError),

    /// This functionality is not yet implemented.
    #[error("this functionality is not yet implemented: {0}")]
    Unimplemented(&'static str),
}
