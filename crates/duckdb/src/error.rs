use thiserror::Error;

/// A crate-specific error enum.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// [chrono::format::ParseError]
    #[error(transparent)]
    ChronoParse(#[from] chrono::format::ParseError),

    /// [cql2::Error]
    #[error(transparent)]
    Cql2(#[from] cql2::Error),

    /// [duckdb::Error]
    #[error(transparent)]
    DuckDB(#[from] duckdb::Error),

    /// [geoarrow_schema::error::GeoArrowError]
    #[error(transparent)]
    GeoArrow(#[from] geoarrow_schema::error::GeoArrowError),

    /// [serde_json::Error]
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    /// [geojson::Error]
    #[error(transparent)]
    GeoJSON(#[from] Box<geojson::Error>),

    /// [stac::Error]
    #[error(transparent)]
    Stac(#[from] stac::Error),

    /// [stac_api::Error]
    #[error(transparent)]
    StacApi(#[from] stac_api::Error),

    /// The query search extension is not implemented.
    #[error("query is not implemented")]
    QueryNotImplemented,

    /// [std::num::TryFromIntError]
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),
}
