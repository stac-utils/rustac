use thiserror::Error;

/// A crate-specific error enum.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// [arrow_schema::ArrowError]
    #[error(transparent)]
    Arrow(#[from] arrow_schema::ArrowError),

    /// [chrono::format::ParseError]
    #[error(transparent)]
    ChronoParse(#[from] chrono::format::ParseError),

    /// [cql2::Error]
    #[error(transparent)]
    Cql2(#[from] Box<cql2::Error>),

    /// [duckdb::Error]
    #[error(transparent)]
    DuckDB(#[from] duckdb::Error),

    /// [duckdb::arrow::error::ArrowError], from the version of arrow used by duckdb
    #[error(transparent)]
    DuckDBArrow(#[from] duckdb::arrow::error::ArrowError),

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

    /// The query search extension is not implemented.
    #[error("query is not implemented")]
    QueryNotImplemented,

    /// [std::num::TryFromIntError]
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),
}
