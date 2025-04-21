//! Use [duckdb](https://duckdb.org/) with [STAC](https://stacspec.org).

#![deny(unused_crate_dependencies)]

use arrow_array::RecordBatch;
use chrono::DateTime;
use cql2::{Expr, ToDuckSQL};
use duckdb::{Connection, types::Value};
use geo::BoundingRect;
use geojson::Geometry;
use stac::{Collection, SpatialExtent, TemporalExtent, geoarrow::Table};
use stac_api::{Direction, Search};
use std::fmt::Debug;
use thiserror::Error;

const DEFAULT_COLLECTION_DESCRIPTION: &str =
    "Auto-generated collection from stac-geoparquet extents";

/// Searches a stac-geoparquet file.
pub fn search(
    href: &str,
    mut search: Search,
    max_items: Option<usize>,
) -> Result<stac_api::ItemCollection> {
    if let Some(max_items) = max_items {
        search.limit = Some(max_items.try_into()?);
    } else {
        search.limit = None;
    };
    let config = Config::from_href(href);
    let client = Client::with_config(config)?;
    client.search_to_json(href, search)
}

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

    /// [geoarrow_array::error::GeoArrowError]
    #[error(transparent)]
    GeoArrow(#[from] geoarrow_array::error::GeoArrowError),

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

    /// [std::num::TryFromIntError]
    #[error(transparent)]
    TryFromInt(#[from] std::num::TryFromIntError),
}

/// A crate-specific result type.
pub type Result<T> = std::result::Result<T, Error>;

/// A client for making DuckDB requests for STAC objects.
#[derive(Debug)]
pub struct Client {
    connection: Connection,

    /// The client's configuration.
    pub config: Config,
}

/// Configuration for a client.
#[derive(Debug, Clone)]
pub struct Config {
    /// Whether to enable hive partitioning.
    ///
    /// False by default.
    pub use_hive_partitioning: bool,

    /// Convert wkb columns to geometries?
    ///
    /// Disable this to enable geopandas reading, for example.
    pub convert_wkb: bool,

    /// Whether to enable the S3 credential chain, which allows s3:// url access.
    ///
    /// True by default.
    pub use_s3_credential_chain: bool,

    /// Whether to enable the Azure credential chain, which allows az:// url access.
    ///
    /// True by default.
    pub use_azure_credential_chain: bool,

    /// Whether to directly install the httpfs extension.
    pub use_httpfs: bool,

    /// Whether to install extensions when creating a new connection.
    pub install_extensions: bool,

    /// Use a custom extension repository.
    pub custom_extension_repository: Option<String>,

    /// Set the extension directory.
    pub extension_directory: Option<String>,
}

/// A SQL query.
#[derive(Debug)]
pub struct Query {
    /// The SQL.
    pub sql: String,

    /// The parameters.
    pub params: Vec<Value>,
}

/// A DuckDB extension
// TODO implement aliases ... I don't know how vectors work yet 😢
#[derive(Debug)]
pub struct Extension {
    /// The extension name.
    pub name: String,

    /// Is the extension loaded?
    pub loaded: bool,

    /// Is the extension installed?
    pub installed: bool,

    /// The path to the extension.
    ///
    /// This might be `(BUILT-IN)` for the core extensions.
    pub install_path: Option<String>,

    /// The extension description.
    pub description: String,

    /// The extension version.
    pub version: Option<String>,

    /// The install mode.
    ///
    /// We don't bother making this an enum, yet.
    pub install_mode: Option<String>,

    /// Where the extension was installed from.
    pub installed_from: Option<String>,
}

impl Config {
    /// Creates a configuration from an href.
    ///
    /// Use this to, e.g., autodetect s3 urls.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_duckdb::Config;
    /// let config = Config::from_href("s3://bucket/item.json");
    /// assert!(config.use_s3_credential_chain);
    /// ```
    pub fn from_href(s: &str) -> Config {
        Config {
            use_s3_credential_chain: s.starts_with("s3://"),
            ..Default::default()
        }
    }
}

impl Client {
    /// Creates a new client with no data sources.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_duckdb::Client;
    ///
    /// let client = Client::new().unwrap();
    /// ```
    pub fn new() -> Result<Client> {
        Client::with_config(Config::default())
    }

    /// Creates a new client with the provided configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_duckdb::{Client, Config};
    ///
    /// let config = Config {
    ///     use_hive_partitioning: true,
    ///     convert_wkb: true,
    ///     use_s3_credential_chain: true,
    ///     use_azure_credential_chain: true,
    ///     use_httpfs: true,
    ///     install_extensions: true,
    ///     custom_extension_repository: None,
    ///     extension_directory: None,
    /// };
    /// let client = Client::with_config(config);
    /// ```
    pub fn with_config(config: Config) -> Result<Client> {
        let connection = Connection::open_in_memory()?;
        if let Some(ref custom_extension_repository) = config.custom_extension_repository {
            log::debug!("setting custom extension repository: {custom_extension_repository}");
            connection.execute(
                "SET custom_extension_repository = ?",
                [custom_extension_repository],
            )?;
        }
        if let Some(ref extension_directory) = config.extension_directory {
            log::debug!("setting extension directory: {extension_directory}");
            connection.execute("SET extension_directory = ?", [extension_directory])?;
        }
        if config.install_extensions {
            log::debug!("installing spatial");
            connection.execute("INSTALL spatial", [])?;
            log::debug!("installing icu");
            connection.execute("INSTALL icu", [])?;
        }
        connection.execute("LOAD spatial", [])?;
        connection.execute("LOAD icu", [])?;
        if config.use_httpfs && config.install_extensions {
            log::debug!("installing httpfs");
            connection.execute("INSTALL httpfs", [])?;
        }
        if config.use_s3_credential_chain {
            if config.install_extensions {
                log::debug!("installing aws");
                connection.execute("INSTALL aws", [])?;
            }
            log::debug!("creating s3 secret");
            connection.execute("CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN)", [])?;
        }
        if config.use_azure_credential_chain {
            if config.install_extensions {
                log::debug!("installing azure");
                connection.execute("INSTALL azure", [])?;
            }
            log::debug!("creating azure secret");
            connection.execute("CREATE SECRET (TYPE azure, PROVIDER CREDENTIAL_CHAIN)", [])?;
        }
        Ok(Client { connection, config })
    }

    /// Returns a vector of all extensions.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_duckdb::Client;
    ///
    /// let client = Client::new().unwrap();
    /// let extensions = client.extensions().unwrap();
    /// let spatial = extensions.into_iter().find(|extension| extension.name == "spatial").unwrap();
    /// assert!(spatial.loaded);
    /// ```
    pub fn extensions(&self) -> Result<Vec<Extension>> {
        let mut statement = self.connection.prepare(
            "SELECT extension_name, loaded, installed, install_path, description, extension_version, install_mode, installed_from FROM duckdb_extensions();",
        )?;
        let extensions = statement
            .query_map([], |row| {
                Ok(Extension {
                    name: row.get("extension_name")?,
                    loaded: row.get("loaded")?,
                    installed: row.get("installed")?,
                    install_path: row.get("install_path")?,
                    description: row.get("description")?,
                    version: row.get("extension_version")?,
                    install_mode: row.get("install_mode")?,
                    installed_from: row.get("installed_from")?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, duckdb::Error>>()?;
        Ok(extensions)
    }

    /// Returns one or more [stac::Collection] from the items in the stac-geoparquet file.
    pub fn collections(&self, href: &str) -> Result<Vec<Collection>> {
        let start_datetime= if self.connection.prepare(&format!(
            "SELECT column_name FROM (DESCRIBE SELECT * from {}) where column_name = 'start_datetime'",
            self.read_parquet_str(href)
        ))?.query([])?.next()?.is_some() {
            "strftime(min(coalesce(start_datetime, datetime)), '%xT%X%z')"
        } else {
            "strftime(min(datetime), '%xT%X%z')"
        };
        let end_datetime = if self
            .connection
            .prepare(&format!(
            "SELECT column_name FROM (DESCRIBE SELECT * from {}) where column_name = 'end_datetime'",
            self.read_parquet_str(href)
        ))?
            .query([])?
            .next()?
            .is_some()
        {
            "strftime(max(coalesce(end_datetime, datetime)), '%xT%X%z')"
        } else {
            "strftime(max(datetime), '%xT%X%z')"
        };
        let mut statement = self.connection.prepare(&format!(
            "SELECT DISTINCT collection FROM {}",
            self.read_parquet_str(href)
        ))?;
        let mut collections = Vec::new();
        for row in statement.query_map([], |row| row.get::<_, String>(0))? {
            let collection_id = row?;
            let mut statement = self.connection.prepare(&
                format!("SELECT ST_AsGeoJSON(ST_Extent_Agg(geometry)), {}, {} FROM {} WHERE collection = $1", start_datetime, end_datetime,
                self.read_parquet_str(href)
            ))?;
            let row = statement.query_row([&collection_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?;
            let mut collection = Collection::new(collection_id, DEFAULT_COLLECTION_DESCRIPTION);
            let geometry: geo::Geometry = Geometry::from_json_value(serde_json::from_str(&row.0)?)
                .map_err(Box::new)?
                .try_into()
                .map_err(Box::new)?;
            if let Some(bbox) = geometry.bounding_rect() {
                collection.extent.spatial = SpatialExtent {
                    bbox: vec![bbox.into()],
                };
            }
            collection.extent.temporal = TemporalExtent {
                interval: vec![[
                    Some(DateTime::parse_from_str(&row.1, "%FT%T%#z")?.into()),
                    Some(DateTime::parse_from_str(&row.2, "%FT%T%#z")?.into()),
                ]],
            };
            collections.push(collection);
        }
        Ok(collections)
    }

    /// Searches this client, returning a [stac::ItemCollection].
    pub fn search(&self, href: &str, search: impl Into<Search>) -> Result<stac::ItemCollection> {
        let record_batches = self.search_to_arrow(href, search)?;
        if record_batches.is_empty() {
            return Ok(Vec::new().into());
        }
        let schema = record_batches[0].schema();
        let table = Table::new(record_batches, schema);
        let items = stac::geoarrow::from_record_batch_reader(table.into_reader())?;
        Ok(items)
    }

    /// Searches this client, returning a [stac_api::ItemCollection].
    ///
    /// Use this method if you want JSON that might not be valid STAC items,
    /// e.g. if you've excluded required fields from the response.
    pub fn search_to_json(
        &self,
        href: &str,
        search: impl Into<Search>,
    ) -> Result<stac_api::ItemCollection> {
        let record_batches = self.search_to_arrow(href, search)?;
        if record_batches.is_empty() {
            return Ok(Vec::new().into());
        }
        let schema = record_batches[0].schema();
        let table = Table::new(record_batches, schema);
        let items = stac::geoarrow::json::from_record_batch_reader(table.into_reader())?;
        let item_collection = stac_api::ItemCollection::new(items)?;
        Ok(item_collection)
    }

    /// Searches this client, returning a vector of all matched record batches.
    pub fn search_to_arrow_table(
        &self,
        href: &str,
        search: impl Into<Search>,
    ) -> Result<Option<Table>> {
        let record_batches = self.search_to_arrow(href, search)?;
        if record_batches.is_empty() {
            Ok(None)
        } else {
            let schema = record_batches[0].schema();
            let table = Table::new(record_batches, schema);
            Ok(Some(table))
        }
    }

    /// Searches this client, returning a vector of all matched record batches.
    pub fn search_to_arrow(
        &self,
        href: &str,
        search: impl Into<Search>,
    ) -> Result<Vec<RecordBatch>> {
        let query = self.query(search, href)?;
        let mut statement = self.connection.prepare(&query.sql)?;
        log::debug!("DuckDB SQL: {}", query.sql);
        statement
            .query_arrow(duckdb::params_from_iter(query.params))?
            .map(|record_batch| {
                let record_batch = if self.config.convert_wkb {
                    stac::geoarrow::with_native_geometry(record_batch, "geometry")?
                } else {
                    stac::geoarrow::add_wkb_metadata(record_batch, "geometry")?
                };
                Ok(record_batch)
            })
            .collect::<Result<_>>()
    }

    fn query(&self, search: impl Into<Search>, href: &str) -> Result<Query> {
        let mut search: Search = search.into();
        // Get suffix information early so we can take ownership of other parts of search as we go along.
        let limit = search.items.limit.take();
        let offset = search
            .items
            .additional_fields
            .get("offset")
            .and_then(|v| v.as_i64());
        let sortby = std::mem::take(&mut search.items.sortby);
        let fields = std::mem::take(&mut search.items.fields);

        let mut statement = self.connection.prepare(&format!(
            "SELECT column_name FROM (DESCRIBE SELECT * from {})",
            self.read_parquet_str(href)
        ))?;
        let mut columns = Vec::new();
        // Can we use SQL magic to make our query not depend on which columns are present?
        let mut has_start_datetime = false;
        let mut has_end_datetime: bool = false;
        for row in statement.query_map([], |row| row.get::<_, String>(0))? {
            let column = row?;
            if column == "start_datetime" {
                has_start_datetime = true;
            }
            if column == "end_datetime" {
                has_end_datetime = true;
            }

            if let Some(fields) = fields.as_ref() {
                if fields.exclude.contains(&column)
                    || !(fields.include.is_empty() || fields.include.contains(&column))
                {
                    continue;
                }
            }

            if column == "geometry" {
                columns.push("ST_AsWKB(geometry) geometry".to_string());
            } else {
                columns.push(format!("\"{}\"", column));
            }
        }

        let mut wheres = Vec::new();
        let mut params = Vec::new();
        if !search.ids.is_empty() {
            wheres.push(format!(
                "id IN ({})",
                (0..search.ids.len())
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(",")
            ));
            params.extend(search.ids.into_iter().map(Value::Text));
        }
        if let Some(intersects) = search.intersects {
            wheres.push("ST_Intersects(geometry, ST_GeomFromGeoJSON(?))".to_string());
            params.push(Value::Text(intersects.to_string()));
        }
        if !search.collections.is_empty() {
            wheres.push(format!(
                "collection IN ({})",
                (0..search.collections.len())
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(",")
            ));
            params.extend(search.collections.into_iter().map(Value::Text));
        }
        if let Some(bbox) = search.items.bbox {
            wheres.push("ST_Intersects(geometry, ST_GeomFromGeoJSON(?))".to_string());
            params.push(Value::Text(bbox.to_geometry().to_string()));
        }
        if let Some(datetime) = search.items.datetime {
            let interval = stac::datetime::parse(&datetime)?;
            if let Some(start) = interval.0 {
                wheres.push(format!(
                    "?::TIMESTAMPTZ <= {}",
                    if has_start_datetime {
                        "start_datetime"
                    } else {
                        "datetime"
                    }
                ));
                params.push(Value::Text(start.to_rfc3339()));
            }
            if let Some(end) = interval.1 {
                wheres.push(format!(
                    "?::TIMESTAMPTZ >= {}", // Inclusive, https://github.com/radiantearth/stac-spec/pull/1280
                    if has_end_datetime {
                        "end_datetime"
                    } else {
                        "datetime"
                    }
                ));
                params.push(Value::Text(end.to_rfc3339()));
            }
        }
        if let Some(filter) = search.items.filter {
            let expr: Expr = filter.try_into()?;
            let sql = expr.to_ducksql()?;
            wheres.push(sql);
        }
        if search.items.query.is_some() {
            todo!("Implement the query extension");
        }

        let mut suffix = String::new();
        if !wheres.is_empty() {
            suffix.push_str(&format!(" WHERE {}", wheres.join(" AND ")));
        }
        if !sortby.is_empty() {
            let mut order_by = Vec::with_capacity(sortby.len());
            for sortby in sortby {
                order_by.push(format!(
                    "{} {}",
                    sortby.field,
                    match sortby.direction {
                        Direction::Ascending => "ASC",
                        Direction::Descending => "DESC",
                    }
                ));
            }
            suffix.push_str(&format!(" ORDER BY {}", order_by.join(", ")));
        }
        if let Some(limit) = limit {
            suffix.push_str(&format!(" LIMIT {}", limit));
        }
        if let Some(offset) = offset {
            suffix.push_str(&format!(" OFFSET {}", offset));
        }
        Ok(Query {
            sql: format!(
                "SELECT {} FROM {}{}",
                columns.join(","),
                self.read_parquet_str(href),
                suffix,
            ),
            params,
        })
    }

    fn read_parquet_str(&self, href: &str) -> String {
        if self.config.use_hive_partitioning {
            format!(
                "read_parquet('{}', filename=true, hive_partitioning=1)",
                href
            )
        } else {
            format!("read_parquet('{}', filename=true)", href)
        }
    }
}

/// Return this crate's version.
///
/// # Examples
///
/// ```
/// println!("{}", stac_duckdb::version());
/// ```
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

impl Default for Config {
    fn default() -> Self {
        Config {
            use_hive_partitioning: false,
            convert_wkb: true,
            use_s3_credential_chain: false,
            use_azure_credential_chain: false,
            use_httpfs: false,
            install_extensions: true,
            custom_extension_repository: None,
            extension_directory: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Client;
    use geo::Geometry;
    use rstest::{fixture, rstest};
    use stac::{Bbox, Validate};
    use stac_api::{Search, Sortby};
    use std::sync::Mutex;

    static MUTEX: Mutex<()> = Mutex::new(());

    #[fixture]
    fn client() -> Client {
        let _mutex = MUTEX.lock().unwrap();
        Client::new().unwrap()
    }

    #[rstest]
    fn search_all(client: Client) {
        let item_collection = client
            .search("data/100-sentinel-2-items.parquet", Search::default())
            .unwrap();
        assert_eq!(item_collection.items.len(), 100);
        item_collection.items[0].validate().unwrap();
    }

    #[rstest]
    fn search_ids(client: Client) {
        let item_collection = client
            .search(
                "data/100-sentinel-2-items.parquet",
                Search::default().ids(vec![
                    "S2A_MSIL2A_20240326T174951_R141_T13TDE_20240329T224429".to_string(),
                ]),
            )
            .unwrap();
        assert_eq!(item_collection.items.len(), 1);
        assert_eq!(
            item_collection.items[0].id,
            "S2A_MSIL2A_20240326T174951_R141_T13TDE_20240329T224429"
        );
    }

    #[rstest]
    fn search_intersects(client: Client) {
        let item_collection = client
            .search(
                "data/100-sentinel-2-items.parquet",
                Search::default().intersects(&Geometry::Point(geo::point! { x: -106., y: 40.5 })),
            )
            .unwrap();
        assert_eq!(item_collection.items.len(), 50);
    }

    #[rstest]
    fn search_collections(client: Client) {
        let item_collection = client
            .search(
                "data/100-sentinel-2-items.parquet",
                Search::default().collections(vec!["sentinel-2-l2a".to_string()]),
            )
            .unwrap();
        assert_eq!(item_collection.items.len(), 100);

        let item_collection = client
            .search(
                "data/100-sentinel-2-items.parquet",
                Search::default().collections(vec!["foobar".to_string()]),
            )
            .unwrap();
        assert_eq!(item_collection.items.len(), 0);
    }

    #[rstest]
    fn search_bbox(client: Client) {
        let item_collection = client
            .search(
                "data/100-sentinel-2-items.parquet",
                Search::default().bbox(Bbox::new(-106.1, 40.5, -106.0, 40.6)),
            )
            .unwrap();
        assert_eq!(item_collection.items.len(), 50);
    }

    #[rstest]
    fn search_datetime(client: Client) {
        let item_collection = client
            .search(
                "data/100-sentinel-2-items.parquet",
                Search::default().datetime("2024-12-02T00:00:00Z/.."),
            )
            .unwrap();
        assert_eq!(item_collection.items.len(), 1);
        let item_collection = client
            .search(
                "data/100-sentinel-2-items.parquet",
                Search::default().datetime("../2024-12-02T00:00:00Z"),
            )
            .unwrap();
        assert_eq!(item_collection.items.len(), 99);
    }

    #[rstest]
    fn search_limit(client: Client) {
        let item_collection = client
            .search(
                "data/100-sentinel-2-items.parquet",
                Search::default().limit(42),
            )
            .unwrap();
        assert_eq!(item_collection.items.len(), 42);
    }

    #[rstest]
    fn search_offset(client: Client) {
        let mut search = Search::default().limit(1);
        search
            .items
            .additional_fields
            .insert("offset".to_string(), 1.into());
        let item_collection = client
            .search("data/100-sentinel-2-items.parquet", search)
            .unwrap();
        assert_eq!(
            item_collection.items[0].id,
            "S2A_MSIL2A_20241201T175721_R141_T13TDE_20241201T213150"
        );
    }

    #[rstest]
    fn search_sortby(client: Client) {
        let item_collection = client
            .search(
                "data/100-sentinel-2-items.parquet",
                Search::default()
                    .sortby(vec![Sortby::asc("datetime")])
                    .limit(1),
            )
            .unwrap();
        assert_eq!(
            item_collection.items[0].id,
            "S2A_MSIL2A_20240326T174951_R141_T13TDE_20240329T224429"
        );

        let item_collection = client
            .search(
                "data/100-sentinel-2-items.parquet",
                Search::default()
                    .sortby(vec![Sortby::desc("datetime")])
                    .limit(1),
            )
            .unwrap();
        assert_eq!(
            item_collection.items[0].id,
            "S2B_MSIL2A_20241203T174629_R098_T13TDE_20241203T211406"
        );
    }

    #[rstest]
    fn search_fields(client: Client) {
        let item_collection = client
            .search_to_json(
                "data/100-sentinel-2-items.parquet",
                Search::default().fields("+id".parse().unwrap()).limit(1),
            )
            .unwrap();
        assert_eq!(item_collection.items[0].len(), 1);
    }

    #[rstest]
    fn collections(client: Client) {
        let collections = client
            .collections("data/100-sentinel-2-items.parquet")
            .unwrap();
        assert_eq!(collections.len(), 1);
    }

    #[rstest]
    fn to_arrow_table(client: Client) {
        let table = client
            .search_to_arrow_table("data/100-sentinel-2-items.parquet", Search::default())
            .unwrap()
            .unwrap();
        assert_eq!(table.len(), 100);

        assert!(
            client
                .search_to_arrow_table(
                    "data/100-sentinel-2-items.parquet",
                    serde_json::from_value::<Search>(serde_json::json!({
                        "collections": ["not-a-collection"]
                    }))
                    .unwrap()
                )
                .unwrap()
                .is_none()
        );
    }

    #[rstest]
    fn to_arrow_table_wkb(mut client: Client) {
        client.config.convert_wkb = false;
        let table = client
            .search_to_arrow_table("data/100-sentinel-2-items.parquet", Search::default())
            .unwrap()
            .unwrap();
        assert_eq!(table.len(), 100);
        let schema = table.into_inner().1;
        assert_eq!(
            schema.field_with_name("geometry").unwrap().metadata()["ARROW:extension:name"],
            "geoarrow.wkb"
        );
    }

    #[rstest]
    fn filter(client: Client) {
        let mut search = Search::default();
        search.filter = Some("sat:relative_orbit = 98".parse().unwrap());
        let item_collection = client
            .search("data/100-sentinel-2-items.parquet", search)
            .unwrap();
        assert_eq!(item_collection.items.len(), 49);
    }
}
