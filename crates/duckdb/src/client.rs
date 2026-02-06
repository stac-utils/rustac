use crate::{Error, Extension, Result};
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{ArrowError, SchemaRef};
use chrono::DateTime;
use cql2::{Expr, ToDuckSQL};
use duckdb::{Connection, Statement, types::Value};
use geo::BoundingRect;
use geojson::Geometry;
use stac::api::{ArrowSearchClient, CollectionSearchClient, Direction, Search, SearchClient};
use stac::{Collection, SpatialExtent, TemporalExtent, geoarrow::DATETIME_COLUMNS};
use std::ops::{Deref, DerefMut};
use std::sync::Mutex;

/// Default hive partitioning value
pub const DEFAULT_USE_HIVE_PARTITIONING: bool = false;

/// Default convert wkb value.
pub const DEFAULT_CONVERT_WKB: bool = true;

/// The default collection description.
pub const DEFAULT_COLLECTION_DESCRIPTION: &str =
    "Auto-generated collection from stac-geoparquet extents";

/// The default union by name value.
pub const DEFAULT_UNION_BY_NAME: bool = true;

/// Whether to remove the filename column by default.
pub const DEFAULT_REMOVE_FILENAME_COLUMN: bool = true;

/// A client for making DuckDB requests for STAC objects.
#[derive(Debug)]
pub struct Client {
    connection: Connection,

    /// Whether to use hive partitioning
    pub use_hive_partitioning: bool,

    /// Whether to convert WKB to native geometries.
    ///
    /// If False, WKB metadata will be added.
    pub convert_wkb: bool,

    /// Whether to use `union_by_name` when querying.
    ///
    /// Defaults to true.
    pub union_by_name: bool,

    /// Whether to remove the `filename` column that DuckDB adds automatically.
    ///
    /// Defaults to true.
    pub remove_filename_column: bool,
}

impl Client {
    /// Creates a new client with an in-memory DuckDB connection.
    ///
    /// This function will install the spatial extension. If you'd like to
    /// manage your own extensions (e.g. if your extensions are stored in a
    /// different location), set things up then use `connection.into()` to get a
    /// new `Client`.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_duckdb::Client;
    ///
    /// let client = Client::new().unwrap();
    /// ```
    pub fn new() -> Result<Client> {
        let connection = Connection::open_in_memory()?;
        connection.execute("INSTALL spatial", [])?;
        connection.execute("LOAD spatial", [])?;
        connection.execute("INSTALL icu", [])?;
        connection.execute("LOAD icu", [])?;
        Ok(connection.into())
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
    /// ```
    pub fn extensions(&self) -> Result<Vec<Extension>> {
        let mut statement = self.prepare(
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
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_duckdb::Client;
    ///
    /// let client = Client::new().unwrap();
    /// let collections = client.collections("data/100-sentinel-2-items.parquet").unwrap();
    /// ```
    pub fn collections(&self, href: &str) -> Result<Vec<Collection>> {
        let start_datetime= if self.prepare(&format!(
            "SELECT column_name FROM (DESCRIBE SELECT * from {}) where column_name = 'start_datetime'",
            self.format_parquet_href(href)
        ))?.query([])?.next()?.is_some() {
            "strftime(min(coalesce(start_datetime, datetime)), '%xT%X%z')"
        } else {
            "strftime(min(datetime), '%xT%X%z')"
        };
        let end_datetime = if self
            .prepare(&format!(
            "SELECT column_name FROM (DESCRIBE SELECT * from {}) where column_name = 'end_datetime'",
            self.format_parquet_href(href)
        ))?
            .query([])?
            .next()?
            .is_some()
        {
            "strftime(max(coalesce(end_datetime, datetime)), '%xT%X%z')"
        } else {
            "strftime(max(datetime), '%xT%X%z')"
        };
        let mut statement = self.prepare(&format!(
            "SELECT DISTINCT collection FROM {}",
            self.format_parquet_href(href)
        ))?;
        let mut collections = Vec::new();
        for row in statement.query_map([], |row| row.get::<_, String>(0))? {
            let collection_id = row?;
            let mut statement = self.connection.prepare(&
                format!("SELECT ST_AsGeoJSON(ST_Extent_Agg(geometry)), {}, {} FROM {} WHERE collection = $1", start_datetime, end_datetime,
                self.format_parquet_href(href)
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

    /// Searches a single stac-geoparquet file.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_duckdb::Client;
    ///
    /// let client = Client::new().unwrap();
    /// let item_collection = client.search("data/100-sentinel-2-items.parquet", Default::default()).unwrap();
    /// ```
    pub fn search(&self, href: &str, search: Search) -> Result<stac::api::ItemCollection> {
        let mut arrow_iter = self.search_to_arrow(href, search)?;
        let Some(schema) = arrow_iter.schema() else {
            return Ok(Default::default());
        };

        let first_batch = match arrow_iter.next() {
            Some(batch) => batch?,
            None => return Ok(Default::default()),
        };

        let batches = std::iter::once(Ok(first_batch))
            .chain(arrow_iter)
            .map(|batch| batch.map_err(|err| ArrowError::ExternalError(Box::new(err))));

        let item_collection = stac::geoarrow::json::from_record_batch_reader(
            RecordBatchIterator::new(batches, schema),
        )?;
        Ok(item_collection.into())
    }

    /// Searches to an iterator of record batches.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_duckdb::Client;
    ///
    /// let client = Client::new().unwrap();
    /// let mut total = 0;
    /// for batch in client
    ///     .search_to_arrow("data/100-sentinel-2-items.parquet", Default::default())
    ///     .unwrap()
    /// {
    ///     let batch = batch.unwrap();
    ///     total += batch.num_rows();
    /// }
    /// assert_eq!(total, 100);
    /// ```
    pub fn search_to_arrow<'conn>(
        &'conn self,
        href: &str,
        search: Search,
    ) -> Result<SearchArrowBatchIter<'conn>> {
        if let Some((sql, params)) = self.build_query(href, search)? {
            log::debug!("duckdb sql: {sql}");
            let mut statement = self.prepare(&sql)?;
            statement.execute(duckdb::params_from_iter(params))?;
            log::debug!("query complete");
            Ok(SearchArrowBatchIter::new(
                statement,
                self.convert_wkb,
                self.remove_filename_column,
            ))
        } else {
            Ok(SearchArrowBatchIter::empty(
                self.convert_wkb,
                self.remove_filename_column,
            ))
        }
    }

    /// Returns the SQL query string and parameters for this href and search object.
    ///
    /// Returns `None` if we can _know_ that the query will return nothing.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_duckdb::Client;
    ///
    /// let client = Client::new().unwrap();
    /// let (sql, params) = client.build_query("data/100-sentinel-2-items.parquet", Default::default()).unwrap().unwrap();
    /// ```
    pub fn build_query(&self, href: &str, search: Search) -> Result<Option<(String, Vec<Value>)>> {
        // Note that we pull out some fields early so we can avoid closing some search strings below.

        if search.items.query.is_some() {
            return Err(Error::QueryNotImplemented);
        }

        // Check which columns we'll be selecting
        let mut statement = self.prepare(&format!(
            "SELECT column_name FROM (DESCRIBE SELECT * from {})",
            self.format_parquet_href(href)
        ))?;
        let mut has_start_datetime = false;
        let mut has_end_datetime = false;
        let mut column_names = Vec::new();
        let mut columns = Vec::new();
        for row in statement.query_map([], |row| row.get::<_, String>(0))? {
            let column = row?;
            if column == "start_datetime" {
                has_start_datetime = true;
            }
            if column == "end_datetime" {
                has_end_datetime = true;
            }

            if let Some(fields) = search.fields.as_ref()
                && (fields.exclude.contains(&column)
                    || !(fields.include.is_empty() || fields.include.contains(&column)))
            {
                continue;
            }

            if column == "geometry" {
                columns.push("ST_AsWKB(geometry) geometry".to_string());
            } else if DATETIME_COLUMNS.contains(&column.as_str()) {
                columns.push(format!("\"{column}\"::TIMESTAMPTZ {column}"))
            } else {
                columns.push(format!("\"{column}\""));
            }
            column_names.push(column);
        }

        // Get limit and offset
        let limit = search.items.limit;
        let offset = search
            .items
            .additional_fields
            .get("offset")
            .and_then(|v| v.as_i64());

        // Build order_by
        let mut order_by = Vec::with_capacity(search.sortby.len());
        for sortby in &search.sortby {
            order_by.push(format!(
                "\"{}\" {}",
                sortby.field,
                match sortby.direction {
                    Direction::Ascending => "ASC",
                    Direction::Descending => "DESC",
                }
            ));
        }

        // Build wheres and params
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
            if expr_properties_match(&expr, &column_names) {
                let sql = expr.to_ducksql().map_err(Box::new)?;
                wheres.push(sql);
            } else {
                return Ok(None);
            }
        }

        let mut suffix = String::new();
        if !wheres.is_empty() {
            suffix.push_str(&format!(" WHERE {}", wheres.join(" AND ")));
        }
        if !order_by.is_empty() {
            suffix.push_str(&format!(" ORDER BY {}", order_by.join(", ")));
        }
        if let Some(limit) = limit {
            suffix.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = offset {
            suffix.push_str(&format!(" OFFSET {offset}"));
        }

        let sql = format!(
            "SELECT {} FROM {}{}",
            columns.join(","),
            self.format_parquet_href(href),
            suffix,
        );
        Ok(Some((sql, params)))
    }

    fn format_parquet_href(&self, href: &str) -> String {
        format!(
            "read_parquet('{}', hive_partitioning={}, union_by_name={})",
            href,
            if self.use_hive_partitioning {
                "true"
            } else {
                "false"
            },
            if self.union_by_name { "true" } else { "false" }
        )
    }
}

fn expr_properties_match(expr: &Expr, properties: &[String]) -> bool {
    use Expr::*;

    match expr {
        Property { property } => properties.contains(property),
        Float(_) | Literal(_) | Bool(_) | Geometry(_) => true,
        Operation { args, .. } => args
            .iter()
            .all(|expr| expr_properties_match(expr, properties)),
        Interval { interval } => interval
            .iter()
            .all(|expr| expr_properties_match(expr, properties)),
        Timestamp { timestamp } => expr_properties_match(timestamp, properties),
        Date { date } => expr_properties_match(date, properties),
        Array(exprs) => exprs
            .iter()
            .all(|expr| expr_properties_match(expr, properties)),
        BBox { bbox } => bbox
            .iter()
            .all(|expr| expr_properties_match(expr, properties)),
        Null => expr_properties_match(expr, properties),
    }
}

impl Deref for Client {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl DerefMut for Client {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl From<Connection> for Client {
    fn from(connection: Connection) -> Self {
        Client {
            connection,
            use_hive_partitioning: DEFAULT_USE_HIVE_PARTITIONING,
            convert_wkb: DEFAULT_CONVERT_WKB,
            union_by_name: DEFAULT_UNION_BY_NAME,
            remove_filename_column: DEFAULT_REMOVE_FILENAME_COLUMN,
        }
    }
}

/// A DuckDB client bound to a specific stac-geoparquet href.
///
/// This wraps a [`Client`] with a specific href, implementing the
/// [`ArrowSearchClient`] trait. Because [`duckdb::Connection`] is not
/// [`Sync`], use [`Mutex<HrefClient>`](std::sync::Mutex) for the async client
/// traits ([`SearchClient`] and [`CollectionSearchClient`]).
///
/// # Examples
///
/// ```
/// use stac::api::ArrowSearchClient;
/// use stac_duckdb::HrefClient;
///
/// let client = HrefClient::new("data/100-sentinel-2-items.parquet").unwrap();
/// let record_batch_reader = client.search_to_arrow(Default::default()).unwrap();
/// ```
#[derive(Debug)]
pub struct HrefClient {
    client: Client,
    href: String,
}

impl HrefClient {
    /// Creates a new `HrefClient` for the given href.
    pub fn new(href: impl ToString) -> Result<HrefClient> {
        let client = Client::new()?;
        Ok(HrefClient {
            client,
            href: href.to_string(),
        })
    }

    /// Creates a new `HrefClient` from an existing [`Client`] and href.
    pub fn from_client(client: Client, href: impl ToString) -> HrefClient {
        HrefClient {
            client,
            href: href.to_string(),
        }
    }

    /// Returns a reference to the underlying [`Client`].
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Returns a mutable reference to the underlying [`Client`].
    pub fn client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    /// Returns the href.
    pub fn href(&self) -> &str {
        &self.href
    }
}

impl ArrowSearchClient for HrefClient {
    type Error = Error;
    type RecordBatchStream<'a> = ArrowBatchReader<'a>;

    fn search_to_arrow(&self, search: Search) -> std::result::Result<ArrowBatchReader<'_>, Error> {
        let iter = self.client.search_to_arrow(&self.href, search)?;
        Ok(ArrowBatchReader::new(iter))
    }
}

/// A thread-safe wrapper around [`HrefClient`] that implements
/// [`SearchClient`] and [`CollectionSearchClient`].
///
/// Use this when you need the async client traits. For [`ArrowSearchClient`],
/// use [`HrefClient`] directly.
///
/// # Examples
///
/// ```
/// use stac::api::SearchClient;
/// use stac_duckdb::SyncHrefClient;
///
/// let client = SyncHrefClient::new("data/100-sentinel-2-items.parquet").unwrap();
/// # tokio_test::block_on(async {
/// let item_collection = client.search(Default::default()).await.unwrap();
/// # })
/// ```
#[derive(Debug)]
pub struct SyncHrefClient {
    inner: Mutex<HrefClient>,
}

impl SyncHrefClient {
    /// Creates a new `SyncHrefClient` for the given href.
    pub fn new(href: impl ToString) -> Result<SyncHrefClient> {
        Ok(SyncHrefClient {
            inner: Mutex::new(HrefClient::new(href)?),
        })
    }

    /// Creates a new `SyncHrefClient` from an existing [`Client`] and href.
    pub fn from_client(client: Client, href: impl ToString) -> SyncHrefClient {
        SyncHrefClient {
            inner: Mutex::new(HrefClient::from_client(client, href)),
        }
    }
}

impl SearchClient for SyncHrefClient {
    type Error = Error;

    async fn search(
        &self,
        search: Search,
    ) -> std::result::Result<stac::api::ItemCollection, Error> {
        let guard = self.inner.lock().expect("SyncHrefClient mutex is poisoned");
        guard.client.search(&guard.href, search)
    }
}

impl CollectionSearchClient for SyncHrefClient {
    type Error = Error;

    async fn collections(&self) -> std::result::Result<Vec<Collection>, Error> {
        let guard = self.inner.lock().expect("SyncHrefClient mutex is poisoned");
        guard.client.collections(&guard.href)
    }
}

/// A wrapper around [`SearchArrowBatchIter`] that implements
/// [`arrow_array::RecordBatchReader`].
pub struct ArrowBatchReader<'a> {
    inner: SearchArrowBatchIter<'a>,
    schema: SchemaRef,
}

impl<'a> ArrowBatchReader<'a> {
    fn new(inner: SearchArrowBatchIter<'a>) -> Self {
        let schema = inner
            .schema()
            .unwrap_or_else(|| arrow_schema::Schema::empty().into());
        Self { inner, schema }
    }
}

impl Iterator for ArrowBatchReader<'_> {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|r| r.map_err(|e| ArrowError::ExternalError(Box::new(e))))
    }
}

impl arrow_array::RecordBatchReader for ArrowBatchReader<'_> {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Iterator returned by [`Client::search_to_arrow`].
pub struct SearchArrowBatchIter<'conn> {
    statement: Option<Statement<'conn>>,
    convert_wkb: bool,
    remove_filename_column: bool,
    schema: Option<SchemaRef>,
}

impl<'conn> SearchArrowBatchIter<'conn> {
    fn new(statement: Statement<'conn>, convert_wkb: bool, remove_filename_column: bool) -> Self {
        let schema = Some(statement.schema());
        Self {
            statement: Some(statement),
            convert_wkb,
            remove_filename_column,
            schema,
        }
    }

    fn empty(convert_wkb: bool, remove_filename_column: bool) -> Self {
        Self {
            statement: None,
            convert_wkb,
            remove_filename_column,
            schema: None,
        }
    }

    pub fn schema(&self) -> Option<SchemaRef> {
        self.schema.clone()
    }

    fn finalize_batch(&self, record_batch: RecordBatch) -> Result<RecordBatch> {
        let mut record_batch = if self.convert_wkb {
            stac::geoarrow::with_native_geometry(record_batch, "geometry")?
        } else {
            stac::geoarrow::add_wkb_metadata(record_batch, "geometry")?
        };
        if self.remove_filename_column {
            record_batch = remove_column(record_batch, "filename");
        }
        Ok(record_batch)
    }
}

impl<'conn> Iterator for SearchArrowBatchIter<'conn> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let statement = self.statement.as_ref()?;

        match statement.step() {
            Some(struct_array) => {
                let record_batch = RecordBatch::from(&struct_array);
                match self.finalize_batch(record_batch) {
                    Ok(batch) => Some(Ok(batch)),
                    Err(err) => {
                        self.statement = None;
                        Some(Err(err))
                    }
                }
            }
            None => {
                self.statement = None;
                None
            }
        }
    }
}

fn remove_column(mut record_batch: RecordBatch, name: &str) -> RecordBatch {
    if let Some((index, _)) = record_batch.schema().column_with_name(name) {
        record_batch.remove_column(index);
    }
    record_batch
}

#[cfg(test)]
mod tests {
    use super::Client;
    use duckdb::Connection;
    use geo::Geometry;
    use rstest::{fixture, rstest};
    use stac::Bbox;
    use stac::api::{Items, Search, Sortby};
    use stac_validate::Validate;

    #[fixture]
    #[once]
    fn install_extensions() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute("INSTALL icu", []).unwrap();
        connection.execute("INSTALL spatial", []).unwrap();
    }

    #[allow(unused_variables)]
    #[fixture]
    fn client(install_extensions: ()) -> Client {
        Client::new().unwrap()
    }

    #[rstest]
    fn extensions(client: Client) {
        let _ = client.extensions().unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn search(client: Client) {
        let item_collection = client
            .search("data/100-sentinel-2-items.parquet", Search::default())
            .unwrap();
        assert_eq!(item_collection.items.len(), 100);
        item_collection.items[0].validate().await.unwrap();
    }

    #[rstest]
    fn search_to_arrow(client: Client) {
        let record_batches = client
            .search_to_arrow("data/100-sentinel-2-items.parquet", Search::default())
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(record_batches.len(), 1);
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
            item_collection.items[0]["id"],
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
    fn search_datetime_empty_interval(client: Client) {
        let item_collection = client
            .search(
                "data/100-sentinel-2-items.parquet",
                Search::default().datetime("2024-12-02T00:00:00Z/"),
            )
            .unwrap();
        assert_eq!(item_collection.items.len(), 1);
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
            item_collection.items[0]["id"],
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
            item_collection.items[0]["id"],
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
            item_collection.items[0]["id"],
            "S2B_MSIL2A_20241203T174629_R098_T13TDE_20241203T211406"
        );
    }

    #[rstest]
    fn search_fields(client: Client) {
        let item_collection = client
            .search(
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
    fn no_convert_wkb(mut client: Client) {
        client.convert_wkb = false;
        let record_batches = client
            .search_to_arrow("data/100-sentinel-2-items.parquet", Search::default())
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let schema = record_batches[0].schema();
        assert_eq!(
            schema.field_with_name("geometry").unwrap().metadata()["ARROW:extension:name"],
            "geoarrow.wkb"
        );
    }

    #[rstest]
    fn filter(client: Client) {
        let search = Search {
            items: Items {
                filter: Some("sat:relative_orbit = 98".parse().unwrap()),
                ..Default::default()
            },
            ..Default::default()
        };
        let item_collection = client
            .search("data/100-sentinel-2-items.parquet", search)
            .unwrap();
        assert_eq!(item_collection.items.len(), 49);
    }

    #[rstest]
    fn filter_no_column(client: Client) {
        let search = Search {
            items: Items {
                filter: Some("foo:bar = 42".parse().unwrap()),
                ..Default::default()
            },
            ..Default::default()
        };
        let item_collection = client
            .search("data/100-sentinel-2-items.parquet", search)
            .unwrap();
        assert_eq!(item_collection.items.len(), 0);
    }

    #[rstest]
    fn sortby_property(client: Client) {
        let search = Search {
            items: Items {
                sortby: vec!["eo:cloud_cover".parse().unwrap()],
                ..Default::default()
            },
            ..Default::default()
        };
        let item_collection = client
            .search("data/100-sentinel-2-items.parquet", search)
            .unwrap();
        assert_eq!(item_collection.items.len(), 100);
    }

    #[rstest]
    fn union_by_name(client: Client) {
        let _ = client.search("data/*.parquet", Default::default()).unwrap();
    }

    #[rstest]
    fn no_union_by_name(mut client: Client) {
        client.union_by_name = false;
        let _ = client
            .search("data/*.parquet", Default::default())
            .unwrap_err();
    }

    #[rstest]
    fn remove_filename_column(client: Client) {
        let item_collection = client
            .search("data/100-sentinel-2-items.parquet", Default::default())
            .unwrap();
        for item in item_collection.items {
            assert!(
                !item["properties"]
                    .as_object()
                    .as_ref()
                    .unwrap()
                    .contains_key("filename")
            );
        }
    }
}
