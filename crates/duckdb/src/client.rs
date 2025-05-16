use crate::{Error, Extension, Result};
use arrow_array::{RecordBatch, RecordBatchIterator};
use chrono::DateTime;
use cql2::{Expr, ToDuckSQL};
use duckdb::{Connection, types::Value};
use geo::BoundingRect;
use geojson::Geometry;
use stac::{Collection, SpatialExtent, TemporalExtent, geoarrow::DATETIME_COLUMNS};
use stac_api::{Direction, Search};
use std::ops::{Deref, DerefMut};

/// Default hive partitioning value
pub const DEFAULT_USE_HIVE_PARTITIONING: bool = false;

/// Default convert wkb value.
pub const DEFAULT_CONVERT_WKB: bool = true;

const DEFAULT_COLLECTION_DESCRIPTION: &str =
    "Auto-generated collection from stac-geoparquet extents";

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
    pub fn search(&self, href: &str, search: Search) -> Result<stac_api::ItemCollection> {
        let record_batches = self.search_to_arrow(href, search)?;
        if record_batches.is_empty() {
            Ok(Default::default())
        } else {
            let schema = record_batches[0].schema();
            let item_collection = stac::geoarrow::json::from_record_batch_reader(
                RecordBatchIterator::new(record_batches.into_iter().map(Ok), schema),
            )?;
            Ok(item_collection.into())
        }
    }

    /// Searches to an iterator of record batches.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_duckdb::Client;
    ///
    /// let client = Client::new().unwrap();
    /// let record_batches = client.search_to_arrow("data/100-sentinel-2-items.parquet", Default::default()).unwrap();
    /// ```
    pub fn search_to_arrow(&self, href: &str, search: Search) -> Result<Vec<RecordBatch>> {
        // TODO can we return an iterator?

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

            if let Some(fields) = search.fields.as_ref() {
                if fields.exclude.contains(&column)
                    || !(fields.include.is_empty() || fields.include.contains(&column))
                {
                    continue;
                }
            }

            if column == "geometry" {
                columns.push("ST_AsWKB(geometry) geometry".to_string());
            } else if DATETIME_COLUMNS.contains(&column.as_str()) {
                columns.push(format!("\"{}\"::TIMESTAMPTZ {}", column, column))
            } else {
                columns.push(format!("\"{}\"", column));
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
                let sql = expr.to_ducksql()?;
                wheres.push(sql);
            } else {
                return Ok(Vec::new());
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
            suffix.push_str(&format!(" LIMIT {}", limit));
        }
        if let Some(offset) = offset {
            suffix.push_str(&format!(" OFFSET {}", offset));
        }

        let sql = format!(
            "SELECT {} FROM {}{}",
            columns.join(","),
            self.format_parquet_href(href),
            suffix,
        );
        log::debug!("duckdb sql: {}", sql);
        let mut statement = self.prepare(&sql)?;
        statement
            .query_arrow(duckdb::params_from_iter(params))?
            .map(|record_batch| {
                let record_batch = if self.convert_wkb {
                    stac::geoarrow::with_native_geometry(record_batch, "geometry")?
                } else {
                    stac::geoarrow::add_wkb_metadata(record_batch, "geometry")?
                };
                Ok(record_batch)
            })
            .collect::<Result<_>>()
    }

    fn format_parquet_href(&self, href: &str) -> String {
        if self.use_hive_partitioning {
            format!(
                "read_parquet('{}', filename=true, hive_partitioning=1)",
                href
            )
        } else {
            format!("read_parquet('{}', filename=true)", href)
        }
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Client;
    use duckdb::Connection;
    use geo::Geometry;
    use rstest::{fixture, rstest};
    use stac::Bbox;
    use stac_api::{Search, Sortby};
    use stac_io::Validate;

    #[fixture]
    #[once]
    fn install_spatial() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute("INSTALL spatial", []).unwrap();
    }

    #[allow(unused_variables)]
    #[fixture]
    fn client(install_spatial: ()) -> Client {
        Client::new().unwrap()
    }

    #[allow(unused_variables)]
    #[rstest]
    fn new(install_spatial: ()) {
        Client::new().unwrap();
    }

    #[rstest]
    fn extensions(client: Client) {
        let _ = client.extensions().unwrap();
    }

    #[rstest]
    fn search(client: Client) {
        let item_collection = client
            .search("data/100-sentinel-2-items.parquet", Search::default())
            .unwrap();
        assert_eq!(item_collection.items.len(), 100);
        item_collection.items[0].validate().unwrap();
    }

    #[rstest]
    fn search_to_arrow(client: Client) {
        let record_batches = client
            .search_to_arrow("data/100-sentinel-2-items.parquet", Search::default())
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
            .unwrap();
        let schema = record_batches[0].schema();
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

    #[rstest]
    fn filter_no_column(client: Client) {
        let mut search = Search::default();
        search.filter = Some("foo:bar = 42".parse().unwrap());
        let item_collection = client
            .search("data/100-sentinel-2-items.parquet", search)
            .unwrap();
        assert_eq!(item_collection.items.len(), 0);
    }

    #[rstest]
    fn sortby_property(client: Client) {
        let mut search = Search::default();
        search.sortby = vec!["eo:cloud_cover".parse().unwrap()];
        let item_collection = client
            .search("data/100-sentinel-2-items.parquet", search)
            .unwrap();
        assert_eq!(item_collection.items.len(), 100);
    }
}
