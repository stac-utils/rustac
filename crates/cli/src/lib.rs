use anyhow::{anyhow, Error, Result};
use clap::{Parser, Subcommand};
use duckdb as _;
use stac::{geoparquet::Compression, Collection, Format, Item, Links, Migrate, Validate};
use stac_api::{GetItems, GetSearch, Search};
use stac_server::Backend;
use std::{collections::HashMap, io::Write, str::FromStr};
use tokio::{io::AsyncReadExt, net::TcpListener, runtime::Handle};

/// stacrs: A command-line interface for the SpatioTemporal Asset Catalog (STAC)
#[derive(Debug, Parser)]
pub struct Stacrs {
    #[command(subcommand)]
    command: Command,

    /// The input format.
    ///
    /// If not provided, the format will be inferred from the file extension.
    /// Possible values (default: json):
    ///
    /// - json
    /// - ndjson (newline-delimited json)
    /// - parquet (stac-geoparquet)
    #[arg(
        short = 'i',
        long = "input-format",
        global = true,
        verbatim_doc_comment
    )]
    input_format: Option<Format>,

    /// Options for getting and putting files from object storage.
    ///
    /// Options should be provided in `key=value` pairs, e.g.: `stacrs --opt aws_access_key_id=redacted --opt other_value=very_important`
    #[arg(long = "opt", global = true, verbatim_doc_comment)]
    options: Vec<KeyValue>,

    /// The output format.
    ///
    /// If not provided, the format will be inferred from the file extension.
    /// Possible values (default: json):
    ///
    /// - json
    /// - ndjson (newline-delimited json)
    /// - parquet (stac-geoparquet)
    #[arg(
        short = 'o',
        long = "output-format",
        global = true,
        verbatim_doc_comment
    )]
    output_format: Option<Format>,

    /// Whether to print compact JSON output.
    ///
    /// By default, JSON output will printed "compact" if it is being output to a file, and printed "pretty" if it is being output to standard output.
    /// Use this argument to force one or the other.
    #[arg(short = 'c', long = "compact-json", global = true)]
    compact_json: Option<bool>,

    /// The parquet compression to use when writing stac-geoparquet.
    ///
    /// Possible values (default: snappy):
    ///
    /// - uncompressed: No compression
    /// - snappy:       Snappy compression (<https://en.wikipedia.org/wiki/Snappy_(compression)>)
    /// - gzip(n):      Gzip compression (<https://www.ietf.org/rfc/rfc1952.txt>)
    /// - lzo:          LZO compression (<https://en.wikipedia.org/wiki/Lempel%E2%80%93Ziv%E2%80%93Oberhumer>)
    /// - brotli(n):    Brotli compression (<https://datatracker.ietf.org/doc/html/rfc7932>)
    /// - lz4:          LZ4 compression (<https://lz4.org/>), [(deprecated)](https://issues.apache.org/jira/browse/PARQUET-2032)
    /// - zstd(n):      ZSTD compression (<https://datatracker.ietf.org/doc/html/rfc8878>)
    /// - lz4-raw:      LZ4 compression (<https://lz4.org/>)
    ///
    /// Some of the compression values have a level, specified as `(n)`. This level should be an integer.
    #[arg(long = "parquet-compression", global = true, verbatim_doc_comment)]
    parquet_compression: Option<Compression>,
}

/// A stacrs subcommand.
#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    /// Translates STAC from one format to another.
    Translate {
        /// The input file.
        ///
        /// To read from standard input, pass `-` or don't provide an argument at all.
        infile: Option<String>,

        /// The output file.
        ///
        /// To write to standard output, pass `-` or don't provide an argument at all.
        outfile: Option<String>,

        /// Migrate this STAC value to another version.
        ///
        /// By default, will migrate to the latest supported version. Use `--to`
        /// to specify a different STAC version.
        #[arg(long = "migrate", default_value_t = false)]
        migrate: bool,

        /// Migrate to this STAC version.
        ///
        /// If not provided, will migrate to the latest supported version. Will
        /// only be used if `--migrate` is passed.
        #[arg(long = "to")]
        to: Option<String>,
    },

    /// Searches a STAC API or stac-geoparquet file.
    Search {
        /// The href of the STAC API or stac-geoparquet file to search.
        href: String,

        /// The output file.
        ///
        /// To write to standard output, pass `-` or don't provide an argument at all.
        outfile: Option<String>,

        /// Use DuckDB to query the href.
        ///
        /// By default, DuckDB will be used if the href ends in `parquet` or
        /// `geoparquet`. Set this value to `true` to force DuckDB to be used,
        /// or to `false` to disable this behavior.
        #[arg(long = "use-duckdb")]
        use_duckdb: Option<bool>,

        /// The maximum number of items to return from the search.
        #[arg(short = 'n', long = "max-items")]
        max_items: Option<usize>,

        /// Searches items by performing intersection between their geometry and provided GeoJSON geometry.
        ///
        /// All GeoJSON geometry types must be supported.
        #[arg(long = "intersects")]
        intersects: Option<String>,

        /// Comma-delimited list of Item ids to return.
        #[arg(long = "ids")]
        ids: Option<String>,

        /// Comma-delimited list of one or more Collection IDs that each matching Item must be in.
        #[arg(long = "collections")]
        collections: Option<String>,

        /// Requested bounding box, as a comma-delimited string.
        #[arg(long = "bbox")]
        bbox: Option<String>,

        /// Single date+time, or a range ('/' separator), formatted to [RFC 3339,
        /// section 5.6](https://tools.ietf.org/html/rfc3339#section-5.6).
        ///
        /// Use double dots `..` for open date ranges.
        #[arg(long = "datetime")]
        datetime: Option<String>,

        /// Include/exclude fields from item collections, as a comma-delimited string.
        #[arg(long = "fields")]
        fields: Option<String>,

        /// Fields by which to sort results, as a comma-delimited string.
        #[arg(long = "sortby")]
        sortby: Option<String>,

        /// CQL2 filter expression.
        #[arg(long = "filter")]
        filter: Option<String>,

        /// The page size to be returned from the server.
        #[arg(long = "limit")]
        limit: Option<String>,
    },

    /// Serves a STAC API.
    Serve {
        /// The hrefs of collections, items, and item collections to load into the API on startup.
        hrefs: Vec<String>,

        /// The address of the server.
        #[arg(short = 'a', long = "addr", default_value = "127.0.0.1:7822")]
        addr: String,

        /// The pgstac connection string, e.g. `postgresql://username:password@localhost:5432/postgis`
        ///
        /// If not provided an in-process memory backend will be used.
        #[arg(long = "pgstac")]
        pgstac: Option<String>,

        /// After loading a collection, load all of its item links.
        #[arg(long = "load-collection-items", default_value_t = true)]
        load_collection_items: bool,

        /// Create collections for any items that don't have one.
        #[arg(long, default_value_t = true)]
        create_collections: bool,
    },

    /// Validates a STAC value.
    ///
    /// The default output format is plain text — use `--output-format=json` to
    /// get structured output.
    Validate {
        /// The input file.
        ///
        /// To read from standard input, pass `-` or don't provide an argument at all.
        infile: Option<String>,
    },
}

#[derive(Debug)]
#[allow(dead_code, clippy::large_enum_variant)]
enum Value {
    Stac(stac::Value),
    Json(serde_json::Value),
}

#[derive(Debug, Clone)]
struct KeyValue(String, String);

impl Stacrs {
    /// Runs this command.
    pub async fn run(self) -> Result<()> {
        match self.command {
            Command::Translate {
                ref infile,
                ref outfile,
                migrate,
                ref to,
            } => {
                let mut value = self.get(infile.as_deref()).await?;
                if migrate {
                    value = value.migrate(
                        &to.as_deref()
                            .map(|s| s.parse().unwrap())
                            .unwrap_or_default(),
                    )?;
                } else if let Some(to) = to {
                    eprintln!("WARNING: --to was passed ({to}) without --migrate, value will not be migrated");
                }
                self.put(outfile.as_deref(), value.into()).await
            }
            Command::Search {
                ref href,
                ref outfile,
                ref use_duckdb,
                ref max_items,
                ref intersects,
                ref ids,
                ref collections,
                ref bbox,
                ref datetime,
                ref fields,
                ref sortby,
                ref filter,
                ref limit,
            } => {
                let use_duckdb = use_duckdb.unwrap_or_else(|| {
                    matches!(Format::infer_from_href(href), Some(Format::Geoparquet(_)))
                });
                let get_items = GetItems {
                    bbox: bbox.clone(),
                    datetime: datetime.clone(),
                    fields: fields.clone(),
                    sortby: sortby.clone(),
                    filter: filter.clone(),
                    limit: limit.clone(),
                    ..Default::default()
                };
                let get_search = GetSearch {
                    intersects: intersects.clone(),
                    ids: ids.clone(),
                    collections: collections.clone(),
                    items: get_items,
                };
                let search: Search = get_search.try_into()?;
                let item_collection = if use_duckdb {
                    stac_duckdb::search(href, search, *max_items)?
                } else {
                    stac_api::client::search(href, search, *max_items).await?
                };
                self.put(
                    outfile.as_deref(),
                    serde_json::to_value(item_collection)?.into(),
                )
                .await
            }
            Command::Serve {
                ref hrefs,
                ref addr,
                ref pgstac,
                load_collection_items,
                create_collections,
            } => {
                let mut collections = Vec::new();
                let mut items: HashMap<String, Vec<stac::Item>> = HashMap::new();
                for href in hrefs {
                    let value = self.get(Some(href.as_str())).await?;
                    match value {
                        stac::Value::Collection(collection) => {
                            if load_collection_items {
                                for link in collection.iter_item_links() {
                                    let value = self.get(Some(link.href.as_str())).await?;
                                    if let stac::Value::Item(item) = value {
                                        items.entry(collection.id.clone()).or_default().push(item);
                                    } else {
                                        return Err(anyhow!(
                                            "item link was not an item: {value:?}"
                                        ));
                                    }
                                }
                            }
                            collections.push(collection);
                        }
                        stac::Value::ItemCollection(item_collection) => {
                            for item in item_collection.items {
                                if let Some(collection) = item.collection.clone() {
                                    items.entry(collection).or_default().push(item);
                                } else {
                                    return Err(anyhow!("item without a collection: {item:?}"));
                                }
                            }
                        }
                        stac::Value::Item(item) => {
                            if let Some(collection) = item.collection.clone() {
                                items.entry(collection).or_default().push(item);
                            } else {
                                return Err(anyhow!("item without a collection: {item:?}"));
                            }
                        }
                        _ => return Err(anyhow!("don't know how to load value: {value:?}")),
                    }
                }

                #[allow(unused_variables)]
                if let Some(pgstac) = pgstac {
                    #[cfg(feature = "pgstac")]
                    {
                        let backend =
                            stac_server::PgstacBackend::new_from_stringlike(pgstac).await?;
                        load_and_serve(addr, backend, collections, items, create_collections).await
                    }
                    #[cfg(not(feature = "pgstac"))]
                    {
                        return Err(anyhow!("stacrs is not compiled with pgstac support"));
                    }
                } else {
                    let backend = stac_server::MemoryBackend::new();
                    load_and_serve(addr, backend, collections, items, create_collections).await
                }
            }
            Command::Validate { ref infile } => {
                let value = self.get(infile.as_deref()).await?;
                let result = Handle::current()
                    .spawn_blocking(move || value.validate())
                    .await?;
                if let Err(error) = result {
                    if let stac::Error::Validation(errors) = error {
                        if let Some(format) = self.output_format {
                            if let Format::Json(_) = format {
                                let value = errors
                                    .into_iter()
                                    .map(|error| error.into_json())
                                    .collect::<Vec<_>>();
                                if self.compact_json.unwrap_or_default() {
                                    serde_json::to_writer(std::io::stdout(), &value)?;
                                } else {
                                    serde_json::to_writer_pretty(std::io::stdout(), &value)?;
                                }
                                println!();
                            } else {
                                return Err(anyhow!("invalid output format: {}", format));
                            }
                        } else {
                            for error in errors {
                                println!("{}", error);
                            }
                        }
                    }
                    std::io::stdout().flush()?;
                    Err(anyhow!("one or more validation errors"))
                } else {
                    Ok(())
                }
            }
        }
    }

    async fn get(&self, href: Option<&str>) -> Result<stac::Value> {
        let href = href.and_then(|s| if s == "-" { None } else { Some(s) });
        let format = self.input_format(href);
        if let Some(href) = href {
            let value: stac::Value = format.get_opts(href, self.opts()).await?;
            Ok(value)
        } else {
            let mut buf = Vec::new();
            let _ = tokio::io::stdin().read_to_end(&mut buf).await?;
            let value: stac::Value = format.from_bytes(buf)?;
            Ok(value)
        }
    }

    async fn put(&self, href: Option<&str>, value: Value) -> Result<()> {
        let href = href.and_then(|s| if s == "-" { None } else { Some(s) });
        let format = self.output_format(href);
        if let Some(href) = href {
            let opts = self.opts();
            let _ = match value {
                Value::Json(json) => format.put_opts(href, json, opts).await?,
                Value::Stac(stac) => format.put_opts(href, stac, opts).await?,
            };
            Ok(())
        } else {
            let mut bytes = match value {
                Value::Json(json) => format.into_vec(json)?,
                Value::Stac(stac) => format.into_vec(stac)?,
            };
            // TODO allow disabling trailing newline
            bytes.push(b'\n');
            std::io::stdout().write_all(&bytes)?;
            Ok(())
        }
    }

    /// Returns the set or inferred input format.
    pub fn input_format(&self, href: Option<&str>) -> Format {
        if let Some(input_format) = self.input_format {
            input_format
        } else if let Some(href) = href {
            Format::infer_from_href(href).unwrap_or_default()
        } else {
            Format::json()
        }
    }

    /// Returns the set or inferred input format.
    pub fn output_format(&self, href: Option<&str>) -> Format {
        let format = if let Some(format) = self.output_format {
            format
        } else if let Some(href) = href {
            Format::infer_from_href(href).unwrap_or_default()
        } else {
            Format::Json(true)
        };
        if matches!(format, Format::Geoparquet(_)) {
            Format::Geoparquet(self.parquet_compression.or(Some(Compression::SNAPPY)))
        } else if let Format::Json(pretty) = format {
            Format::Json(self.compact_json.map(|c| !c).unwrap_or(pretty))
        } else {
            format
        }
    }

    fn opts(&self) -> Vec<(String, String)> {
        self.options
            .iter()
            .cloned()
            .map(|kv| (kv.0, kv.1))
            .collect()
    }
}

impl From<stac::Value> for Value {
    fn from(value: stac::Value) -> Self {
        Value::Stac(value)
    }
}

impl From<serde_json::Value> for Value {
    fn from(value: serde_json::Value) -> Self {
        Value::Json(value)
    }
}

impl FromStr for KeyValue {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        if let Some((key, value)) = s.split_once('=') {
            Ok(KeyValue(key.to_string(), value.to_string()))
        } else {
            Err(anyhow!("invalid key=value: {s}"))
        }
    }
}

async fn load_and_serve(
    addr: &str,
    mut backend: impl Backend,
    collections: Vec<Collection>,
    mut items: HashMap<String, Vec<Item>>,
    create_collections: bool,
) -> Result<()> {
    for collection in collections {
        let items = items.remove(&collection.id);
        backend.add_collection(collection).await?;
        if let Some(items) = items {
            backend.add_items(items).await?;
        }
    }
    if create_collections {
        for (collection_id, items) in items {
            let collection = Collection::from_id_and_items(collection_id, &items);
            backend.add_collection(collection).await?;
            backend.add_items(items).await?;
        }
    } else if !items.is_empty() {
        return Err(anyhow!(
            "items don't have a collection and `create_collections` is false"
        ));
    }
    let root = format!("http://{}", addr);
    let api = stac_server::Api::new(backend, &root)?;
    let router = stac_server::routes::from_api(api);
    let listener = TcpListener::bind(&addr).await?;
    eprintln!("Serving a STAC API at {}", root);
    axum::serve(listener, router).await.map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use super::Stacrs;
    use assert_cmd::Command;
    use clap::Parser;
    use rstest::{fixture, rstest};
    use stac::{geoparquet::Compression, Format};

    #[fixture]
    fn command() -> Command {
        Command::cargo_bin("stacrs").unwrap()
    }

    #[rstest]
    fn translate_json(mut command: Command) {
        command
            .arg("translate")
            .arg("examples/simple-item.json")
            .assert()
            .success();
    }

    #[rstest]
    fn migrate(mut command: Command) {
        command
            .arg("translate")
            .arg("../../spec-examples/v1.0.0/simple-item.json")
            .arg("--migrate")
            .assert()
            .success();
    }

    #[test]
    fn input_format() {
        let stacrs = Stacrs::parse_from(["stacrs", "translate"]);
        assert_eq!(stacrs.input_format(None), Format::Json(false));

        let stacrs = Stacrs::parse_from(["stacrs", "translate"]);
        assert_eq!(stacrs.input_format(Some("file.json")), Format::Json(false));

        let stacrs = Stacrs::parse_from(["stacrs", "translate"]);
        assert_eq!(stacrs.input_format(Some("file.ndjson")), Format::NdJson);

        let stacrs = Stacrs::parse_from(["stacrs", "translate"]);
        assert_eq!(
            stacrs.input_format(Some("file.parquet")),
            Format::Geoparquet(None)
        );

        let stacrs = Stacrs::parse_from(["stacrs", "--input-format", "json", "translate"]);
        assert_eq!(stacrs.input_format(None), Format::Json(false));

        let stacrs = Stacrs::parse_from(["stacrs", "--input-format", "ndjson", "translate"]);
        assert_eq!(stacrs.input_format(None), Format::NdJson);

        let stacrs = Stacrs::parse_from(["stacrs", "--input-format", "parquet", "translate"]);
        assert_eq!(stacrs.input_format(None), Format::Geoparquet(None));

        let stacrs = Stacrs::parse_from([
            "stacrs",
            "--input-format",
            "json",
            "--compact-json",
            "false",
            "translate",
        ]);
        assert_eq!(stacrs.input_format(None), Format::Json(false));
    }

    #[test]
    fn output_format() {
        let stacrs = Stacrs::parse_from(["stacrs", "translate"]);
        assert_eq!(stacrs.output_format(None), Format::Json(true));

        let stacrs = Stacrs::parse_from(["stacrs", "translate"]);
        assert_eq!(stacrs.output_format(Some("file.json")), Format::Json(false));

        let stacrs = Stacrs::parse_from(["stacrs", "translate"]);
        assert_eq!(stacrs.output_format(Some("file.ndjson")), Format::NdJson);

        let stacrs = Stacrs::parse_from(["stacrs", "translate"]);
        assert_eq!(
            stacrs.output_format(Some("file.parquet")),
            Format::Geoparquet(Some(Compression::SNAPPY))
        );

        let stacrs = Stacrs::parse_from(["stacrs", "--output-format", "json", "translate"]);
        assert_eq!(stacrs.output_format(None), Format::Json(false));

        let stacrs = Stacrs::parse_from(["stacrs", "--output-format", "ndjson", "translate"]);
        assert_eq!(stacrs.output_format(None), Format::NdJson);

        let stacrs = Stacrs::parse_from(["stacrs", "--output-format", "parquet", "translate"]);
        assert_eq!(
            stacrs.output_format(None),
            Format::Geoparquet(Some(Compression::SNAPPY))
        );

        let stacrs = Stacrs::parse_from([
            "stacrs",
            "--output-format",
            "json",
            "--compact-json",
            "false",
            "translate",
        ]);
        assert_eq!(stacrs.output_format(None), Format::Json(true));

        let stacrs = Stacrs::parse_from([
            "stacrs",
            "--output-format",
            "parquet",
            "--parquet-compression",
            "lzo",
            "translate",
        ]);
        assert_eq!(
            stacrs.output_format(None),
            Format::Geoparquet(Some(Compression::LZO))
        );
    }

    #[rstest]
    fn validate(mut command: Command) {
        command
            .arg("validate")
            .arg("examples/simple-item.json")
            .assert()
            .success();
        command
            .arg("validate")
            .arg("data/invalid-item.json")
            .assert()
            .failure();
    }
}
