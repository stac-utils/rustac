// The verbosity stuff is cribbed from https://github.com/clap-rs/clap-verbosity-flag/blob/c621a6a8a7c0b6df8f1464a985a5d076b4915693/src/lib.rs and updated for tracing

#![deny(unused_crate_dependencies)]

use anyhow::{Error, Result, anyhow};
use async_stream::try_stream;
use clap::{CommandFactory, Parser, Subcommand};
use futures_core::TryStream;
use futures_util::{TryStreamExt, pin_mut};
use stac::api::{GetItems, GetSearch, Search};
use stac::{
    Assets, Collection, Item, Links, Migrate, SelfHref,
    geoparquet::{Compression, default_compression},
};
use stac_io::{Format, StacStore};
use stac_server::Backend;
use stac_validate::Validate;
use std::path::Path;
use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    str::FromStr,
};
use tokio::{io::AsyncReadExt, net::TcpListener, task::JoinSet};
use tracing::metadata::Level;
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::{
    fmt::writer::MakeWriterExt, layer::SubscriberExt, util::SubscriberInitExt,
};
use url::Url;

const DEFAULT_COLLECTION_ID: &str = "default-collection-id";

/// rustac: A command-line interface for the SpatioTemporal Asset Catalog (STAC)
#[derive(Debug, Parser)]
pub struct Rustac {
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
    /// Options should be provided in `key=value` pairs, e.g.: `rustac --opt aws_access_key_id=redacted --opt other_value=very_important`
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

    /// Maximum number of rows per row group in parquet files.
    ///
    /// The default is 150,000 rows per group, which is the maximum recommended value for Geoparquet files.
    /// When records are ordered spatially or temporally, lower values result in smaller row groups (better for selective queries),
    /// while higher values result in larger row groups (better compression).
    #[arg(
        long = "parquet-max-row-group-size",
        global = true,
        verbatim_doc_comment
    )]
    parquet_max_row_group_size: Option<usize>,

    #[arg(
        long,
        short = 'v',
        action = clap::ArgAction::Count,
        global = true,
        help = ErrorLevel::verbose_help(),
        long_help = ErrorLevel::verbose_long_help(),
    )]
    verbose: u8,

    #[arg(
        long,
        short = 'q',
        action = clap::ArgAction::Count,
        global = true,
        help = ErrorLevel::quiet_help(),
        long_help = ErrorLevel::quiet_long_help(),
        conflicts_with = "verbose",
    )]
    quiet: u8,
}

/// A rustac subcommand.
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
        /// The href of the STAC API, stac-geoparquet file, or pgstac to search.
        href: String,

        /// The output file.
        ///
        /// To write to standard output, pass `-` or don't provide an argument at all.
        outfile: Option<String>,

        /// The search implementation to use.
        ///
        /// If not provided, the implementation will be inferred from the href:
        /// - `postgresql://` URLs will use the `postgresql` implementation
        /// - `.parquet` or `.geoparquet` files will use the `duckdb` implementation
        /// - All other hrefs will use the `api` implementation
        ///
        /// Possible values:
        /// - api: Search a STAC API endpoint
        /// - duckdb: Search a stac-geoparquet file using DuckDB
        /// - postgresql: Search a pgstac database
        #[arg(long = "search-with", verbatim_doc_comment)]
        search_with: Option<SearchImplementation>,

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

        /// Request headers to include in STAC API Search, as a comma-delimited string.
        ///
        /// Each header should be provided in `KEY=VALUE` format
        /// e.g.: `rustac search  --header "x-my-header=value" --header "x-my-other-header=this"`
        #[arg(long = "header")]
        headers: Vec<String>,
    },

    /// Serves a STAC API.
    Serve {
        /// The hrefs of collections, items, and item collections to load into the API on startup.
        hrefs: Vec<String>,

        /// The address of the server. Defaults to `127.0.0.1:7822`.
        /// Either a URL `https://some-host.io/stac` or a local address like `127.0.0.1:7822`.
        #[arg(short = 'a', long = "addr", default_value = "127.0.0.1:7822")]
        addr: String,

        /// The address to bind the server to, if different from `--addr`.
        #[arg(short = 'b', long = "bind")]
        bind: Option<String>,

        /// The pgstac connection string, e.g. `postgresql://username:password@localhost:5432/postgis`
        ///
        /// If not provided an in-process memory backend will be used.
        #[arg(long = "pgstac")]
        pgstac: Option<String>,

        /// Use DuckDB to serve items from a stac-geoparquet file.
        ///
        /// The server will automatically use DuckDB if the feature is enabled,
        /// `use_duckdb` is `None`, and there is only one `href` that ends in
        /// `parquet`.
        #[arg(long = "use-duckdb")]
        use_duckdb: Option<bool>,

        /// After loading a collection, load all of its item links.
        #[arg(long = "load-collection-items", default_value_t = true)]
        load_collection_items: bool,

        /// Create collections for any items that don't have one.
        #[arg(long, default_value_t = true)]
        create_collections: bool,
    },

    /// Crawls a STAC Catalog or Collection by following its links.
    ///
    /// Items are saved as item collections (in the output format) in the output directory.
    Crawl {
        /// The href of a STAC Catalog or Collection
        href: String,

        /// The output directory
        ///
        /// This doesn't have to be local, by the way.
        directory: String,
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

    /// Generate completion scripts for a given shell.
    GenerateCompletions {
        /// The shell to generate completion scripts for.
        shell: clap_complete::Shell,
    },

    /// Generate a STAC collection from one or more items
    Collection {
        /// The input file.
        ///
        /// To read from standard input, pass `-` or don't provide an argument at all.
        infile: Option<String>,

        /// The output file.
        ///
        /// To write to standard output, pass `-` or don't provide an argument at all.
        outfile: Option<String>,

        /// The id of the output collection
        ///
        /// If not provided, will default to the file name without an extension.
        id: Option<String>,
    },
}

#[derive(Debug)]
#[allow(dead_code, clippy::large_enum_variant)]
enum Value {
    Stac(stac::Value),
    Json(serde_json::Value),
}

/// The search implementation to use.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum SearchImplementation {
    /// Search a STAC API endpoint
    Api,
    /// Search a stac-geoparquet file using DuckDB
    Duckdb,
    /// Search a pgstac database
    Postgresql,
}

#[derive(Debug, Clone)]
struct KeyValue(String, String);

#[derive(Copy, Clone, Debug, Default)]
struct ErrorLevel;

impl Rustac {
    /// Runs this command.
    ///
    /// If `init_tracing_subscriber` is `false`, it is expected that the caller
    /// is setting up the appropriate logging (e.g. Python).
    pub async fn run(self, init_tracing_subscriber: bool) -> Result<()> {
        if init_tracing_subscriber {
            let indicatif_layer = IndicatifLayer::new();
            tracing_subscriber::registry()
                .with(
                    tracing_subscriber::fmt::layer().with_writer(
                        indicatif_layer
                            .get_stderr_writer()
                            .with_max_level(self.log_level().unwrap_or(Level::WARN)),
                    ),
                )
                .with(indicatif_layer)
                .init();
        }
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
                    eprintln!(
                        "WARNING: --to was passed ({to}) without --migrate, value will not be migrated"
                    );
                }
                self.put(outfile.as_deref(), value.into()).await
            }
            Command::Search {
                ref href,
                ref outfile,
                search_with,
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
                ref headers,
            } => {
                // Infer the search implementation from the href if not explicitly provided
                let search_impl = search_with.unwrap_or_else(|| {
                    if href.starts_with("postgresql://") {
                        SearchImplementation::Postgresql
                    } else if matches!(Format::infer_from_href(href), Some(Format::Geoparquet(_))) {
                        SearchImplementation::Duckdb
                    } else {
                        SearchImplementation::Api
                    }
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
                let search = search.normalize_datetimes()?;
                let item_collection = match search_impl {
                    SearchImplementation::Postgresql => {
                        #[cfg(feature = "pgstac")]
                        {
                            pgstac::search(href, search, *max_items).await?
                        }
                        #[cfg(not(feature = "pgstac"))]
                        {
                            return Err(anyhow!("rustac is not compiled with pgstac support"));
                        }
                    }
                    SearchImplementation::Duckdb => stac_duckdb::search(href, search, *max_items)?,
                    SearchImplementation::Api => {
                        let h: Vec<(String, String)> = headers
                            .iter()
                            .map(|s| {
                                s.split_once('=')
                                    .map(|(k, v)| (k.to_string(), v.to_string()))
                                    .ok_or_else(|| {
                                        anyhow!(
                                            "invalid header '{}', expected format KEY=VALUE",
                                            s
                                        )
                                    })
                            })
                            .collect::<Result<Vec<_>>>()?;
                        stac_io::api::search(href, search, *max_items, &h).await?
                    }
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
                ref bind,
                ref pgstac,
                use_duckdb,
                load_collection_items,
                create_collections,
            } => {
                let bind = bind.as_deref().unwrap_or(&addr);
                if matches!(use_duckdb, Some(true))
                    || (use_duckdb.is_none() && hrefs.len() == 1 && hrefs[0].ends_with("parquet"))
                {
                    let backend = stac_server::DuckdbBackend::new(&hrefs[0]).await?;
                    eprintln!("Backend: duckdb");
                    return load_and_serve(
                        bind,
                        addr,
                        backend,
                        Vec::new(),
                        HashMap::new(),
                        create_collections,
                    )
                    .await;
                }
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
                                    items.entry(String::new()).or_default().push(item);
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
                        eprintln!("Backend: pgstac");
                        load_and_serve(bind, addr, backend, collections, items, create_collections)
                            .await
                    }
                    #[cfg(not(feature = "pgstac"))]
                    {
                        Err(anyhow!("rustac is not compiled with pgstac support"))
                    }
                } else {
                    let backend = stac_server::MemoryBackend::new();
                    eprintln!("Backend: memory");
                    load_and_serve(bind, addr, backend, collections, items, create_collections)
                        .await
                }
            }
            Command::Crawl {
                ref href,
                ref directory,
            } => {
                let opts = self.opts();
                let (store, path) = stac_io::parse_href_opts(href.clone(), opts.clone())?;
                let value: stac::Value = store.get(path).await.unwrap();
                let mut items: HashMap<Option<String>, Vec<Item>> = HashMap::new();
                let crawl = crawl(value, store).await;
                pin_mut!(crawl);
                let mut warned = false;
                while let Some(item) = crawl.try_next().await? {
                    let collection = item.collection.clone();
                    if collection.as_deref() == Some(DEFAULT_COLLECTION_ID) && !warned {
                        warned = true;
                        tracing::warn!(
                            "collection id matches the default collection id, so any collection-less items will be grouped into this collection: {DEFAULT_COLLECTION_ID}"
                        )
                    }
                    items.entry(collection).or_default().push(item);
                }
                let (store, path) = stac_io::parse_href_opts(directory.clone(), opts)?;
                let format = self.output_format(None);
                for (collection, items) in items {
                    let file_name = format!(
                        "{}.{}",
                        collection.as_deref().unwrap_or(DEFAULT_COLLECTION_ID),
                        format.extension()
                    );
                    store
                        .put_format(
                            path.child(file_name),
                            stac::ItemCollection::from(items),
                            format,
                        )
                        .await?;
                }
                Ok(())
            }
            Command::Validate { ref infile } => {
                let value = self.get(infile.as_deref()).await?;
                let result = value.validate().await;
                if let Err(error) = result {
                    if let stac_validate::Error::Validation(errors) = error {
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
                                println!("{error}");
                            }
                        }
                    }
                    std::io::stdout().flush()?;
                    Err(anyhow!("one or more validation errors"))
                } else {
                    Ok(())
                }
            }
            Command::GenerateCompletions { shell } => {
                let mut command = Rustac::command();
                clap_complete::generate(shell, &mut command, "rustac", &mut std::io::stdout());
                Ok(())
            }
            Command::Collection {
                ref infile,
                ref outfile,
                ref id,
            } => {
                let value = self.get(infile.as_deref()).await?;
                let id = id.clone().unwrap_or_else(|| {
                    infile
                        .as_deref()
                        .and_then(|infile| {
                            Path::new(infile)
                                .file_stem()
                                .map(|s| s.to_string_lossy().into_owned())
                        })
                        .unwrap_or("default-collection-id".to_string())
                });
                let collection = match value {
                    stac::Value::ItemCollection(item_collection) => {
                        Collection::from_id_and_items(id, &item_collection.items)
                    }
                    stac::Value::Item(item) => Collection::from_id_and_items(id, &[item]),
                    stac::Value::Collection(collection) => collection,
                    stac::Value::Catalog(catalog) => {
                        let mut json = serde_json::to_value(catalog)?;
                        let _ = json
                            .as_object_mut()
                            .unwrap()
                            .insert("type".to_string(), "Collection".into());
                        serde_json::from_value(json)?
                    }
                };
                self.put(
                    outfile.as_deref(),
                    Value::Stac(stac::Value::Collection(collection)),
                )
                .await?;
                Ok(())
            }
        }
    }

    async fn get(&self, href: Option<&str>) -> Result<stac::Value> {
        let href = href.and_then(|s| if s == "-" { None } else { Some(s) });
        let format = self.input_format(href);
        if let Some(href) = href {
            let (store, path) = stac_io::parse_href_opts(href, self.opts())?;
            let value: stac::Value = store.get_format(path, format).await?;
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
            let (store, path) = stac_io::parse_href_opts(href, self.opts())?;
            let _ = match value {
                Value::Json(json) => store.put_format(path, json, format).await?,
                Value::Stac(stac) => store.put_format(path, stac, format).await?,
            };
            Ok(())
        } else {
            let mut bytes = match value {
                Value::Json(json) => format.into_vec(json)?,
                Value::Stac(stac) => format.into_vec(stac)?,
            };
            // TODO allow disabling trailing newline
            if !matches!(format, Format::NdJson) {
                bytes.push(b'\n');
            }
            std::io::stdout().write_all(&bytes)?;
            Ok(())
        }
    }

    pub fn log_level(&self) -> Option<Level> {
        level_enum(self.verbosity())
    }

    fn verbosity(&self) -> i8 {
        level_value(ErrorLevel::default()) - (self.quiet as i8) + (self.verbose as i8)
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
            use stac::geoparquet::WriterOptions;

            let mut writer_options = WriterOptions::new()
                .with_compression(self.parquet_compression.or(Some(default_compression())));

            if let Some(max_row_group_size) = self.parquet_max_row_group_size {
                writer_options = writer_options.with_max_row_group_size(max_row_group_size);
            }

            Format::Geoparquet(writer_options)
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

impl ErrorLevel {
    fn default() -> Option<Level> {
        Some(Level::ERROR)
    }

    fn verbose_help() -> Option<&'static str> {
        Some("Increase verbosity")
    }

    fn verbose_long_help() -> Option<&'static str> {
        None
    }

    fn quiet_help() -> Option<&'static str> {
        Some("Decrease verbosity")
    }

    fn quiet_long_help() -> Option<&'static str> {
        None
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
    bind: &str,
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
        for (mut collection_id, mut items) in items {
            if collection_id.is_empty() {
                if backend.collection(DEFAULT_COLLECTION_ID).await?.is_some() {
                    return Err(anyhow!(
                        "cannot auto-create collections, a collection already exists with id={DEFAULT_COLLECTION_ID}"
                    ));
                } else {
                    collection_id = DEFAULT_COLLECTION_ID.to_string();
                }
            }
            for item in &mut items {
                item.collection = Some(collection_id.to_string());
            }
            let collection = Collection::from_id_and_items(collection_id, &items);
            backend.add_collection(collection).await?;
            backend.add_items(items).await?;
        }
    } else if !items.is_empty() {
        return Err(anyhow!(
            "items don't have a collection and `create_collections` is false"
        ));
    }

    let root = Url::parse(addr)
        .map(|url| url.to_string())
        .unwrap_or(format!("http://{addr}"));
    let api = stac_server::Api::new(backend, &root)?;
    let router = stac_server::routes::from_api(api);
    let listener = TcpListener::bind(&bind).await?;
    eprintln!("Serving a STAC API at {root}");
    axum::serve(listener, router).await.map_err(Error::from)
}

fn level_enum(verbosity: i8) -> Option<Level> {
    match verbosity {
        i8::MIN..=-1 => None,
        0 => Some(Level::ERROR),
        1 => Some(Level::WARN),
        2 => Some(Level::INFO),
        3 => Some(Level::DEBUG),
        4..=i8::MAX => Some(Level::TRACE),
    }
}

fn level_value(level: Option<Level>) -> i8 {
    match level {
        None => -1,
        Some(Level::ERROR) => 0,
        Some(Level::WARN) => 1,
        Some(Level::INFO) => 2,
        Some(Level::DEBUG) => 3,
        Some(Level::TRACE) => 4,
    }
}

async fn crawl(value: stac::Value, store: StacStore) -> impl TryStream<Item = Result<Item>> {
    use stac::Value::*;

    try_stream! {
        let mut values = VecDeque::from([value]);
        while let Some(mut value) = values.pop_front() {
            value.make_links_absolute()?;
            match value {
                Catalog(_) | Collection(_) => {
                    if let Catalog(ref catalog) = value {
                        tracing::info!("got catalog={}", catalog.id);
                    }
                    if let Collection(ref collection) = value {
                        tracing::info!("got collection={}", collection.id);
                    }
                    let mut join_set: JoinSet<Result<stac::Value>> = JoinSet::new();
                    for link in value
                        .links()
                        .iter()
                        .filter(|link| link.is_child() || link.is_item())
                        .cloned()
                    {
                        let store = store.clone();
                        let url = Url::parse(&link.href)?;
                        join_set.spawn(async move {
                            let value: stac::Value = store.get(url.path()).await?;
                            Ok(value)
                        });
                    }
                    while let Some(result) = join_set.join_next().await {
                        let value = result??;
                        values.push_back(value);
                    }
                }
                Item(mut item) => {
                    if let Some(self_href) = item.self_href() {
                        let self_href=  self_href.to_string();
                        item.make_assets_absolute(&self_href)?;
                    }
                    yield item;
                }
                ItemCollection(item_collection) => {
                    for mut item in item_collection.items {
                        if let Some(self_href) = item.self_href() {
                            let self_href = self_href.to_string();
                            item.make_assets_absolute(&self_href)?;
                        }
                        yield item;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
use {assert_cmd as _, rstest as _, tempfile as _};
