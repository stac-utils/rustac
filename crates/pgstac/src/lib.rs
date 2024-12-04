//! Rust interface for [pgstac](https://github.com/stac-utils/pgstac).
//!
//! # Examples
//!
//! [Pgstac] is a trait to query a **pgstac** database.
//! It is implemented for anything that implements [tokio_postgres::GenericClient]:
//!
//! ```no_run
//! use pgstac::Pgstac;
//! use tokio_postgres::NoTls;
//!
//! # tokio_test::block_on(async {
//! let config = "postgresql://username:password@localhost:5432/postgis";
//! let (client, connection) = tokio_postgres::connect(config, NoTls).await.unwrap();
//! tokio::spawn(async move {
//!     if let Err(e) = connection.await {
//!      eprintln!("connection error: {}", e);
//!     }
//! });
//! println!("{}", client.pgstac_version().await.unwrap());
//! # })
//! ```
//!
//! If you want to work in a transaction, you can do that too:
//!
//! ```no_run
//! use pgstac::Pgstac;
//! use stac::Collection;
//! use tokio_postgres::NoTls;
//!
//! # tokio_test::block_on(async {
//! let config = "postgresql://username:password@localhost:5432/postgis";
//! let (mut client, connection) = tokio_postgres::connect(config, NoTls).await.unwrap();
//! tokio::spawn(async move {
//!     if let Err(e) = connection.await {
//!      eprintln!("connection error: {}", e);
//!     }
//! });
//! let transaction = client.transaction().await.unwrap();
//! transaction.add_collection(Collection::new("an-id", "a description")).await.unwrap();
//! transaction.commit().await.unwrap();
//! # })
//! ```
//!
//! # Features
//!
//! - `tls`: provide a function to create an unverified tls provider, which can be useful in some circumstances (see <https://github.com/stac-utils/stac-rs/issues/375>)

#![deny(
    elided_lifetimes_in_paths,
    explicit_outlives_requirements,
    keyword_idents,
    macro_use_extern_crate,
    meta_variable_misuse,
    missing_abi,
    missing_debug_implementations,
    non_ascii_idents,
    noop_method_call,
    rust_2021_incompatible_closure_captures,
    rust_2021_incompatible_or_patterns,
    rust_2021_prefixes_incompatible_syntax,
    rust_2021_prelude_collisions,
    single_use_lifetimes,
    trivial_casts,
    trivial_numeric_casts,
    unreachable_pub,
    unsafe_code,
    unsafe_op_in_unsafe_fn,
    unused_crate_dependencies,
    unused_extern_crates,
    unused_import_braces,
    unused_lifetimes,
    unused_qualifications,
    unused_results
)]
#![warn(missing_docs)]

mod page;
#[cfg(feature = "tls")]
mod tls;

pub use page::Page;
use serde::{de::DeserializeOwned, Serialize};
use tokio_postgres::{types::ToSql, GenericClient, Row};
#[cfg(feature = "tls")]
pub use {tls::make_unverified_tls, tokio_postgres_rustls::MakeRustlsConnect};

/// Crate-specific error enum.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// [serde_json::Error]
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    /// [tokio_postgres::Error]
    #[error(transparent)]
    TokioPostgres(#[from] tokio_postgres::Error),
}

/// Crate-specific result type.
pub type Result<T> = std::result::Result<T, Error>;

/// A [serde_json::Value].
pub type JsonValue = serde_json::Value;

/// Methods for working with **pgstac**.
#[allow(async_fn_in_trait)]
pub trait Pgstac: GenericClient {
    /// Returns the **pgstac** version.
    async fn pgstac_version(&self) -> Result<String> {
        self.string("get_version", &[]).await
    }

    /// Returns the value of the `context` **pgstac** setting.
    ///
    /// This setting defaults to "off".  See [the **pgstac**
    /// docs](https://github.com/stac-utils/pgstac/blob/main/docs/src/pgstac.md#pgstac-settings)
    /// for more information on the settings and their meaning.
    async fn context(&self) -> Result<bool> {
        self.string("get_setting", &[&"context"])
            .await
            .map(|value| value == "on")
    }

    /// Sets the value of the `context` **pgstac** setting.
    ///
    /// This setting defaults to "off".  See [the **pgstac**
    /// docs](https://github.com/stac-utils/pgstac/blob/main/docs/src/pgstac.md#pgstac-settings)
    /// for more information on the settings and their meaning.
    async fn set_context(&self, enable: bool) -> Result<()> {
        let value = if enable { "on" } else { "off" };
        self.execute(
            "INSERT INTO pgstac_settings (name, value) VALUES ('context', $1) ON CONFLICT ON CONSTRAINT pgstac_settings_pkey DO UPDATE SET value = excluded.value;",
            &[&value],
        ).await.map(|_| ()).map_err(Error::from)
    }

    /// Fetches all collections.
    async fn collections(&self) -> Result<Vec<JsonValue>> {
        self.vec("all_collections", &[]).await
    }

    /// Fetches a collection by id.
    async fn collection(&self, id: &str) -> Result<Option<JsonValue>> {
        self.opt("get_collection", &[&id]).await
    }

    /// Adds a collection.
    async fn add_collection<T>(&self, collection: T) -> Result<()>
    where
        T: Serialize,
    {
        let collection = serde_json::to_value(collection)?;
        self.void("create_collection", &[&collection]).await
    }

    /// Adds or updates a collection.
    async fn upsert_collection<T>(&self, collection: T) -> Result<()>
    where
        T: Serialize,
    {
        let collection = serde_json::to_value(collection)?;
        self.void("upsert_collection", &[&collection]).await
    }

    /// Updates a collection.
    async fn update_collection<T>(&self, collection: T) -> Result<()>
    where
        T: Serialize,
    {
        let collection = serde_json::to_value(collection)?;
        self.void("update_collection", &[&collection]).await
    }

    /// Deletes a collection.
    async fn delete_collection(&self, id: &str) -> Result<()> {
        self.void("delete_collection", &[&id]).await
    }

    /// Fetches an item.
    async fn item(&self, id: &str, collection: &str) -> Result<Option<JsonValue>> {
        self.opt("get_item", &[&id, &collection]).await
    }

    /// Adds an item.
    async fn add_item<T>(&self, item: T) -> Result<()>
    where
        T: Serialize,
    {
        let item = serde_json::to_value(item)?;
        self.void("create_item", &[&item]).await
    }

    /// Adds items.
    async fn add_items<T>(&self, items: &[T]) -> Result<()>
    where
        T: Serialize,
    {
        let items = serde_json::to_value(items)?;
        self.void("create_items", &[&items]).await
    }

    /// Updates an item.
    async fn update_item<T>(&self, item: T) -> Result<()>
    where
        T: Serialize,
    {
        let item = serde_json::to_value(item)?;
        self.void("update_item", &[&item]).await
    }

    /// Upserts an item.
    async fn upsert_item<T>(&self, item: T) -> Result<()>
    where
        T: Serialize,
    {
        let item = serde_json::to_value(item)?;
        self.void("upsert_item", &[&item]).await
    }

    /// Upserts items.
    ///
    /// To avoid having to iterate the entire slice to serialize, these items
    /// must all be [serde_json::Value].
    async fn upsert_items<T>(&self, items: &[T]) -> Result<()>
    where
        T: Serialize,
    {
        let items = serde_json::to_value(items)?;
        self.void("upsert_items", &[&items]).await
    }

    /// Searches for items.
    async fn search<T>(&self, search: T) -> Result<Page>
    where
        T: Serialize,
    {
        let search = serde_json::to_value(search)?;
        // TODO do we want to check for cql2-text?
        self.value("search", &[&search]).await
    }

    /// Runs a pgstac function.
    async fn pgstac(
        &self,
        function: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> std::result::Result<Row, tokio_postgres::Error> {
        let param_string = (0..params.len())
            .map(|i| format!("${}", i + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!("SELECT * from pgstac.{}({})", function, param_string);
        self.query_one(&query, params).await
    }

    /// Returns a string result from a pgstac function.
    async fn string(&self, function: &str, params: &[&(dyn ToSql + Sync)]) -> Result<String> {
        let row = self.pgstac(function, params).await?;
        row.try_get(function).map_err(Error::from)
    }

    /// Returns a vector from a pgstac function.
    async fn vec<T>(&self, function: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        if let Some(value) = self.opt(function, params).await? {
            Ok(value)
        } else {
            Ok(Vec::new())
        }
    }

    /// Returns an optional value from a pgstac function.
    async fn opt<T>(&self, function: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let row = self.pgstac(function, params).await?;
        let option: Option<JsonValue> = row.try_get(function)?;
        let option = option.map(|v| serde_json::from_value(v)).transpose()?;
        Ok(option)
    }

    /// Returns a deserializable value from a pgstac function.
    async fn value<T>(&self, function: &str, params: &[&(dyn ToSql + Sync)]) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let row = self.pgstac(function, params).await?;
        let value = row.try_get(function)?;
        serde_json::from_value(value).map_err(Error::from)
    }

    /// Returns nothing from a pgstac function.
    async fn void(&self, function: &str, params: &[&(dyn ToSql + Sync)]) -> Result<()> {
        let _ = self.pgstac(function, params).await?;
        Ok(())
    }
}

impl<T> Pgstac for T where T: GenericClient {}

#[cfg(test)]
pub(crate) mod tests {
    use super::Pgstac;
    use geojson::{Geometry, Value};
    use pgstac_test::pgstac_test;
    use serde_json::{json, Map};
    use stac::{Collection, Item};
    use stac_api::{Fields, Filter, Search, Sortby};
    use std::sync::Mutex;
    use tokio_postgres::Transaction;
    use tokio_test as _;

    // This is an absolutely heinous way to ensure that only one test is hitting
    // the DB at a time -- the MUTEX is used in the pgstac-test crate as part of
    // the code generated by `pgstac_test`.
    //
    // There's got to be a better way.
    pub(crate) static MUTEX: Mutex<()> = Mutex::new(());

    fn longmont() -> Geometry {
        Geometry::new(Value::Point(vec![-105.1019, 40.1672]))
    }

    #[pgstac_test]
    async fn pgstac_version(client: &Transaction<'_>) {
        let _ = client.pgstac_version().await.unwrap();
    }

    #[pgstac_test]
    async fn context(client: &Transaction<'_>) {
        assert!(!client.context().await.unwrap());
    }

    #[pgstac_test]
    async fn set_context(client: &Transaction<'_>) {
        client.set_context(true).await.unwrap();
        assert!(client.context().await.unwrap());
    }

    #[pgstac_test]
    async fn collections(client: &Transaction<'_>) {
        assert!(client.collections().await.unwrap().is_empty());
        client
            .add_collection(Collection::new("an-id", "a description"))
            .await
            .unwrap();
        assert_eq!(client.collections().await.unwrap().len(), 1);
    }

    #[pgstac_test]
    async fn add_collection_duplicate(client: &Transaction<'_>) {
        assert!(client.collections().await.unwrap().is_empty());
        let collection = Collection::new("an-id", "a description");
        client.add_collection(collection.clone()).await.unwrap();
        assert!(client.add_collection(collection).await.is_err());
    }

    #[pgstac_test]
    async fn upsert_collection(client: &Transaction<'_>) {
        assert!(client.collections().await.unwrap().is_empty());
        let mut collection = Collection::new("an-id", "a description");
        client.upsert_collection(collection.clone()).await.unwrap();
        collection.title = Some("a title".to_string());
        client.upsert_collection(collection).await.unwrap();
        assert_eq!(
            client.collection("an-id").await.unwrap().unwrap()["title"],
            "a title"
        );
    }

    #[pgstac_test]
    async fn update_collection(client: &Transaction<'_>) {
        let mut collection = Collection::new("an-id", "a description");
        client.add_collection(collection.clone()).await.unwrap();
        assert!(client
            .collection("an-id")
            .await
            .unwrap()
            .unwrap()
            .get("title")
            .is_none());
        collection.title = Some("a title".to_string());
        client.update_collection(collection).await.unwrap();
        assert_eq!(client.collections().await.unwrap().len(), 1);
        assert_eq!(
            client.collection("an-id").await.unwrap().unwrap()["title"],
            "a title"
        );
    }

    #[pgstac_test]
    async fn update_collection_does_not_exit(client: &Transaction<'_>) {
        let collection = Collection::new("an-id", "a description");
        assert!(client.update_collection(collection).await.is_err());
    }

    #[pgstac_test]
    async fn collection_not_found(client: &Transaction<'_>) {
        assert!(client.collection("not-an-id").await.unwrap().is_none());
    }

    #[pgstac_test]
    async fn delete_collection(client: &Transaction<'_>) {
        let collection = Collection::new("an-id", "a description");
        client.add_collection(collection.clone()).await.unwrap();
        assert!(client.collection("an-id").await.unwrap().is_some());
        client.delete_collection("an-id").await.unwrap();
        assert!(client.collection("an-id").await.unwrap().is_none());
    }

    #[pgstac_test]
    async fn delete_collection_does_not_exist(client: &Transaction<'_>) {
        assert!(client.delete_collection("not-an-id").await.is_err());
    }

    #[pgstac_test]
    async fn item(client: &Transaction<'_>) {
        assert!(client
            .item("an-id", "collection-id")
            .await
            .unwrap()
            .is_none());
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        let _ = item
            .additional_fields
            .insert("type".into(), "Feature".into());
        client.add_item(item.clone()).await.unwrap();
        assert_eq!(
            client
                .item("an-id", "collection-id")
                .await
                .unwrap()
                .unwrap(),
            serde_json::to_value(item).unwrap(),
        );
    }

    #[pgstac_test]
    async fn item_without_collection(client: &Transaction<'_>) {
        let item = Item::new("an-id");
        assert!(client.add_item(item.clone()).await.is_err());
    }

    #[pgstac_test]
    async fn update_item(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        let _ = item
            .properties
            .additional_fields
            .insert("foo".into(), "bar".into());
        client.update_item(item).await.unwrap();
        assert_eq!(
            client
                .item("an-id", "collection-id")
                .await
                .unwrap()
                .unwrap()["properties"]["foo"],
            "bar"
        );
    }

    #[pgstac_test]
    async fn upsert_item(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.upsert_item(item.clone()).await.unwrap();
        client.upsert_item(item).await.unwrap();
    }

    #[pgstac_test]
    async fn add_items(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        let mut other_item = item.clone();
        other_item.id = "other-id".to_string();
        client.add_items(&[item, other_item]).await.unwrap();
        assert!(client
            .item("an-id", "collection-id")
            .await
            .unwrap()
            .is_some());
        assert!(client
            .item("other-id", "collection-id")
            .await
            .unwrap()
            .is_some());
    }

    #[pgstac_test]
    async fn upsert_items(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        let mut other_item = item.clone();
        other_item.id = "other-id".to_string();
        let items = vec![item, other_item];
        client.upsert_items(&items).await.unwrap();
        client.upsert_items(&items).await.unwrap();
    }

    #[pgstac_test]
    async fn search_everything(client: &Transaction<'_>) {
        assert!(client
            .search(Search::default())
            .await
            .unwrap()
            .features
            .is_empty());
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        assert_eq!(
            client.search(Search::default()).await.unwrap().features[0],
            *serde_json::to_value(item).unwrap().as_object().unwrap()
        );
    }

    #[pgstac_test]
    async fn search_ids(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        let search = Search {
            ids: Some(vec!["an-id".to_string()]),
            ..Default::default()
        };
        assert_eq!(client.search(search).await.unwrap().features.len(), 1);
        let search = Search {
            ids: Some(vec!["not-an-id".to_string()]),
            ..Default::default()
        };
        assert!(client.search(search).await.unwrap().features.is_empty());
    }

    #[pgstac_test]
    async fn search_collections(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        let search = Search {
            collections: Some(vec!["collection-id".to_string()]),
            ..Default::default()
        };
        assert_eq!(client.search(search).await.unwrap().features.len(), 1);
        let search = Search {
            collections: Some(vec!["not-an-id".to_string()]),
            ..Default::default()
        };
        assert!(client.search(search).await.unwrap().features.is_empty());
    }

    #[pgstac_test]
    async fn search_limit(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        item.id = "another-id".to_string();
        client.add_item(item).await.unwrap();
        let mut search = Search::default();
        search.items.limit = Some(1);
        let page = client.search(search).await.unwrap();
        assert_eq!(page.features.len(), 1);
        assert_eq!(page.context.limit.unwrap(), 1);
    }

    #[pgstac_test]
    async fn search_bbox(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        let mut search = Search::default();
        search.items.bbox = Some(vec![-106., 40., -105., 41.].try_into().unwrap());
        assert_eq!(
            client.search(search.clone()).await.unwrap().features.len(),
            1
        );
        search.items.bbox = Some(vec![-106., 41., -105., 42.].try_into().unwrap());
        assert!(client.search(search).await.unwrap().features.is_empty());
    }

    #[pgstac_test]
    async fn search_datetime(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        item.properties.datetime = Some("2023-01-07T00:00:00Z".parse().unwrap());
        client.add_item(item.clone()).await.unwrap();
        let mut search = Search::default();
        search.items.datetime = Some("2023-01-07T00:00:00Z".to_string());
        assert_eq!(
            client.search(search.clone()).await.unwrap().features.len(),
            1
        );
        search.items.datetime = Some("2023-01-08T00:00:00Z".to_string());
        assert!(client.search(search).await.unwrap().features.is_empty());
    }

    #[pgstac_test]
    async fn search_intersects(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        let search = Search {
            intersects: Some(
                serde_json::from_value(
                    serde_json::to_value(Geometry::new(Value::Polygon(vec![vec![
                        vec![-106., 40.],
                        vec![-106., 41.],
                        vec![-105., 41.],
                        vec![-105., 40.],
                        vec![-106., 40.],
                    ]])))
                    .unwrap(),
                )
                .unwrap(),
            ),
            ..Default::default()
        };
        assert_eq!(client.search(search).await.unwrap().features.len(), 1);
        let search = Search {
            intersects: Some(
                serde_json::from_value(
                    serde_json::to_value(Geometry::new(Value::Polygon(vec![vec![
                        vec![-104., 40.],
                        vec![-104., 41.],
                        vec![-103., 41.],
                        vec![-103., 40.],
                        vec![-104., 40.],
                    ]])))
                    .unwrap(),
                )
                .unwrap(),
            ),
            ..Default::default()
        };
        assert!(client.search(search).await.unwrap().features.is_empty());
    }

    #[pgstac_test]
    async fn pagination(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.properties.datetime = Some("2023-01-08T00:00:00Z".parse().unwrap());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        item.id = "another-id".to_string();
        item.properties.datetime = Some("2023-01-07T00:00:00Z".parse().unwrap());
        client.add_item(item).await.unwrap();
        let mut search = Search::default();
        search.items.limit = Some(1);
        let page = client.search(search.clone()).await.unwrap();
        assert_eq!(page.features[0]["id"], "an-id");
        let _ = search
            .additional_fields
            .insert("token".to_string(), page.next_token().into());
        let page = client.search(search.clone()).await.unwrap();
        assert_eq!(page.features[0]["id"], "another-id");
        let _ = search
            .additional_fields
            .insert("token".to_string(), page.prev_token().into());
        let page = client.search(search).await.unwrap();
        assert_eq!(page.features[0]["id"], "an-id");
    }

    #[pgstac_test]
    async fn fields(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("an-id");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        let _ = item
            .properties
            .additional_fields
            .insert("foo".into(), 42.into());
        let _ = item
            .properties
            .additional_fields
            .insert("bar".into(), 43.into());
        client.add_item(item).await.unwrap();
        let mut search = Search::default();
        search.items.fields = Some(Fields {
            include: vec!["properties.foo".to_string()],
            exclude: vec!["properties.bar".to_string()],
        });
        let page = client.search(search).await.unwrap();
        let item = &page.features[0];
        assert!(item["properties"].as_object().unwrap().get("foo").is_some());
        assert!(item["properties"].as_object().unwrap().get("bar").is_none());
    }

    #[pgstac_test]
    async fn sortby(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("a");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        client.add_item(item.clone()).await.unwrap();
        item.id = "b".to_string();
        client.add_item(item).await.unwrap();
        let mut search = Search::default();
        search.items.sortby = Some(vec![Sortby::asc("id")]);
        let page = client.search(search.clone()).await.unwrap();
        assert_eq!(page.features[0]["id"], "a");
        assert_eq!(page.features[1]["id"], "b");

        search.items.sortby = Some(vec![Sortby::desc("id")]);
        let page = client.search(search).await.unwrap();
        assert_eq!(page.features[0]["id"], "b");
        assert_eq!(page.features[1]["id"], "a");
    }

    #[pgstac_test]
    async fn filter(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("a");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        let _ = item
            .properties
            .additional_fields
            .insert("foo".into(), 42.into());
        client.add_item(item.clone()).await.unwrap();
        item.id = "b".to_string();
        let _ = item
            .properties
            .additional_fields
            .insert("foo".into(), 43.into());
        client.add_item(item).await.unwrap();
        let mut filter = Map::new();
        let _ = filter.insert("op".into(), "=".into());
        let _ = filter.insert("args".into(), json!([{"property": "foo"}, 42]));
        let mut search = Search::default();
        search.items.filter = Some(Filter::Cql2Json(filter));
        let page = client.search(search).await.unwrap();
        assert_eq!(page.features.len(), 1);
    }

    #[pgstac_test]
    async fn query(client: &Transaction<'_>) {
        let collection = Collection::new("collection-id", "a description");
        client.add_collection(collection).await.unwrap();
        let mut item = Item::new("a");
        item.collection = Some("collection-id".to_string());
        item.geometry = Some(longmont());
        let _ = item
            .properties
            .additional_fields
            .insert("foo".into(), 42.into());
        client.add_item(item.clone()).await.unwrap();
        item.id = "b".to_string();
        let _ = item
            .properties
            .additional_fields
            .insert("foo".into(), 43.into());
        client.add_item(item).await.unwrap();
        let mut query = Map::new();
        let _ = query.insert("foo".into(), json!({"eq": 42}));
        let mut search = Search::default();
        search.items.query = Some(query);
        let page = client.search(search).await.unwrap();
        assert_eq!(page.features.len(), 1);
    }
}
