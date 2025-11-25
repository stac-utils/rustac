//! Routes for serving API endpoints.

use crate::{Api, Backend};
use axum::{
    Json, Router,
    extract::{Path, Query, State, rejection::JsonRejection},
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use bytes::{BufMut, BytesMut};
use http::Method;
use serde::Serialize;
use stac::api::{Collections, GetItems, GetSearch, ItemCollection, Items, Root, Search};
use stac::{
    Collection, Item,
    mime::{APPLICATION_GEOJSON, APPLICATION_OPENAPI_3_0},
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

/// Errors for our axum routes.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// An server error.
    Server(crate::Error),

    /// An error raised when something is not found.
    NotFound(String),

    /// An error raised when it's a bad request from the client.
    BadRequest(String),
}

type Result<T> = std::result::Result<T, Error>;

/// A wrapper struct for any geojson response.
// Taken from https://docs.rs/axum/latest/src/axum/json.rs.html#93
#[derive(Debug)]
pub struct GeoJson<T>(pub T);

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        match self {
            Error::Server(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
            Error::NotFound(message) => (StatusCode::NOT_FOUND, message),
            Error::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
        }
        .into_response()
    }
}

impl From<crate::Error> for Error {
    fn from(error: crate::Error) -> Self {
        Error::Server(error)
    }
}

impl From<JsonRejection> for Error {
    fn from(json_rejection: JsonRejection) -> Self {
        Error::BadRequest(format!("bad request, json rejection: {json_rejection}"))
    }
}

impl<T> IntoResponse for GeoJson<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        // Use a small initial capacity of 128 bytes like serde_json::to_vec
        // https://docs.rs/serde_json/1.0.82/src/serde_json/ser.rs.html#2189
        let mut buf = BytesMut::with_capacity(128).writer();
        match serde_json::to_writer(&mut buf, &self.0) {
            Ok(()) => (
                [(CONTENT_TYPE, HeaderValue::from_static(APPLICATION_GEOJSON))],
                buf.into_inner().freeze(),
            )
                .into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    CONTENT_TYPE,
                    HeaderValue::from_static(mime::TEXT_PLAIN_UTF_8.as_ref()),
                )],
                err.to_string(),
            )
                .into_response(),
        }
    }
}

/// Creates an [axum::Router] from an [Api].
///
/// # Examples
///
/// ```
/// use stac_server::{Api, MemoryBackend, routes};
///
/// let api = Api::new(MemoryBackend::new(), "http://stac.test").unwrap();
/// let router = routes::from_api(api);
/// ```
pub fn from_api<B: Backend>(api: Api<B>) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/api", get(service_desc))
        .route("/api.html", get(service_doc))
        .route("/conformance", get(conformance))
        .route("/queryables", get(queryables))
        .route("/collections", get(collections))
        .route("/collections/{collection_id}", get(collection))
        .route("/collections/{collection_id}/items", get(items))
        .route("/collections/{collection_id}/items/{item_id}", get(item))
        .route("/search", get(get_search))
        .route("/search", post(post_search))
        .layer(CorsLayer::permissive()) // TODO make this configurable
        .layer(TraceLayer::new_for_http())
        .with_state(api)
}

/// Returns the `/` endpoint from the [core conformance
/// class](https://github.com/radiantearth/stac-api-spec/tree/release/v1.0.0/core#endpoints).
pub async fn root<B: Backend>(State(api): State<Api<B>>) -> Result<Json<Root>> {
    api.root().await.map(Json).map_err(Error::from)
}

/// Returns the `/api` endpoint from the [core conformance
/// class](https://github.com/radiantearth/stac-api-spec/tree/release/v1.0.0/core#endpoints).
pub async fn service_desc() -> Response {
    // The OpenAPI definition is completely stolen from [stac-server](https://github.com/stac-utils/stac-server/blob/dd7e3acbf47485425e2068fd7fbbceeafe4b4e8c/src/lambdas/api/openapi.yaml).
    //
    // TODO add a script to update the definition in this library.
    (
        [(CONTENT_TYPE, APPLICATION_OPENAPI_3_0)],
        include_str!("openapi.yaml"),
    )
        .into_response()
}

/// Returns the `/api.html` endpoint from the [core conformance
/// class](https://github.com/radiantearth/stac-api-spec/tree/release/v1.0.0/core#endpoints).
pub async fn service_doc() -> Response {
    // The redoc file is completely stolen from [stac-server](https://github.com/stac-utils/stac-server/blob/dd7e3acbf47485425e2068fd7fbbceeafe4b4e8c/src/lambdas/api/redoc.html).
    Html(include_str!("redoc.html")).into_response()
}

/// Returns the `/conformance` endpoint from the [ogcapi-features conformance
/// class](https://github.com/radiantearth/stac-api-spec/blob/release/v1.0.0/ogcapi-features/README.md#endpoints).
pub async fn conformance<B: Backend>(State(api): State<Api<B>>) -> Response {
    Json(api.conformance()).into_response()
}

/// Returns the `/queryables` endpoint.
pub async fn queryables<B: Backend>(State(api): State<Api<B>>) -> Response {
    (
        [(CONTENT_TYPE, "application/schema+json")],
        Json(api.queryables()),
    )
        .into_response()
}

/// Returns the `/collections` endpoint from the [ogcapi-features conformance
/// class](https://github.com/radiantearth/stac-api-spec/blob/release/v1.0.0/ogcapi-features/README.md#endpoints).
pub async fn collections<B: Backend>(State(api): State<Api<B>>) -> Result<Json<Collections>> {
    api.collections().await.map(Json).map_err(Error::from)
}

/// Returns the `/collections/{collectionId}` endpoint from the [ogcapi-features
/// conformance
/// class](https://github.com/radiantearth/stac-api-spec/blob/release/v1.0.0/ogcapi-features/README.md#endpoints).
pub async fn collection<B: Backend>(
    State(api): State<Api<B>>,
    Path(collection_id): Path<String>,
) -> Result<Json<Collection>> {
    api.collection(&collection_id)
        .await
        .map_err(Error::from)
        .and_then(|option| {
            option
                .ok_or_else(|| Error::NotFound(format!("no collection with id='{collection_id}'")))
        })
        .map(Json)
}

/// Returns the `/collections/{collectionId}/items` endpoint from the
/// [ogcapi-features conformance
/// class](https://github.com/radiantearth/stac-api-spec/tree/release/v1.0.0/ogcapi-features#collection-items-collectionscollectioniditems)
pub async fn items<B: Backend>(
    State(api): State<Api<B>>,
    Path(collection_id): Path<String>,
    items: Query<GetItems>,
) -> Result<GeoJson<ItemCollection>> {
    let items = Items::try_from(items.0)
        .and_then(Items::valid)
        .map_err(|error| Error::BadRequest(format!("invalid query: {error}")))?;
    api.items(&collection_id, items)
        .await
        .map_err(Error::from)
        .and_then(|option| {
            option
                .ok_or_else(|| Error::NotFound(format!(" no collection with id='{collection_id}'")))
        })
        .map(GeoJson)
}

/// Returns the `/collections/{collectionId}/items/{itemId}` endpoint from the
/// [ogcapi-features conformance
/// class](https://github.com/radiantearth/stac-api-spec/tree/release/v1.0.0/ogcapi-features#collection-items-collectionscollectioniditems)
pub async fn item<B: Backend>(
    State(api): State<Api<B>>,
    Path((collection_id, item_id)): Path<(String, String)>,
) -> Result<GeoJson<Item>> {
    api.item(&collection_id, &item_id)
        .await?
        .ok_or_else(|| {
            Error::NotFound(format!(
                "no item with id='{item_id}' in collection='{collection_id}'"
            ))
        })
        .map(GeoJson)
}

/// Returns the GET `/search` endpoint from the [item search conformance
/// class](https://github.com/radiantearth/stac-api-spec/tree/release/v1.0.0/item-search)
pub async fn get_search<B: Backend>(
    State(api): State<Api<B>>,
    search: Query<GetSearch>,
) -> Result<GeoJson<ItemCollection>> {
    tracing::debug!("GET /search: {:?}", search.0);
    let search = Search::try_from(search.0)
        .and_then(Search::valid)
        .map_err(|error| Error::BadRequest(error.to_string()))?;

    Ok(GeoJson(api.search(search, Method::GET).await?))
}

/// Returns the POST `/search` endpoint from the [item search conformance
/// class](https://github.com/radiantearth/stac-api-spec/tree/release/v1.0.0/item-search)
pub async fn post_search<B: Backend>(
    State(api): State<Api<B>>,
    search: std::result::Result<Json<Search>, JsonRejection>,
) -> Result<GeoJson<ItemCollection>> {
    let search = search?
        .0
        .valid()
        .map_err(|error| Error::BadRequest(error.to_string()))?;
    Ok(GeoJson(api.search(search, Method::POST).await?))
}

#[cfg(test)]
mod tests {
    use crate::{Api, Backend, MemoryBackend};
    use axum::{
        body::Body,
        http::{Request, Response, StatusCode, header::CONTENT_TYPE},
    };
    use stac::{Collection, Item};
    use tower::util::ServiceExt;

    async fn get(backend: MemoryBackend, uri: &str) -> Response<Body> {
        let router = super::from_api(
            Api::new(backend, "http://stac.test/")
                .unwrap()
                .id("an-id")
                .description("a description"),
        );
        router
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn post(backend: MemoryBackend, uri: &str) -> Response<Body> {
        let router = super::from_api(
            Api::new(backend, "http://stac.test/")
                .unwrap()
                .id("an-id")
                .description("a description"),
        );
        router
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .method("POST")
                    .header("Content-Type", "application/json")
                    .body("{}".to_string())
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn root() {
        let response = get(MemoryBackend::new(), "/").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }

    #[tokio::test]
    async fn service_description() {
        let response = get(MemoryBackend::new(), "/api").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/vnd.oai.openapi+json;version=3.0"
        );
    }

    #[tokio::test]
    async fn service_doc() {
        let response = get(MemoryBackend::new(), "/api.html").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
    }

    #[tokio::test]
    async fn conformance() {
        let response = get(MemoryBackend::new(), "/conformance").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }

    #[tokio::test]
    async fn collections() {
        let response = get(MemoryBackend::new(), "/collections").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }

    #[tokio::test]
    async fn collection() {
        let response = get(MemoryBackend::new(), "/collections/an-id").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let mut backend = MemoryBackend::new();
        backend
            .add_collection(Collection::new("an-id", "A description"))
            .await
            .unwrap();
        let response = get(backend, "/collections/an-id").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }

    #[tokio::test]
    async fn items() {
        let response = get(MemoryBackend::new(), "/collections/collection-id/items").await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let mut backend = MemoryBackend::new();
        backend
            .add_collection(Collection::new("collection-id", "A description"))
            .await
            .unwrap();
        backend
            .add_item(Item::new("item-id").collection("collection-id"))
            .await
            .unwrap();
        let response = get(backend, "/collections/collection-id/items").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/geo+json"
        );
    }

    #[tokio::test]
    async fn item() {
        let response = get(
            MemoryBackend::new(),
            "/collections/collection-id/items/item-id",
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let mut backend = MemoryBackend::new();
        backend
            .add_collection(Collection::new("collection-id", "A description"))
            .await
            .unwrap();
        backend
            .add_item(Item::new("item-id").collection("collection-id"))
            .await
            .unwrap();
        let response = get(backend, "/collections/collection-id/items/item-id").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/geo+json"
        );
    }

    #[tokio::test]
    async fn get_search() {
        let response = get(MemoryBackend::new(), "/search").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/geo+json"
        );
    }

    #[tokio::test]
    async fn post_search() {
        let response = post(MemoryBackend::new(), "/search").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/geo+json"
        );
    }
}
