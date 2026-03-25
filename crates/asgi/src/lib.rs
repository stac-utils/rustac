//! ASGI adapter for `stac-server`.
//!
//! This crate separates Python/ASGI integration concerns from the core server crate.
//! It provides:
//! - ASGI boundary models (`scope`, `receive`, `send` style structs/enums)
//! - Router dispatch helpers
//! - OpenAPI merge helpers suitable for host FastAPI apps
//! - `RustacAsgiApp` convenience wrapper to minimize binding glue code

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{HeaderName, HeaderValue, Method, Request, Uri},
};
use bytes::Bytes;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use std::collections::BTreeSet;
use std::str::FromStr;
use thiserror::Error;
use tower::ServiceExt;

/// A convenience wrapper around a mounted rustac router and metadata useful for
/// Python bindings.
#[derive(Clone, Debug)]
pub struct RustacAsgiApp {
    router: Router,
    openapi_schema: JsonValue,
    provided_route_patterns: BTreeSet<String>,
}

impl RustacAsgiApp {
    /// Creates a new ASGI app with a MemoryBackend.
    pub fn new(root_href: &str) -> Result<Self> {
        let backend = stac_server::MemoryBackend::new();
        let api = stac_server::Api::new(backend, root_href)?;
        Self::from_api(api)
    }

    /// Builds an ASGI app wrapper from an existing `stac_server::Api`.
    pub fn from_api<B: stac_server::Backend>(api: stac_server::Api<B>) -> Result<Self> {
        let router = stac_server::routes::from_api(api);
        // TODO: Load full OpenAPI schema from stac-server at runtime
        let openapi_schema = json!({
            "openapi": "3.0.3",
            "info": {"title": "STAC API", "version": "1.0.0"},
            "paths": {
                "/": {},
                "/api": {},
                "/api.html": {},
                "/conformance": {},
                "/queryables": {},
                "/collections": {},
                "/collections/{collection_id}": {},
                "/collections/{collection_id}/items": {},
                "/collections/{collection_id}/items/{item_id}": {},
                "/search": {}
            },
            "components": {
                "schemas": {}
            }
        });
        let provided_route_patterns = [
            "/".to_string(),
            "/api".to_string(),
            "/api.html".to_string(),
            "/conformance".to_string(),
            "/queryables".to_string(),
            "/collections".to_string(),
            "/collections/{collection_id}".to_string(),
            "/collections/{collection_id}/items".to_string(),
            "/collections/{collection_id}/items/{item_id}".to_string(),
            "/search".to_string(),
        ]
        .into_iter()
        .collect();
        Ok(Self {
            router,
            openapi_schema,
            provided_route_patterns,
        })
    }

    /// Returns the raw Rustac OpenAPI schema.
    pub fn openapi_schema(&self) -> JsonValue {
        self.openapi_schema.clone()
    }

    /// Returns the raw Rustac OpenAPI schema encoded as JSON text.
    pub fn openapi_schema_json(&self) -> Result<String> {
        serde_json::to_string(&self.openapi_schema()).map_err(|error| Error::JsonCodec {
            context: "openapi_schema_json",
            message: error.to_string(),
        })
    }

    /// Returns normalized route patterns provided by rustac router.
    pub fn provided_route_patterns(&self) -> BTreeSet<String> {
        self.provided_route_patterns.clone()
    }

    /// Returns normalized route patterns as a JSON array string.
    pub fn provided_route_patterns_json(&self) -> Result<String> {
        let patterns: Vec<String> = self.provided_route_patterns().into_iter().collect();
        serde_json::to_string(&patterns).map_err(|error| Error::JsonCodec {
            context: "provided_route_patterns_json",
            message: error.to_string(),
        })
    }

    /// Merges Rustac OpenAPI schema into host app schema using a mount prefix.
    pub fn merge_openapi_schema(&self, base_schema: JsonValue, mount_prefix: &str) -> Result<JsonValue> {
        merge_openapi_schema(base_schema, self.openapi_schema(), mount_prefix)
    }

    /// Merges host schema provided as JSON text and returns merged schema as
    /// JSON text.
    pub fn merge_openapi_schema_json(
        &self,
        base_schema_json: &str,
        mount_prefix: &str,
    ) -> Result<String> {
        let base_schema: JsonValue =
            serde_json::from_str(base_schema_json).map_err(|error| Error::JsonCodec {
                context: "merge_openapi_schema_json.base_schema",
                message: error.to_string(),
            })?;
        let merged = self.merge_openapi_schema(base_schema, mount_prefix)?;
        serde_json::to_string(&merged).map_err(|error| Error::JsonCodec {
            context: "merge_openapi_schema_json.merged",
            message: error.to_string(),
        })
    }

    /// Dispatches a buffered request.
    pub async fn dispatch(&self, request: AsgiHttpRequest) -> Result<AsgiHttpResponse> {
        dispatch(self.router.clone(), request).await
    }

    /// Dispatches scope/receive events and returns send events.
    pub async fn dispatch_events(
        &self,
        scope: AsgiHttpScope,
        receive_events: Vec<AsgiHttpReceiveEvent>,
    ) -> Result<Vec<AsgiHttpSendEvent>> {
        dispatch_events(self.router.clone(), scope, receive_events).await
    }

    /// Dispatches JSON-encoded scope/receive payloads and returns JSON-encoded
    /// send events.
    pub async fn dispatch_events_json(
        &self,
        scope_json: &str,
        receive_events_json: &str,
    ) -> Result<String> {
        let scope: AsgiHttpScope =
            serde_json::from_str(scope_json).map_err(|error| Error::JsonCodec {
                context: "dispatch_events_json.scope",
                message: error.to_string(),
            })?;
        let receive_events: Vec<AsgiHttpReceiveEvent> =
            serde_json::from_str(receive_events_json).map_err(|error| Error::JsonCodec {
                context: "dispatch_events_json.receive_events",
                message: error.to_string(),
            })?;
        let events = self.dispatch_events(scope, receive_events).await?;
        serde_json::to_string(&events).map_err(|error| Error::JsonCodec {
            context: "dispatch_events_json.send_events",
            message: error.to_string(),
        })
    }
}

/// A simplified ASGI-style HTTP request.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AsgiHttpRequest {
    /// The uppercase HTTP method (e.g. `GET`, `POST`).
    pub method: String,

    /// The request path (e.g. `/search`).
    pub path: String,

    /// The raw query string without the leading `?`.
    pub query_string: Option<String>,

    /// HTTP headers represented as `(name, value)` string pairs.
    pub headers: Vec<(String, String)>,

    /// Request body bytes.
    pub body: Vec<u8>,
}

/// A simplified ASGI-style HTTP response.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AsgiHttpResponse {
    /// Numeric HTTP status code.
    pub status: u16,

    /// HTTP headers represented as `(name, value)` string pairs.
    pub headers: Vec<(String, String)>,

    /// Response body bytes.
    pub body: Vec<u8>,
}

/// HTTP scope from Python ASGI `scope` (type=`http`) that can be converted into
/// an [AsgiHttpRequest] using accompanying receive events.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AsgiHttpScope {
    /// The uppercase HTTP method (e.g. `GET`, `POST`).
    pub method: String,

    /// The request path (e.g. `/search`).
    pub path: String,

    /// The raw query string without the leading `?`.
    pub query_string: Option<String>,

    /// HTTP headers represented as `(name, value)` string pairs.
    pub headers: Vec<(String, String)>,
}

/// Receive-side ASGI HTTP events (from Python `receive`).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AsgiHttpReceiveEvent {
    /// HTTP request body chunk.
    RequestBody {
        /// Bytes for this body chunk.
        body: Vec<u8>,
        /// Whether additional chunks will follow.
        more_body: bool,
    },
}

/// Send-side ASGI HTTP events (to Python `send`).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AsgiHttpSendEvent {
    /// Initial response start event.
    ResponseStart {
        /// Numeric HTTP status code.
        status: u16,
        /// HTTP headers represented as `(name, value)` string pairs.
        headers: Vec<(String, String)>,
    },
    /// Response body chunk event.
    ResponseBody {
        /// Bytes for this body chunk.
        body: Vec<u8>,
        /// Whether additional chunks will follow.
        more_body: bool,
    },
}

/// Errors that can occur while converting or dispatching ASGI-style messages.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The request method was invalid.
    #[error("invalid request method: {0}")]
    InvalidMethod(String),

    /// The request URI was invalid.
    #[error("invalid request URI: {0}")]
    InvalidUri(String),

    /// A request header name was invalid.
    #[error("invalid header name: {0}")]
    InvalidHeaderName(String),

    /// A request header value was invalid.
    #[error("invalid header value for '{name}': {value}")]
    InvalidHeaderValue {
        /// The header name.
        name: String,
        /// The invalid header value.
        value: String,
    },

    /// Failed to read the response body.
    #[error("error reading response body: {0}")]
    ResponseBody(#[from] axum::Error),

    /// Receive events were empty or did not include a terminal body chunk.
    #[error("invalid receive event sequence: missing terminal body event")]
    InvalidReceiveSequence,

    /// OpenAPI document was not an object.
    #[error("invalid OpenAPI document, expected JSON object: {0}")]
    InvalidOpenApi(String),

    /// JSON serialization or deserialization failed for an FFI helper.
    #[error("json codec error ({context}): {message}")]
    JsonCodec {
        /// Operation context where the failure happened.
        context: &'static str,
        /// JSON error message.
        message: String,
    },

    /// An error from stac-server.
    #[error("stac-server error: {0}")]
    StacServer(#[from] stac_server::Error),
}

/// A crate-specific result type for ASGI adapter operations.
pub type Result<T> = std::result::Result<T, Error>;

impl AsgiHttpRequest {
    fn into_http_request(self) -> Result<Request<Body>> {
        let method = Method::from_bytes(self.method.as_bytes())
            .map_err(|_| Error::InvalidMethod(self.method.clone()))?;
        let uri = build_uri(&self.path, self.query_string.as_deref())?;

        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::from(self.body))
            .map_err(|err| Error::InvalidUri(err.to_string()))?;

        for (name, value) in self.headers {
            let header_name = HeaderName::from_str(&name)
                .map_err(|_| Error::InvalidHeaderName(name.clone()))?;
            let header_value =
                HeaderValue::from_str(&value).map_err(|_| Error::InvalidHeaderValue {
                    name,
                    value,
                })?;
            let _ = request.headers_mut().append(header_name, header_value);
        }

        Ok(request)
    }

    fn into_http_request_streaming(self) -> Result<Request<Body>> {
        let method = Method::from_bytes(self.method.as_bytes())
            .map_err(|_| Error::InvalidMethod(self.method.clone()))?;
        let uri = build_uri(&self.path, self.query_string.as_deref())?;

        let body_stream = futures_util::stream::once(async move {
            Ok::<Bytes, std::convert::Infallible>(Bytes::from(self.body))
        });
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::from_stream(body_stream))
            .map_err(|err| Error::InvalidUri(err.to_string()))?;

        for (name, value) in self.headers {
            let header_name = HeaderName::from_str(&name)
                .map_err(|_| Error::InvalidHeaderName(name.clone()))?;
            let header_value =
                HeaderValue::from_str(&value).map_err(|_| Error::InvalidHeaderValue {
                    name,
                    value,
                })?;
            let _ = request.headers_mut().append(header_name, header_value);
        }

        Ok(request)
    }
}

impl AsgiHttpScope {
    /// Builds a full request from ASGI `scope` and `receive` events.
    pub fn into_request(
        self,
        receive_events: Vec<AsgiHttpReceiveEvent>,
    ) -> Result<AsgiHttpRequest> {
        let mut body = Vec::new();
        let mut saw_terminal = false;
        for event in receive_events {
            match event {
                AsgiHttpReceiveEvent::RequestBody {
                    body: chunk,
                    more_body,
                } => {
                    body.extend(chunk);
                    if !more_body {
                        saw_terminal = true;
                    }
                }
            }
        }
        if !saw_terminal {
            return Err(Error::InvalidReceiveSequence);
        }
        Ok(AsgiHttpRequest {
            method: self.method,
            path: self.path,
            query_string: self.query_string,
            headers: self.headers,
            body,
        })
    }
}

/// Dispatches an ASGI-style request into an `axum::Router` and returns an
/// ASGI-style response.
pub async fn dispatch(router: Router, request: AsgiHttpRequest) -> Result<AsgiHttpResponse> {
    let request = request.into_http_request()?;
    let response = router
        .oneshot(request)
        .await
        .expect("router service should be infallible");

    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect();
    let body = to_bytes(response.into_body(), usize::MAX).await?.to_vec();

    Ok(AsgiHttpResponse {
        status,
        headers,
        body,
    })
}

/// Dispatches an ASGI scope plus receive events, returning send events while
/// preserving body chunk boundaries in the response.
pub async fn dispatch_events(
    router: Router,
    scope: AsgiHttpScope,
    receive_events: Vec<AsgiHttpReceiveEvent>,
) -> Result<Vec<AsgiHttpSendEvent>> {
    let request = scope.into_request(receive_events)?;
    dispatch_streaming(router, request).await
}

/// Dispatches a request and returns send events with chunked response body
/// events suitable for Python ASGI `send` forwarding.
pub async fn dispatch_streaming(
    router: Router,
    request: AsgiHttpRequest,
) -> Result<Vec<AsgiHttpSendEvent>> {
    let request = request.into_http_request_streaming()?;
    let response = router
        .oneshot(request)
        .await
        .expect("router service should be infallible");

    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_string(), value.to_string()))
        })
        .collect();

    let mut events = vec![AsgiHttpSendEvent::ResponseStart { status, headers }];
    let mut chunks: Vec<Vec<u8>> = Vec::new();
    let mut stream = response.into_body().into_data_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        chunks.push(chunk.to_vec());
    }
    if chunks.is_empty() {
        events.push(AsgiHttpSendEvent::ResponseBody {
            body: Vec::new(),
            more_body: false,
        });
    } else {
        let last_idx = chunks.len() - 1;
        for (idx, chunk) in chunks.into_iter().enumerate() {
            events.push(AsgiHttpSendEvent::ResponseBody {
                body: chunk,
                more_body: idx != last_idx,
            });
        }
    }
    Ok(events)
}

fn build_uri(path: &str, query_string: Option<&str>) -> Result<Uri> {
    let mut uri = path.to_string();
    if let Some(query_string) = query_string
        && !query_string.is_empty()
    {
        uri.push('?');
        uri.push_str(query_string);
    }
    Uri::from_str(&uri).map_err(|_| Error::InvalidUri(uri))
}

/// Merges a Rustac OpenAPI schema into a host FastAPI/OpenAPI schema.
///
/// Merge behavior:
/// - Rustac paths are mounted under `mount_prefix`.
/// - Existing host path keys are preserved.
/// - Component key collisions are copied with `rustac_` prefix.
pub fn merge_openapi_schema(
    base_schema: JsonValue,
    rustac_schema: JsonValue,
    mount_prefix: &str,
) -> Result<JsonValue> {
    let mut merged_obj = into_object(base_schema, "base_schema")?;
    let rustac_obj = into_object(rustac_schema, "rustac_schema")?;

    let mut merged_paths = match merged_obj.remove("paths") {
        Some(JsonValue::Object(map)) => map,
        Some(_) => return Err(Error::InvalidOpenApi("base_schema.paths".to_string())),
        None => JsonMap::new(),
    };

    if let Some(JsonValue::Object(rustac_paths)) = rustac_obj.get("paths") {
        for (path, path_item) in rustac_paths {
            let mounted_path = mount_path(mount_prefix, path);
            let _ = merged_paths
                .entry(mounted_path)
                .or_insert_with(|| path_item.clone());
        }
    }
    let _ = merged_obj.insert("paths".to_string(), JsonValue::Object(merged_paths));

    let mut merged_components = match merged_obj.remove("components") {
        Some(JsonValue::Object(map)) => map,
        Some(_) => {
            return Err(Error::InvalidOpenApi(
                "base_schema.components".to_string(),
            ));
        }
        None => JsonMap::new(),
    };

    if let Some(JsonValue::Object(rustac_components)) = rustac_obj.get("components") {
        merge_components(&mut merged_components, rustac_components)?;
    }
    let _ = merged_obj.insert(
        "components".to_string(),
        JsonValue::Object(merged_components),
    );

    Ok(JsonValue::Object(merged_obj))
}

fn into_object(value: JsonValue, name: &'static str) -> Result<JsonMap<String, JsonValue>> {
    match value {
        JsonValue::Object(obj) => Ok(obj),
        _ => Err(Error::InvalidOpenApi(name.to_string())),
    }
}

fn mount_path(mount_prefix: &str, path: &str) -> String {
    let prefix = mount_prefix.trim_end_matches('/');
    if prefix.is_empty() || prefix == "/" {
        path.to_string()
    } else {
        format!("{prefix}{path}")
    }
}

fn merge_components(
    merged_components: &mut JsonMap<String, JsonValue>,
    rustac_components: &JsonMap<String, JsonValue>,
) -> Result<()> {
    for (section, section_value) in rustac_components {
        let section_obj = match section_value {
            JsonValue::Object(map) => map,
            _ => {
                return Err(Error::InvalidOpenApi(format!(
                    "rustac_schema.components.{section}"
                )));
            }
        };

        let merged_section = merged_components
            .entry(section.clone())
            .or_insert_with(|| JsonValue::Object(JsonMap::new()));
        let merged_section_obj = match merged_section {
            JsonValue::Object(map) => map,
            _ => {
                return Err(Error::InvalidOpenApi(format!(
                    "base_schema.components.{section}"
                )));
            }
        };

        for (key, value) in section_obj {
            let target = if merged_section_obj.contains_key(key) {
                format!("rustac_{key}")
            } else {
                key.clone()
            };
            let _ = merged_section_obj.insert(target, value.clone());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AsgiHttpReceiveEvent, AsgiHttpRequest, AsgiHttpScope, AsgiHttpSendEvent, Error,
        RustacAsgiApp, dispatch, dispatch_events, dispatch_streaming, merge_openapi_schema,
    };
    use serde_json::json;
    use stac_server::{Api, MemoryBackend};

    fn app() -> RustacAsgiApp {
        let api = Api::new(MemoryBackend::new(), "http://stac.test/").unwrap();
        RustacAsgiApp::from_api(api).unwrap()
    }

    #[tokio::test]
    async fn dispatch_root() {
        let request = AsgiHttpRequest {
            method: "GET".to_string(),
            path: "/".to_string(),
            query_string: None,
            headers: Vec::new(),
            body: Vec::new(),
        };
        let response = dispatch(stac_server::routes::from_api(Api::new(MemoryBackend::new(), "http://stac.test/").unwrap()), request)
            .await
            .unwrap();
        assert_eq!(response.status, 200);
    }

    #[tokio::test]
    async fn dispatch_via_wrapper() {
        let request = AsgiHttpRequest {
            method: "GET".to_string(),
            path: "/".to_string(),
            query_string: None,
            headers: Vec::new(),
            body: Vec::new(),
        };
        let response = app().dispatch(request).await.unwrap();
        assert_eq!(response.status, 200);
    }

    #[tokio::test]
    async fn dispatch_post_search() {
        let request = AsgiHttpRequest {
            method: "POST".to_string(),
            path: "/search".to_string(),
            query_string: None,
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            body: b"{}".to_vec(),
        };
        let response = app().dispatch(request).await.unwrap();
        assert_eq!(response.status, 200);
    }

    #[tokio::test]
    async fn invalid_header_name() {
        let request = AsgiHttpRequest {
            method: "GET".to_string(),
            path: "/".to_string(),
            query_string: None,
            headers: vec![("bad header".to_string(), "x".to_string())],
            body: Vec::new(),
        };
        let error = app().dispatch(request).await.err().unwrap();
        assert!(matches!(error, Error::InvalidHeaderName(_)));
    }

    #[tokio::test]
    async fn dispatch_events_from_scope_and_receive() {
        let scope = AsgiHttpScope {
            method: "POST".to_string(),
            path: "/search".to_string(),
            query_string: None,
            headers: vec![("content-type".to_string(), "application/json".to_string())],
        };
        let events = vec![AsgiHttpReceiveEvent::RequestBody {
            body: b"{}".to_vec(),
            more_body: false,
        }];

        let response_events = dispatch_events(
            stac_server::routes::from_api(Api::new(MemoryBackend::new(), "http://stac.test/").unwrap()),
            scope,
            events,
        )
        .await
        .unwrap();
        assert!(matches!(
            response_events.first(),
            Some(AsgiHttpSendEvent::ResponseStart { status: 200, .. })
        ));
    }

    #[tokio::test]
    async fn streaming_response_includes_terminal_chunk() {
        let request = AsgiHttpRequest {
            method: "GET".to_string(),
            path: "/".to_string(),
            query_string: None,
            headers: Vec::new(),
            body: Vec::new(),
        };
        let response_events = dispatch_streaming(
            stac_server::routes::from_api(Api::new(MemoryBackend::new(), "http://stac.test/").unwrap()),
            request,
        )
        .await
        .unwrap();
        assert!(
            response_events.iter().any(|event| {
                matches!(
                    event,
                    AsgiHttpSendEvent::ResponseBody {
                        more_body: false,
                        ..
                    }
                )
            })
        );
    }

    #[tokio::test]
    async fn invalid_receive_sequence() {
        let scope = AsgiHttpScope {
            method: "GET".to_string(),
            path: "/".to_string(),
            query_string: None,
            headers: Vec::new(),
        };
        let events = vec![AsgiHttpReceiveEvent::RequestBody {
            body: Vec::new(),
            more_body: true,
        }];
        let error = app().dispatch_events(scope, events).await.err().unwrap();
        assert!(matches!(error, Error::InvalidReceiveSequence));
    }

    #[test]
    fn merge_openapi_schema_prefixes_paths() {
        let base = json!({
            "openapi": "3.0.3",
            "paths": {
                "/healthz": { "get": {"summary": "health"}}
            },
            "components": {
                "schemas": {
                    "Health": {"type": "object"}
                }
            }
        });
        let rustac = json!({
            "openapi": "3.0.3",
            "paths": {
                "/search": { "get": {"summary": "search"}}
            },
            "components": {
                "schemas": {
                    "Health": {"type": "string"},
                    "Search": {"type": "object"}
                }
            }
        });

        let merged = merge_openapi_schema(base, rustac, "/stac").unwrap();
        let paths = merged.get("paths").unwrap().as_object().unwrap();
        assert!(paths.contains_key("/healthz"));
        assert!(paths.contains_key("/stac/search"));

        let schemas = merged
            .get("components")
            .unwrap()
            .get("schemas")
            .unwrap()
            .as_object()
            .unwrap();
        assert!(schemas.contains_key("Health"));
        assert!(schemas.contains_key("rustac_Health"));
        assert!(schemas.contains_key("Search"));
    }

    #[test]
    fn wrapper_exposes_route_patterns() {
        let routes = app().provided_route_patterns();
        assert!(routes.contains("/search"));
        assert!(routes.contains("/conformance"));
    }

    #[test]
    fn wrapper_exposes_json_helpers() {
        let app = app();
        let openapi_json = app.openapi_schema_json().unwrap();
        let openapi: serde_json::Value = serde_json::from_str(&openapi_json).unwrap();
        assert_eq!(openapi.get("openapi").unwrap(), "3.0.3");

        let routes_json = app.provided_route_patterns_json().unwrap();
        let routes: Vec<String> = serde_json::from_str(&routes_json).unwrap();
        assert!(routes.contains(&"/search".to_string()));

        let base_schema = json!({"openapi":"3.0.3","paths":{},"components":{}});
        let merged = app
            .merge_openapi_schema_json(&base_schema.to_string(), "/stac")
            .unwrap();
        let merged: serde_json::Value = serde_json::from_str(&merged).unwrap();
        assert!(merged
            .get("paths")
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("/stac/search"));
    }

    #[tokio::test]
    async fn dispatch_events_json_roundtrip() {
        let app = app();
        let scope = json!({
            "method": "POST",
            "path": "/search",
            "query_string": null,
            "headers": [["content-type", "application/json"]]
        });
        let receive_events = json!([
            {
                "RequestBody": {
                    "body": [123, 125],
                    "more_body": false
                }
            }
        ]);

        let send_events_json = app
            .dispatch_events_json(&scope.to_string(), &receive_events.to_string())
            .await
            .unwrap();
        let send_events: Vec<serde_json::Value> = serde_json::from_str(&send_events_json).unwrap();
        assert!(!send_events.is_empty());
    }
}
