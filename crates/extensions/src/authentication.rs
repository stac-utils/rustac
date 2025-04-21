//! Provides a standard set of fields to describe authentication and
//! authorization schemes, flows, and scopes required to access
//! [Assets](stac::Asset) and [Links](stac::Link) that align with the [OpenAPI
//! security spec](https://swagger.io/docs/specification/authentication/).

use crate::Extension;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The authentication extension fields.
#[derive(Debug, Serialize, Deserialize)]
pub struct Authentication {
    /// A property that contains all of the [scheme definitions](Scheme) used by
    /// [Assets](stac::Asset) and [Links](stac::Link) in the STAC [Item](crate::Item) or [Collection](crate::Collection).
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub schemes: IndexMap<String, Scheme>,

    /// A property that specifies which schemes may be used to access an [Asset](stac::Asset)
    /// or [Link](stac::Link).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refs: Vec<String>,
}

/// The Authentication Scheme extends the [OpenAPI security
/// spec](https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#security-scheme-object)
/// for support of OAuth2.0, API Key, and OpenID Connect authentication.
#[derive(Debug, Serialize, Deserialize)]
pub struct Scheme {
    /// The authentication scheme type used to access the data (`http` | `s3` |
    /// `signedUrl` | `oauth2` | `apiKey` | `openIdConnect` | a custom scheme type ).
    pub r#type: String,

    /// Additional instructions for authentication.
    ///
    /// [CommonMark 0.29](https://commonmark.org/) syntax MAY be used for rich text representation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// The name of the header, query, or cookie parameter to be used.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// The location of the API key (`query` | `header` | `cookie`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#in: Option<In>,

    /// The name of the HTTP Authorization scheme to be used in the Authorization header as defined in RFC7235.
    ///
    /// The values used SHOULD be registered in the IANA Authentication Scheme registry.
    /// (`basic` | `bearer` | `digest` | `dpop` | `hoba` | `mutual` |
    /// `negotiate` | `oauth` (1.0) | `privatetoken` | `scram-sha-1` |
    /// `scram-sha-256` | `vapid`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheme: Option<String>,

    /// Scenarios an API client performs to get an access token from the authorization server.
    ///
    /// For oauth2 the following keys are pre-defined for the corresponding
    /// OAuth flows: `authorizationCode` | `implicit` | `password` |
    /// `clientCredentials`.  The OAuth2 Flow Object applies for oauth2, the
    /// Signed URL Object applies to signedUrl.
    #[serde(skip_serializing_if = "IndexMap::is_empty", default)]
    pub flows: IndexMap<String, Flow>,

    /// OpenID Connect URL to discover OpenID configuration values.
    ///
    /// This MUST be in the form of a URL.
    #[serde(skip_serializing_if = "Option::is_none", rename = "openIdConnectUrl")]
    pub open_id_connect_url: Option<String>,
}

/// The OAuth2 Flow Object applies for oauth2, the Signed URL Object applies to signedUrl.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Flow {
    /// Based on the [OpenAPI OAuth Flow
    /// Object](https://github.com/OAI/OpenAPI-Specification/blob/main/versions/3.0.3.md#oauth-flows-object).
    ///
    /// Allows configuration of the supported OAuth Flows.
    OAuth2 {
        /// The authorization URL to be used for this flow.
        ///
        /// This MUST be in the form of a URL.
        #[serde(skip_serializing_if = "Option::is_none", rename = "authorizationUrl")]
        authorization_url: Option<String>,

        /// The token URL to be used for this flow.
        ///
        /// This MUST be in the form of a URL.
        #[serde(skip_serializing_if = "Option::is_none", rename = "tokenUrl")]
        token_url: Option<String>,

        /// The available scopes for the authentication scheme.
        ///
        /// A map between the scope name and a short description for it. The map MAY be empty.
        scopes: IndexMap<String, String>,

        /// The URL to be used for obtaining refresh tokens.
        ///
        /// This MUST be in the form of a URL.
        #[serde(skip_serializing_if = "Option::is_none", rename = "refreshUrl")]
        refresh_url: Option<String>,
    },

    /// A signed url flow.
    SignedUrl {
        /// The method to be used for requests.
        method: String,

        /// The signed URL API endpoint to be used for this flow.
        ///
        /// If not inferred from the client environment, this must be defined in the authentication flow.
        #[serde(skip_serializing_if = "Option::is_none", rename = "authorizationApi")]
        authorization_api: Option<String>,

        /// Parameter definition for requests to the authorizationApi
        #[serde(skip_serializing_if = "IndexMap::is_empty")]
        parameters: IndexMap<String, Parameter>,

        /// Key name for the signed URL field in an authorizationApi response
        #[serde(skip_serializing_if = "Option::is_none", rename = "responseField")]
        response_field: Option<String>,
    },
}

/// Definition for a request parameter.
#[derive(Debug, Serialize, Deserialize)]
pub struct Parameter {
    /// The location of the parameter (`query` | `header` | `body`).
    pub r#in: String,

    /// Setting for optional or required parameter.
    pub required: bool,

    /// Plain language description of the parameter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Schema object following the [JSON Schema draft-07](Schema object following the JSON Schema draft-07).
    pub schema: IndexMap<String, Value>,
}

/// Query, header, or cookie.
#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
pub enum In {
    /// In the GET query string.
    #[serde(rename = "query")]
    Query,

    /// In the headers.
    #[default]
    #[serde(rename = "header")]
    Header,

    /// In the cookie.
    #[serde(rename = "cookie")]
    Cookie,
}

impl Extension for Authentication {
    const IDENTIFIER: &'static str =
        "https://stac-extensions.github.io/authentication/v1.1.0/schema.json";
    const PREFIX: &'static str = "auth";
}

#[cfg(test)]
mod tests {
    use super::{Authentication, In, Scheme};
    use crate::{Collection, Extensions, Item};
    use serde_json::json;

    #[test]
    fn collection() {
        let collection: Collection = stac::read("data/auth/collection.json").unwrap();
        let authentication: Authentication = collection.extension().unwrap();
        let oauth = authentication.schemes.get("oauth").unwrap();
        let _ = oauth.flows.get("authorizationCode").unwrap();
    }

    #[test]
    fn item() {
        let collection: Item = stac::read("data/auth/item.json").unwrap();
        let authentication: Authentication = collection.extension().unwrap();
        let _ = authentication.schemes.get("none").unwrap();
    }

    #[test]
    fn api_key() {
        let scheme: Scheme = serde_json::from_value(json!({
          "type": "apiKey",
          "in": "query",
          "name": "API_KEY"
        }))
        .unwrap();
        assert_eq!(scheme.r#in.unwrap(), In::Query);
    }
}
