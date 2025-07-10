use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use stac::Link;
use stac_api::{Context, Item};

/// A page of search results.
#[derive(Debug, Deserialize, Serialize)]
pub struct Page {
    /// These are the out features, usually STAC items, but maybe not legal STAC
    /// items if fields are excluded.
    pub features: Vec<Item>,

    /// The next id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,

    /// The previous id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev: Option<String>,

    /// The search context.
    ///
    /// This was removed in pgstac v0.9
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Context>,

    /// The number of values returned.
    ///
    /// Added in pgstac v0.9
    #[serde(rename = "numberReturned", skip_serializing_if = "Option::is_none")]
    pub number_returned: Option<usize>,

    /// Links
    ///
    /// Added in pgstac v0.9
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<Link>,

    /// Additional fields.
    #[serde(flatten)]
    pub additional_fields: Map<String, Value>,
}

impl Page {
    /// Returns this page's next token, if it has one.
    pub fn next_token(&self) -> Option<String> {
        self.next.as_ref().map(|next| format!("next:{next}"))
    }

    /// Returns this page's prev token, if it has one.
    pub fn prev_token(&self) -> Option<String> {
        self.prev.as_ref().map(|prev| format!("prev:{prev}"))
    }
}
