use crate::{Error, Result};
use cql2::Expr;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{convert::Infallible, str::FromStr};

/// The language of the filter expression.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "filter-lang", content = "filter")]
pub enum Filter {
    /// `cql2-text`
    #[serde(rename = "cql2-text")]
    Cql2Text(String),

    /// `cql2-json`
    #[serde(rename = "cql2-json")]
    Cql2Json(Map<String, Value>),
}

impl Filter {
    /// Converts this filter to cql2-json.
    pub fn into_cql2_json(self) -> Result<Filter> {
        match self {
            Filter::Cql2Json(_) => Ok(self),
            Filter::Cql2Text(text) => {
                let expr = cql2::parse_text(&text).map_err(Box::new)?;
                Ok(Filter::Cql2Json(serde_json::from_value(
                    serde_json::to_value(expr)?,
                )?))
            }
        }
    }

    /// Converts this filter to cql2-json.
    pub fn into_cql2_text(self) -> Result<Filter> {
        match self {
            Filter::Cql2Text(_) => Ok(self),
            Filter::Cql2Json(json) => {
                let expr: Expr = serde_json::from_value(Value::Object(json))?;
                Ok(Filter::Cql2Text(expr.to_text().map_err(Box::new)?))
            }
        }
    }
}

impl Default for Filter {
    fn default() -> Self {
        Filter::Cql2Json(Default::default())
    }
}

impl FromStr for Filter {
    type Err = Infallible;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(Filter::Cql2Text(s.to_string()))
    }
}

impl TryFrom<Filter> for Expr {
    type Error = Error;
    fn try_from(value: Filter) -> Result<Self> {
        match value {
            Filter::Cql2Json(json) => {
                serde_json::from_value(Value::Object(json)).map_err(Error::from)
            }
            Filter::Cql2Text(text) => cql2::parse_text(&text).map_err(|e| Error::from(Box::new(e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Filter;
    use cql2::Expr;
    use serde_json::json;

    #[test]
    fn json() {
        let filter = Filter::Cql2Json(json!({
                  "filter": {
                    "op" : "and",
                    "args": [
                      {
                        "op": "=",
                        "args": [ { "property": "id" }, "LC08_L1TP_060247_20180905_20180912_01_T1_L1TP" ]
                      },
                      {
                        "op": "=",
                        "args" : [ { "property": "collection" }, "landsat8_l1tp" ]
                      }
                    ]
                  }
                }
            ).as_object().unwrap().clone(),
        );
        let value = serde_json::to_value(filter).unwrap();
        assert_eq!(value["filter-lang"], "cql2-json");
        assert!(value.get("filter").is_some());
    }

    #[test]
    fn text() {
        let filter = Filter::Cql2Text(
            "id='LC08_L1TP_060247_20180905_20180912_01_T1_L1TP' AND collection='landsat8_l1tp'"
                .to_string(),
        );
        let value = serde_json::to_value(filter).unwrap();
        assert_eq!(value["filter-lang"], "cql2-text");
        assert!(value.get("filter").is_some());
    }

    #[test]
    fn expr() {
        let filter = Filter::Cql2Text(
            "id='LC08_L1TP_060247_20180905_20180912_01_T1_L1TP' AND collection='landsat8_l1tp'"
                .to_string(),
        );
        let _: Expr = filter.try_into().unwrap();
    }
}
