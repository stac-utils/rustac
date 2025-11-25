use serde::{Deserialize, Serialize};
use std::{
    convert::Infallible,
    fmt::{Display, Formatter},
    str::FromStr,
};

/// Include/exclude fields from item collections.
///
/// By default, STAC API endpoints that return Item objects return every field
/// of those Items. However, Item objects can have hundreds of fields, or large
/// geometries, and even smaller Item objects can add up when large numbers of
/// them are in results. Frequently, not all fields in an Item are used, so this
/// specification provides a mechanism for clients to request that servers to
/// explicitly include or exclude certain fields.
#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq)]
pub struct Fields {
    /// Fields to include.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<String>,

    /// Fields to exclude.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<String>,
}

impl FromStr for Fields {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut include = Vec::new();
        let mut exclude = Vec::new();
        for field in s.split(',').filter(|s| !s.is_empty()) {
            if let Some(field) = field.strip_prefix('-') {
                exclude.push(field.to_string());
            } else if let Some(field) = field.strip_prefix('+') {
                include.push(field.to_string());
            } else {
                include.push(field.to_string());
            }
        }
        Ok(Fields { include, exclude })
    }
}

impl Display for Fields {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut fields = Vec::new();
        for include in &self.include {
            fields.push(include.to_string());
        }
        for exclude in &self.exclude {
            fields.push(format!("-{exclude}"));
        }
        write!(f, "{}", fields.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::Fields;

    #[test]
    fn empty() {
        assert_eq!(Fields::default(), "".parse().unwrap());
    }

    #[test]
    fn plus() {
        assert_eq!(
            Fields {
                include: vec!["foo".to_string()],
                exclude: Vec::new(),
            },
            "+foo".parse().unwrap()
        );
    }

    #[test]
    fn includes() {
        assert_eq!(
            Fields {
                include: vec![
                    "id".to_string(),
                    "type".to_string(),
                    "geometry".to_string(),
                    "bbox".to_string(),
                    "properties".to_string(),
                    "links".to_string(),
                    "assets".to_string(),
                ],
                exclude: Vec::new()
            },
            "id,type,geometry,bbox,properties,links,assets"
                .parse()
                .unwrap()
        );
        assert_eq!(
            Fields {
                include: vec![
                    "id".to_string(),
                    "type".to_string(),
                    "geometry".to_string(),
                    "bbox".to_string(),
                    "properties".to_string(),
                    "links".to_string(),
                    "assets".to_string(),
                ],
                exclude: Vec::new()
            }
            .to_string(),
            "id,type,geometry,bbox,properties,links,assets"
        )
    }

    #[test]
    fn exclude() {
        assert_eq!(
            Fields {
                include: Vec::new(),
                exclude: vec!["geometry".to_string()]
            },
            "-geometry".parse().unwrap()
        );
        assert_eq!(
            Fields {
                include: Vec::new(),
                exclude: vec!["geometry".to_string()]
            }
            .to_string(),
            "-geometry"
        );
    }
}
