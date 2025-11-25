use serde::{Deserialize, Serialize};
use std::{
    convert::Infallible,
    fmt::{Display, Formatter, Result},
    str::FromStr,
};

/// Fields by which to sort results.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Sortby {
    /// The field to sort by.
    pub field: String,

    /// The direction to sort by.
    pub direction: Direction,
}

/// The direction of sorting.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum Direction {
    /// Ascending
    #[serde(rename = "asc")]
    Ascending,

    /// Descending
    #[serde(rename = "desc")]
    Descending,
}

impl Sortby {
    /// Creates a new ascending sortby for the field.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::api::Sortby;
    /// let sortby = Sortby::asc("id");
    /// ```
    pub fn asc(field: impl ToString) -> Sortby {
        Sortby {
            field: field.to_string(),
            direction: Direction::Ascending,
        }
    }

    /// Creates a new descending sortby for the field.
    ///
    /// # Examples
    ///
    /// ```
    /// # use stac::api::Sortby;
    /// let sortby = Sortby::desc("id");
    /// ```
    pub fn desc(field: impl ToString) -> Sortby {
        Sortby {
            field: field.to_string(),
            direction: Direction::Descending,
        }
    }
}

impl FromStr for Sortby {
    type Err = Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Infallible> {
        if let Some(s) = s.strip_prefix('+') {
            Ok(Sortby::asc(s))
        } else if let Some(s) = s.strip_prefix('-') {
            Ok(Sortby::desc(s))
        } else {
            Ok(Sortby::asc(s))
        }
    }
}

impl Display for Sortby {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        match self.direction {
            Direction::Ascending => write!(f, "{}", self.field),
            Direction::Descending => write!(f, "-{}", self.field),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Sortby;
    use serde_json::json;

    #[test]
    fn optional_plus() {
        assert_eq!(
            "properties.created".parse::<Sortby>().unwrap(),
            "+properties.created".parse().unwrap()
        );
    }

    #[test]
    fn descending() {
        assert_eq!(Sortby::desc("id"), "-id".parse().unwrap());
    }

    #[test]
    fn names() {
        assert_eq!(
            json!({"field": "foo", "direction": "asc"}),
            serde_json::to_value(Sortby::asc("foo")).unwrap()
        );
        assert_eq!(
            json!({"field": "foo", "direction": "desc"}),
            serde_json::to_value(Sortby::desc("foo")).unwrap()
        );
    }
}
