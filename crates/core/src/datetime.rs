//! Datetime utilities.

use crate::{Error, Result};
use chrono::{DateTime, FixedOffset};

/// A start and end datetime.
pub type Interval = (Option<DateTime<FixedOffset>>, Option<DateTime<FixedOffset>>);

/// Parse a datetime or datetime interval into a start and end datetime.
///
/// Returns `None` to indicate an open interval.
///
/// # Examples
///
/// ```
/// let (start, end) = stac::datetime::parse("2023-07-11T12:00:00Z/..").unwrap();
/// assert!(start.is_some());
/// assert!(end.is_none());
/// ```
pub fn parse(datetime: &str) -> Result<Interval> {
    if datetime.contains('/') {
        let mut iter = datetime.split('/');
        let start = iter
            .next()
            .ok_or_else(|| Error::InvalidDatetime(datetime.to_string()))
            .and_then(parse_one)?;
        let end = iter
            .next()
            .ok_or_else(|| Error::InvalidDatetime(datetime.to_string()))
            .and_then(parse_one)?;
        if iter.next().is_some() {
            return Err(Error::InvalidDatetime(datetime.to_string()));
        }
        Ok((start, end))
    } else if datetime == ".." {
        Err(Error::InvalidDatetime(datetime.to_string()))
    } else {
        let datetime = DateTime::parse_from_rfc3339(datetime).map(Some)?;
        Ok((datetime, datetime))
    }
}

fn parse_one(s: &str) -> Result<Option<DateTime<FixedOffset>>> {
    if s == ".." {
        Ok(None)
    } else if s.is_empty() {
        log::warn!("an empty string in a datetime interval are invalid, converting to \"..\"");
        Ok(None)
    } else {
        DateTime::parse_from_rfc3339(s)
            .map(Some)
            .map_err(Error::from)
    }
}

mod tests {
    #[test]
    fn empty_interval() {
        let _ = super::parse("2024-04-27T00:00:00Z/").unwrap();
        let _ = super::parse("/2024-04-27T00:00:00Z").unwrap();
    }
}
