//! Datetime utilities.

use crate::{Error, Result};
use chrono::{DateTime, NaiveDateTime, Utc};

/// A start and end datetime.
pub type Interval = (Option<DateTime<Utc>>, Option<DateTime<Utc>>);

/// Parses a datetime or datetime interval into a start and end datetime.
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
        let datetime = parse_datetime_permissively(datetime).map(Some)?;
        Ok((datetime, datetime))
    }
}

/// Parses a single datetime permissively.
pub fn parse_datetime_permissively(s: &str) -> Result<DateTime<Utc>> {
    match DateTime::parse_from_rfc3339(&s) {
        Ok(datetime) => Ok(datetime.to_utc()),
        Err(err) => {
            log::warn!(
                "error when parsing item datetime as rfc3339 ({err}), trying to parse as naive datetime"
            );
            let (mut datetime, remainder) =
                NaiveDateTime::parse_and_remainder(&s, "%Y-%m-%dT%H:%M:%S")?;
            // This isn't super efficient but we're in a read-invalid-data path, so I think it's fine.
            if !remainder.is_empty() && remainder.starts_with(".") {
                datetime = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f")?;
            }
            Ok(datetime.and_utc())
        }
    }
}

fn parse_one(s: &str) -> Result<Option<DateTime<Utc>>> {
    if s == ".." {
        Ok(None)
    } else if s.is_empty() {
        log::warn!("an empty string in a datetime interval are invalid, converting to \"..\"");
        Ok(None)
    } else {
        parse_datetime_permissively(s).map(Some)
    }
}

mod tests {
    #[test]
    fn empty_interval() {
        let _ = super::parse("2024-04-27T00:00:00Z/").unwrap();
        let _ = super::parse("/2024-04-27T00:00:00Z").unwrap();
    }
}
