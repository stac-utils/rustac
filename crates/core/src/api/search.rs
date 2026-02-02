use super::{Fields, GetItems, Items, Result, Sortby};
use crate::Error;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime, TimeZone, Utc};
use geojson::Geometry;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use stac::{Bbox, Item};
use std::ops::{Deref, DerefMut};

/// The core parameters for STAC search are defined by OAFeat, and STAC adds a few parameters for convenience.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Search {
    /// Many fields are shared with [Items], so we re-use that structure.
    #[serde(flatten)]
    pub items: Items,

    /// Searches items by performing intersection between their geometry and provided GeoJSON geometry.
    ///
    /// All GeoJSON geometry types must be supported.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intersects: Option<Geometry>,

    /// Array of Item ids to return.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub ids: Vec<String>,

    /// Array of one or more Collection IDs that each matching Item must be in.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub collections: Vec<String>,
}

/// GET parameters for the item search endpoint.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct GetSearch {
    /// Many fields are shared with [Items], so we re-use that structure.
    #[serde(flatten)]
    pub items: GetItems,

    /// Searches items by performing intersection between their geometry and provided GeoJSON geometry.
    ///
    /// All GeoJSON geometry types must be supported.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intersects: Option<String>,

    /// Comma-delimited list of Item ids to return.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids: Option<String>,

    /// Comma-delimited list of one or more Collection IDs that each matching Item must be in.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collections: Option<String>,
}

impl Search {
    /// Creates a new, empty search.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Search;
    ///
    /// let search = Search::new();
    /// ```
    pub fn new() -> Search {
        Search::default()
    }

    /// Sets the ids field of this search.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Search;
    /// let search = Search::new().ids(vec!["an-id".to_string()]);
    /// ```
    pub fn ids(mut self, ids: Vec<String>) -> Search {
        self.ids = ids;
        self
    }

    /// Sets the intersects of this search.
    pub fn intersects(mut self, intersects: impl Into<Geometry>) -> Search {
        self.intersects = Some(intersects.into());
        self
    }

    /// Sets the collections of this search.
    pub fn collections(mut self, collections: Vec<String>) -> Search {
        self.collections = collections;
        self
    }

    /// Sets the bbox of this search.
    pub fn bbox(mut self, bbox: impl Into<Bbox>) -> Search {
        self.items.bbox = Some(bbox.into());
        self
    }

    /// Sets the datetime of this search.
    pub fn datetime(mut self, datetime: impl ToString) -> Search {
        self.items.datetime = Some(datetime.to_string());
        self
    }

    /// Sets the limit of this search.
    pub fn limit(mut self, limit: u64) -> Search {
        self.items.limit = Some(limit);
        self
    }

    /// Sets the sortby of this search.
    pub fn sortby(mut self, sortby: Vec<Sortby>) -> Search {
        self.items.sortby = sortby;
        self
    }

    /// Sets the fields of this search.
    pub fn fields(mut self, fields: Fields) -> Search {
        self.items.fields = Some(fields);
        self
    }

    /// Returns an error if this search is invalid, e.g. if both bbox and intersects are specified.
    ///
    /// Returns the search unchanged if it is valid.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Search;
    /// use geojson::{Geometry, Value};
    ///
    /// let mut search = Search::default();
    /// search.items.bbox =  Some(vec![-180.0, -90.0, 180.0, 80.0].try_into().unwrap());
    /// search = search.valid().unwrap();
    /// search.intersects = Some(Geometry::new(Value::Point(vec![0.0, 0.0])));
    /// search.valid().unwrap_err();
    /// ```
    pub fn valid(mut self) -> Result<Search> {
        self.items = self.items.valid()?;
        if self.items.bbox.is_some() & self.intersects.is_some() {
            Err(Error::SearchHasBboxAndIntersects(Box::new(self.clone())))
        } else {
            Ok(self)
        }
    }

    /// Returns true if this item matches this search.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac::api::Search;
    ///
    /// let item = Item::new("an-id");
    /// assert!(Search::new().matches(&item).unwrap());
    /// assert!(!Search::new().ids(vec!["not-the-id".to_string()]).matches(&item).unwrap());
    /// ```
    pub fn matches(&self, item: &Item) -> Result<bool> {
        Ok(self.collection_matches(item)
            & self.id_matches(item)
            & self.intersects_matches(item)?
            & self.items.matches(item)?)
    }

    /// Returns true if this item's collection matches this search.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Search;
    /// use stac::Item;
    ///
    /// let mut search = Search::new();
    /// let mut item = Item::new("item-id");
    /// assert!(search.collection_matches(&item));
    /// search.collections = vec!["collection-id".to_string()];
    /// assert!(!search.collection_matches(&item));
    /// item.collection = Some("collection-id".to_string());
    /// assert!(search.collection_matches(&item));
    /// item.collection = Some("another-collection-id".to_string());
    /// assert!(!search.collection_matches(&item));
    /// ```
    pub fn collection_matches(&self, item: &Item) -> bool {
        if self.collections.is_empty() {
            true
        } else if let Some(collection) = item.collection.as_ref() {
            self.collections.contains(collection)
        } else {
            false
        }
    }

    /// Returns true if this item's id matches this search.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Search;
    /// use stac::Item;
    ///
    /// let mut search = Search::new();
    /// let mut item = Item::new("item-id");
    /// assert!(search.id_matches(&item));
    /// search.ids = vec!["item-id".to_string()];
    /// assert!(search.id_matches(&item));
    /// search.ids = vec!["another-id".to_string()];
    /// assert!(!search.id_matches(&item));
    /// ```
    pub fn id_matches(&self, item: &Item) -> bool {
        self.ids.is_empty() || self.ids.contains(&item.id)
    }

    /// Returns true if this item's geometry matches this search's intersects.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geo")]
    /// # {
    /// use stac::api::Search;
    /// use stac::Item;
    /// use geojson::{Geometry, Value};
    ///
    /// let mut search = Search::new();
    /// let mut item = Item::new("item-id");
    /// assert!(search.intersects_matches(&item).unwrap());
    /// search.intersects = Some(Geometry::new(Value::Point(vec![-105.1, 41.1])));
    /// assert!(!search.intersects_matches(&item).unwrap());
    /// item.set_geometry(Geometry::new(Value::Point(vec![-105.1, 41.1])));
    /// assert!(search.intersects_matches(&item).unwrap());
    /// # }
    /// ```
    #[allow(unused_variables)]
    pub fn intersects_matches(&self, item: &Item) -> Result<bool> {
        match self.intersects.clone() {
            Some(intersects) => {
                #[cfg(feature = "geo")]
                {
                    let intersects: geo::Geometry = intersects.try_into().map_err(Box::new)?;
                    item.intersects(&intersects)
                }
                #[cfg(not(feature = "geo"))]
                {
                    Err(Error::FeatureNotEnabled("geo"))
                }
            }
            _ => Ok(true),
        }
    }

    /// Converts this search's filter to cql2-json, if set.
    pub fn into_cql2_json(mut self) -> Result<Search> {
        self.items = self.items.into_cql2_json()?;
        Ok(self)
    }

    /// Normalizes datetime parameters by expanding partial dates to full RFC 3339 datetime ranges.
    ///
    /// This method validates and normalizes datetime parameters by expanding partial dates (year, year-month, year-month-day) to full RFC 3339 datetime ranges:
    /// - For single dates:
    ///   - Year only (e.g., "2023") → 2023-01-01T00:00:00Z/2023-12-31T23:59:59Z
    ///   - Year-Month (e.g., "2023-06") → 2023-06-01T00:00:00Z/2023-06-30T23:59:59Z
    ///   - ISO 8601 date (e.g., "2023-06-15") → 2023-06-15T00:00:00Z/2023-06-15T23:59:59Z
    /// - For date ranges:
    ///   - Year to Year (e.g., "2017/2018") → 2017-01-01T00:00:00Z/2018-12-31T23:59:59Z
    ///   - Year-Month to Year-Month (e.g., "2017-06/2017-07") → 2017-06-01T00:00:00Z/2017-07-31T23:59:59Z
    ///   - Date to Date (e.g., "2017-06-10/2017-06-11") → 2017-06-10T00:00:00Z/2017-06-11T23:59:59Z
    ///
    /// Datetime values already in RFC 3339 format are preserved. Open-ended ranges using `..` are supported.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Search;
    ///
    /// // Partial dates are expanded to ranges
    /// let search = Search {
    ///     items: stac::api::Items {
    ///         datetime: Some("2023".to_string()),
    ///         ..Default::default()
    ///     },
    ///     ..Default::default()
    /// };
    /// let normalized = search.normalize_datetimes().unwrap();
    /// assert_eq!(
    ///     normalized.datetime.as_ref().unwrap(),
    ///     "2023-01-01T00:00:00+00:00/2023-12-31T23:59:59+00:00"
    /// );
    ///
    /// // RFC 3339 datetimes are preserved as single values
    /// let search = Search {
    ///     items: stac::api::Items {
    ///         datetime: Some("2023-06-01T00:00:00Z".to_string()),
    ///         ..Default::default()
    ///     },
    ///     ..Default::default()
    /// };
    /// let normalized = search.normalize_datetimes().unwrap();
    /// assert_eq!(
    ///     normalized.datetime.as_ref().unwrap(),
    ///     "2023-06-01T00:00:00+00:00"
    /// );
    /// ```
    pub fn normalize_datetimes(mut self) -> Result<Search> {
        if let Some(datetime) = self.datetime.as_deref() {
            if let Some((start_str, end_str)) = datetime.split_once('/') {
                // Start and end datetime range
                let start = if start_str.is_empty() || start_str == ".." {
                    None
                } else {
                    Some(
                        DateTime::parse_from_rfc3339(start_str)
                            .or_else(|_| expand_datetime_to_start(start_str))?,
                    )
                };

                let end = if end_str.is_empty() || end_str == ".." {
                    None
                } else {
                    Some(
                        DateTime::parse_from_rfc3339(end_str)
                            .or_else(|_| expand_datetime_to_end(end_str))?,
                    )
                };

                if let Some(start) = start {
                    if let Some(end) = end {
                        if end < start {
                            return Err(Error::StartIsAfterEnd(start, end));
                        }
                        self.datetime =
                            Some(format!("{}/{}", start.to_rfc3339(), end.to_rfc3339()));
                    } else {
                        // Open end datetime
                        self.datetime = Some(format!("{}/..", start.to_rfc3339()));
                    }
                } else if let Some(end) = end {
                    // Open start datetime
                    self.datetime = Some(format!("../{}", end.to_rfc3339()));
                } else {
                    return Err(Error::EmptyDatetimeInterval);
                }
            } else {
                // Single datetime
                if let Ok(parsed) = DateTime::parse_from_rfc3339(datetime) {
                    self.datetime = Some(parsed.to_rfc3339());
                } else {
                    let start = expand_datetime_to_start(datetime)?;
                    let end = expand_datetime_to_end(datetime)?;
                    self.datetime = Some(format!("{}/{}", start.to_rfc3339(), end.to_rfc3339()));
                }
            }
        }
        Ok(self)
    }
}

/// Expands a partial datetime string to the start of the period.
fn expand_datetime_to_start(s: &str) -> Result<DateTime<FixedOffset>> {
    let trimmed = s.trim();
    let midnight = NaiveTime::from_hms_opt(0, 0, 0).expect("midnight (0, 0, 0) is always valid");

    // Case 1: Year only (e.g., "2023") -> 2023-01-01T00:00:00Z
    if trimmed.len() == 4
        && trimmed.chars().all(|c| c.is_numeric())
        && let Ok(year) = trimmed.parse::<i32>()
    {
        let date = NaiveDate::from_ymd_opt(year, 1, 1).ok_or(Error::InvalidYear(year))?;
        let datetime = date.and_time(midnight);
        return Ok(Utc.from_utc_datetime(&datetime).fixed_offset());
    }

    // Case 2: Year-Month (e.g., "2023-01") -> 2023-01-01T00:00:00Z
    if trimmed.len() == 7
        && trimmed.chars().nth(4) == Some('-')
        && let Some((year_str, month_str)) = trimmed.split_once('-')
        && let (Ok(year), Ok(month)) = (year_str.parse::<i32>(), month_str.parse::<u32>())
        && (1..=12).contains(&month)
    {
        let date = NaiveDate::from_ymd_opt(year, month, 1).ok_or(Error::InvalidYear(year))?;
        let datetime = date.and_time(midnight);
        return Ok(Utc.from_utc_datetime(&datetime).fixed_offset());
    }

    // Case 3: ISO 8601 date (e.g., "2023-06-15") -> 2023-06-15T00:00:00Z
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        let datetime = date.and_time(midnight);
        return Ok(Utc.from_utc_datetime(&datetime).fixed_offset());
    }

    Err(Error::UnrecognizedDateFormat(s.to_string()))
}

/// Expands a partial datetime string to the end of the period.
fn expand_datetime_to_end(s: &str) -> Result<DateTime<FixedOffset>> {
    let trimmed = s.trim();
    let end_of_day = NaiveTime::from_hms_opt(23, 59, 59).expect("23:59:59 is always valid");

    // Case 1: Year only (e.g., "2023") -> 2023-12-31T23:59:59Z
    if trimmed.len() == 4
        && trimmed.chars().all(|c| c.is_numeric())
        && let Ok(year) = trimmed.parse::<i32>()
    {
        let date = NaiveDate::from_ymd_opt(year, 12, 31).ok_or(Error::InvalidYear(year))?;
        let datetime = date.and_time(end_of_day);
        return Ok(Utc.from_utc_datetime(&datetime).fixed_offset());
    }

    // Case 2: Year-Month (e.g., "2023-01") -> 2023-01-31T23:59:59Z (last day of month)
    if trimmed.len() == 7
        && trimmed.chars().nth(4) == Some('-')
        && let Some((year_str, month_str)) = trimmed.split_once('-')
        && let (Ok(year), Ok(month)) = (year_str.parse::<i32>(), month_str.parse::<u32>())
        && (1..=12).contains(&month)
    {
        // Get the last day of the month by going to first day of next month, then back one day
        let last_day = if month == 12 {
            NaiveDate::from_ymd_opt(year + 1, 1, 1)
        } else {
            NaiveDate::from_ymd_opt(year, month + 1, 1)
        }
        .ok_or(Error::InvalidYear(year))?
        .pred_opt()
        .ok_or(Error::InvalidYear(year))?;

        let datetime = last_day.and_time(end_of_day);
        return Ok(Utc.from_utc_datetime(&datetime).fixed_offset());
    }

    // Case 3: ISO 8601 date (e.g., "2023-06-15") -> 2023-06-15T23:59:59Z
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        let datetime = date.and_time(end_of_day);
        return Ok(Utc.from_utc_datetime(&datetime).fixed_offset());
    }

    Err(Error::UnrecognizedDateFormat(s.to_string()))
}

impl TryFrom<Search> for GetSearch {
    type Error = Error;

    fn try_from(search: Search) -> Result<GetSearch> {
        let get_items: GetItems = search.items.try_into()?;
        let intersects = search
            .intersects
            .map(|intersects| serde_json::to_string(&intersects))
            .transpose()?;
        let collections = if search.collections.is_empty() {
            None
        } else {
            Some(search.collections.join(","))
        };
        let ids = if search.ids.is_empty() {
            None
        } else {
            Some(search.ids.join(","))
        };
        Ok(GetSearch {
            items: get_items,
            intersects,
            ids,
            collections,
        })
    }
}

impl TryFrom<GetSearch> for Search {
    type Error = Error;

    fn try_from(get_search: GetSearch) -> Result<Search> {
        let items: Items = get_search.items.try_into()?;
        let intersects = get_search
            .intersects
            .map(|intersects| serde_json::from_str(&intersects))
            .transpose()?;
        let collections = get_search
            .collections
            .map(|collections| collections.split(',').map(|s| s.to_string()).collect())
            .unwrap_or_default();
        let ids = get_search
            .ids
            .map(|ids| ids.split(',').map(|s| s.to_string()).collect())
            .unwrap_or_default();
        Ok(Search {
            items,
            intersects,
            ids,
            collections,
        })
    }
}

impl From<Items> for Search {
    fn from(items: Items) -> Self {
        Search {
            items,
            ..Default::default()
        }
    }
}

impl crate::Fields for Search {
    fn fields(&self) -> &Map<String, Value> {
        &self.items.additional_fields
    }
    fn fields_mut(&mut self) -> &mut Map<String, Value> {
        &mut self.items.additional_fields
    }
}

impl Deref for Search {
    type Target = Items;
    fn deref(&self) -> &Self::Target {
        &self.items
    }
}

impl DerefMut for Search {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.items
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetime_year_only_expands_to_full_year() {
        let search = Search {
            items: Items {
                datetime: Some("2023".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let normalized = search.normalize_datetimes().unwrap();
        assert_eq!(
            normalized.datetime.as_ref().unwrap(),
            "2023-01-01T00:00:00+00:00/2023-12-31T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_year_month_expands_to_full_month() {
        let search = Search {
            items: Items {
                datetime: Some("2023-06".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let normalized = search.normalize_datetimes().unwrap();
        assert_eq!(
            normalized.datetime.as_ref().unwrap(),
            "2023-06-01T00:00:00+00:00/2023-06-30T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_date_expands_to_full_day() {
        let search = Search {
            items: Items {
                datetime: Some("2023-06-10".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let normalized = search.normalize_datetimes().unwrap();
        assert_eq!(
            normalized.datetime.as_ref().unwrap(),
            "2023-06-10T00:00:00+00:00/2023-06-10T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_rfc3339_stays_as_single_datetime() {
        let search = Search {
            items: Items {
                datetime: Some("2023-06-01T00:00:00Z".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let normalized = search.normalize_datetimes().unwrap();
        assert_eq!(
            normalized.datetime.as_ref().unwrap(),
            "2023-06-01T00:00:00+00:00"
        );
    }

    #[test]
    fn datetime_range_year_to_year() {
        let search = Search {
            items: Items {
                datetime: Some("2017/2018".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let normalized = search.normalize_datetimes().unwrap();
        assert_eq!(
            normalized.datetime.as_ref().unwrap(),
            "2017-01-01T00:00:00+00:00/2018-12-31T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_range_year_month_to_year_month() {
        let search = Search {
            items: Items {
                datetime: Some("2017-06/2017-07".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let normalized = search.normalize_datetimes().unwrap();
        assert_eq!(
            normalized.datetime.as_ref().unwrap(),
            "2017-06-01T00:00:00+00:00/2017-07-31T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_range_date_to_date() {
        let search = Search {
            items: Items {
                datetime: Some("2017-06-10/2017-06-11".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let normalized = search.normalize_datetimes().unwrap();
        assert_eq!(
            normalized.datetime.as_ref().unwrap(),
            "2017-06-10T00:00:00+00:00/2017-06-11T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_open_end_range() {
        let search = Search {
            items: Items {
                datetime: Some("2020-01-01/..".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let normalized = search.normalize_datetimes().unwrap();
        assert_eq!(
            normalized.datetime.as_ref().unwrap(),
            "2020-01-01T00:00:00+00:00/.."
        );
    }

    #[test]
    fn datetime_open_start_range() {
        let search = Search {
            items: Items {
                datetime: Some("../2020-12-31".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let normalized = search.normalize_datetimes().unwrap();
        assert_eq!(
            normalized.datetime.as_ref().unwrap(),
            "../2020-12-31T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_february_leap_year() {
        let search = Search {
            items: Items {
                datetime: Some("2024-02".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let normalized = search.normalize_datetimes().unwrap();
        assert_eq!(
            normalized.datetime.as_ref().unwrap(),
            "2024-02-01T00:00:00+00:00/2024-02-29T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_february_non_leap_year() {
        let search = Search {
            items: Items {
                datetime: Some("2023-02".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let normalized = search.normalize_datetimes().unwrap();
        assert_eq!(
            normalized.datetime.as_ref().unwrap(),
            "2023-02-01T00:00:00+00:00/2023-02-28T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_range_rfc3339_to_rfc3339() {
        let search = Search {
            items: Items {
                datetime: Some("2023-01-01T00:00:00Z/2023-12-31T23:59:59Z".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        let normalized = search.normalize_datetimes().unwrap();
        assert_eq!(
            normalized.datetime.as_ref().unwrap(),
            "2023-01-01T00:00:00+00:00/2023-12-31T23:59:59+00:00"
        );
    }
}
