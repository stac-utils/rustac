use super::{Fields, Filter, Result, Search, Sortby};
use crate::Error;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime, TimeZone, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use stac::{Bbox, Item};

/// Parameters for the items endpoint from STAC API - Features.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Items {
    /// The maximum number of results to return (page size).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    /// Requested bounding box.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<Bbox>,

    /// Single date+time, or a range ('/' separator), formatted to [RFC 3339,
    /// section 5.6](https://tools.ietf.org/html/rfc3339#section-5.6).
    ///
    /// Use double dots `..` for open date ranges.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datetime: Option<String>,

    /// Include/exclude fields from item collections.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Fields>,

    /// Fields by which to sort results.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub sortby: Vec<Sortby>,

    /// Recommended to not be passed, but server must only accept
    /// <http://www.opengis.net/def/crs/OGC/1.3/CRS84> as a valid value, may
    /// reject any others
    #[serde(skip_serializing_if = "Option::is_none", rename = "filter-crs")]
    pub filter_crs: Option<String>,

    /// CQL2 filter expression.
    #[serde(skip_serializing_if = "Option::is_none", flatten)]
    pub filter: Option<Filter>,

    /// Additional filtering based on properties.
    ///
    /// It is recommended to use the filter extension instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<Map<String, Value>>,

    /// Additional fields.
    #[serde(flatten)]
    pub additional_fields: Map<String, Value>,
}

/// GET parameters for the items endpoint from STAC API - Features.
///
/// This is a lot like [Search](crate::api::Search), but without intersects, ids, and
/// collections.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct GetItems {
    /// The maximum number of results to return (page size).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<String>,

    /// Requested bounding box.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<String>,

    /// Single date+time, or a range ('/' separator), formatted to [RFC 3339,
    /// section 5.6](https://tools.ietf.org/html/rfc3339#section-5.6).
    ///
    /// Use double dots `..` for open date ranges.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datetime: Option<String>,

    /// Include/exclude fields from item collections.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<String>,

    /// Fields by which to sort results.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sortby: Option<String>,

    /// Recommended to not be passed, but server must only accept
    /// <http://www.opengis.net/def/crs/OGC/1.3/CRS84> as a valid value, may
    /// reject any others
    #[serde(skip_serializing_if = "Option::is_none", rename = "filter-crs")]
    pub filter_crs: Option<String>,

    /// This should always be cql2-text if present.
    #[serde(skip_serializing_if = "Option::is_none", rename = "filter-lang")]
    pub filter_lang: Option<String>,

    /// CQL2 filter expression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,

    /// Additional fields.
    #[serde(flatten)]
    pub additional_fields: IndexMap<String, String>,
}

impl Items {
    /// Runs a set of validity checks on this query and returns an error if it is invalid.
    ///
    /// Returns the items, with normalized datetime values, if it is valid.
    ///
    /// This method validates and normalizes datetime parameters by expanding partial dates (year, year-month, year-month-day) to full RFC 3339 datetime ranges:
    ///     - For single dates:
    ///       - Year only (e.g., "2023") → 2023-01-01T00:00:00Z/2023-12-31T23:59:59Z
    ///       - Year-Month (e.g., "2023-06") → 2023-06-01T00:00:00Z/2023-06-30T23:59:59Z
    ///       - ISO 8601 date (e.g., "2023-06-15") → 2023-06-15T00:00:00Z/2023-06-15T23:59:59Z
    ///     - For date ranges:
    ///       - Year to Year (e.g., "2017/2018") → 2017-01-01T00:00:00Z/2018-12-31T23:59:59Z
    ///       - Year-Month to Year-Month (e.g., "2017-06/2017-07") → 2017-06-01T00:00:00Z/2017-07-31T23:59:59Z
    ///       - Date to Date (e.g., "2017-06-10/2017-06-11") → 2017-06-10T00:00:00Z/2017-06-11T23:59:59Z
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Items;
    ///
    /// let items = Items::default().valid().unwrap();
    ///
    /// // Partial dates are expanded to ranges
    /// let items = Items {
    ///     datetime: Some("2023".to_string()),
    ///     ..Default::default()
    /// };
    /// let validated = items.valid().unwrap();
    /// assert_eq!(
    ///     validated.datetime.unwrap(),
    ///     "2023-01-01T00:00:00+00:00/2023-12-31T23:59:59+00:00"
    /// );
    /// ```
    pub fn valid(mut self) -> Result<Items> {
        if let Some(bbox) = self.bbox.as_ref() {
            if !bbox.is_valid() {
                return Err(Error::InvalidBbox((*bbox).into(), "invalid bbox"));
            }
        }
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

    /// Returns true if this items structure matches the given item.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Items;
    /// use stac::Item;
    ///
    /// assert!(Items::default().matches(&Item::new("an-id")).unwrap());
    /// ```
    pub fn matches(&self, item: &Item) -> Result<bool> {
        Ok(self.bbox_matches(item)?
            & self.datetime_matches(item)?
            & self.query_matches(item)?
            & self.filter_matches(item)?)
    }

    /// Returns true if this item's geometry matches this search's bbox.
    ///
    /// If **stac** is not built with the `geo` feature, this will return an error.
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
    /// assert!(search.bbox_matches(&item).unwrap());
    /// search.bbox = Some(vec![-110.0, 40.0, -100.0, 50.0].try_into().unwrap());
    /// assert!(!search.bbox_matches(&item).unwrap());
    /// item.set_geometry(Geometry::new(Value::Point(vec![-105.1, 41.1])));
    /// assert!(search.bbox_matches(&item).unwrap());
    /// # }
    /// ```
    #[allow(unused_variables)]
    pub fn bbox_matches(&self, item: &Item) -> Result<bool> {
        if let Some(bbox) = self.bbox.as_ref() {
            #[cfg(feature = "geo")]
            {
                let bbox: geo::Rect = (*bbox).into();
                item.intersects(&bbox).map_err(Error::from)
            }
            #[cfg(not(feature = "geo"))]
            {
                Err(Error::FeatureNotEnabled("geo"))
            }
        } else {
            Ok(true)
        }
    }

    /// Returns true if this item's datetime matches this items structure.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Search;
    /// use stac::Item;
    ///
    /// let mut search = Search::new();
    /// let mut item = Item::new("item-id");  // default datetime is now
    /// assert!(search.datetime_matches(&item).unwrap());
    /// search.datetime = Some("../2023-10-09T00:00:00Z".to_string());
    /// assert!(!search.datetime_matches(&item).unwrap());
    /// item.properties.datetime = Some("2023-10-08T00:00:00Z".parse().unwrap());
    /// assert!(search.datetime_matches(&item).unwrap());
    /// ```
    pub fn datetime_matches(&self, item: &Item) -> Result<bool> {
        if let Some(datetime) = self.datetime.as_ref() {
            item.intersects_datetime_str(datetime).map_err(Error::from)
        } else {
            Ok(true)
        }
    }

    /// Returns true if this item's matches this search query.
    ///
    /// Currently unsupported, always raises an error if query is set.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Search;
    /// use stac::Item;
    ///
    /// let mut search = Search::new();
    /// let mut item = Item::new("item-id");
    /// assert!(search.query_matches(&item).unwrap());
    /// search.query = Some(Default::default());
    /// assert!(search.query_matches(&item).is_err());
    /// ```
    pub fn query_matches(&self, _: &Item) -> Result<bool> {
        if self.query.as_ref().is_some() {
            // TODO implement
            Err(Error::Unimplemented("query"))
        } else {
            Ok(true)
        }
    }

    /// Returns true if this item matches this search's filter.
    ///
    /// Currently unsupported, always raises an error if filter is set.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Search;
    /// use stac::Item;
    ///
    /// let mut search = Search::new();
    /// let mut item = Item::new("item-id");
    /// assert!(search.filter_matches(&item).unwrap());
    /// search.filter = Some(Default::default());
    /// assert!(search.filter_matches(&item).is_err());
    /// ```
    pub fn filter_matches(&self, _: &Item) -> Result<bool> {
        if self.filter.as_ref().is_some() {
            // TODO implement
            Err(Error::Unimplemented("filter"))
        } else {
            Ok(true)
        }
    }

    /// Converts this items object to a search in the given collection.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Items;
    /// let items = Items {
    ///     datetime: Some("2023".to_string()),
    ///     ..Default::default()
    /// };
    /// let search = items.search_collection("collection-id");
    /// assert_eq!(search.collections, vec!["collection-id"]);
    /// ```
    pub fn search_collection(self, collection_id: impl ToString) -> Search {
        Search {
            items: self,
            intersects: None,
            ids: Vec::new(),
            collections: vec![collection_id.to_string()],
        }
    }

    /// Converts the filter to cql2-json, if it is set.
    pub fn into_cql2_json(mut self) -> Result<Items> {
        if let Some(filter) = self.filter {
            self.filter = Some(filter.into_cql2_json()?);
        }
        Ok(self)
    }
}

impl TryFrom<Items> for GetItems {
    type Error = Error;

    fn try_from(items: Items) -> Result<GetItems> {
        if let Some(query) = items.query {
            return Err(Error::CannotConvertQueryToString(query));
        }
        let filter = if let Some(filter) = items.filter {
            match filter {
                Filter::Cql2Json(json) => {
                    return Err(Error::CannotConvertCql2JsonToString(json));
                }
                Filter::Cql2Text(text) => Some(text),
            }
        } else {
            None
        };
        Ok(GetItems {
            limit: items.limit.map(|n| n.to_string()),
            bbox: items.bbox.map(|bbox| {
                Vec::from(bbox)
                    .into_iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            }),
            datetime: items.datetime,
            fields: items.fields.map(|fields| fields.to_string()),
            sortby: if items.sortby.is_empty() {
                None
            } else {
                Some(
                    items
                        .sortby
                        .into_iter()
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                )
            },
            filter_crs: items.filter_crs,
            filter_lang: if filter.is_some() {
                Some("cql2-text".to_string())
            } else {
                None
            },
            filter,
            additional_fields: items
                .additional_fields
                .into_iter()
                .map(|(key, value)| (key, value.to_string()))
                .collect(),
        })
    }
}

impl TryFrom<GetItems> for Items {
    type Error = Error;

    fn try_from(get_items: GetItems) -> Result<Items> {
        let bbox = if let Some(value) = get_items.bbox {
            let mut bbox = Vec::new();
            for s in value.split(',') {
                bbox.push(s.parse()?)
            }
            Some(bbox.try_into()?)
        } else {
            None
        };

        let sortby = get_items
            .sortby
            .map(|s| {
                let mut sortby = Vec::new();
                for s in s.split(',') {
                    sortby.push(s.parse().expect("infallible"));
                }
                sortby
            })
            .unwrap_or_default();

        Ok(Items {
            limit: get_items.limit.map(|limit| limit.parse()).transpose()?,
            bbox,
            datetime: get_items.datetime,
            fields: get_items
                .fields
                .map(|fields| fields.parse().expect("infallible")),
            sortby,
            filter_crs: get_items.filter_crs,
            filter: get_items.filter.map(Filter::Cql2Text),
            query: None,
            additional_fields: get_items
                .additional_fields
                .into_iter()
                .map(|(key, value)| (key, Value::String(value)))
                .collect(),
        })
    }
}

impl crate::Fields for Items {
    fn fields(&self) -> &Map<String, Value> {
        &self.additional_fields
    }
    fn fields_mut(&mut self) -> &mut Map<String, Value> {
        &mut self.additional_fields
    }
}

/// Expands a partial datetime string to the start of the period.
fn expand_datetime_to_start(s: &str) -> Result<DateTime<FixedOffset>> {
    let trimmed = s.trim();
    let midnight = NaiveTime::from_hms_opt(0, 0, 0).expect("midnight (0, 0, 0) is always valid");

    // Case 1: Year only (e.g., "2023") -> 2023-01-01T00:00:00Z
    if trimmed.len() == 4 && trimmed.chars().all(|c| c.is_numeric()) {
        if let Ok(year) = trimmed.parse::<i32>() {
            let date = NaiveDate::from_ymd_opt(year, 1, 1).ok_or(Error::InvalidYear(year))?;
            let datetime = date.and_time(midnight);
            return Ok(Utc.from_utc_datetime(&datetime).fixed_offset());
        }
    }

    // Case 2: Year-Month (e.g., "2023-01") -> 2023-01-01T00:00:00Z
    if trimmed.len() == 7 && trimmed.chars().nth(4) == Some('-') {
        if let Some((year_str, month_str)) = trimmed.split_once('-') {
            if let (Ok(year), Ok(month)) = (year_str.parse::<i32>(), month_str.parse::<u32>()) {
                if month >= 1 && month <= 12 {
                    let date =
                        NaiveDate::from_ymd_opt(year, month, 1).ok_or(Error::InvalidYear(year))?;
                    let datetime = date.and_time(midnight);
                    return Ok(Utc.from_utc_datetime(&datetime).fixed_offset());
                }
            }
        }
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
    if trimmed.len() == 4 && trimmed.chars().all(|c| c.is_numeric()) {
        if let Ok(year) = trimmed.parse::<i32>() {
            let date = NaiveDate::from_ymd_opt(year, 12, 31).ok_or(Error::InvalidYear(year))?;
            let datetime = date.and_time(end_of_day);
            return Ok(Utc.from_utc_datetime(&datetime).fixed_offset());
        }
    }

    // Case 2: Year-Month (e.g., "2023-01") -> 2023-01-31T23:59:59Z (last day of month)
    if trimmed.len() == 7 && trimmed.chars().nth(4) == Some('-') {
        if let Some((year_str, month_str)) = trimmed.split_once('-') {
            if let (Ok(year), Ok(month)) = (year_str.parse::<i32>(), month_str.parse::<u32>()) {
                if month >= 1 && month <= 12 {
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
            }
        }
    }

    // Case 3: ISO 8601 date (e.g., "2023-06-15") -> 2023-06-15T23:59:59Z
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        let datetime = date.and_time(end_of_day);
        return Ok(Utc.from_utc_datetime(&datetime).fixed_offset());
    }

    Err(Error::UnrecognizedDateFormat(s.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{GetItems, Items};
    use crate::api::{Fields, Filter, Sortby, sort::Direction};
    use indexmap::IndexMap;
    use serde_json::{Map, Value, json};

    #[test]
    fn get_items_try_from_items() {
        let mut additional_fields = IndexMap::new();
        let _ = additional_fields.insert("token".to_string(), "foobar".to_string());

        let get_items = GetItems {
            limit: Some("42".to_string()),
            bbox: Some("-1,-2,1,2".to_string()),
            datetime: Some("2023".to_string()),
            fields: Some("+foo,-bar".to_string()),
            sortby: Some("-foo".to_string()),
            filter_crs: None,
            filter_lang: Some("cql2-text".to_string()),
            filter: Some("dummy text".to_string()),
            additional_fields,
        };

        let items: Items = get_items.try_into().unwrap();
        assert_eq!(items.limit.unwrap(), 42);
        assert_eq!(
            items.bbox.unwrap(),
            vec![-1.0, -2.0, 1.0, 2.0].try_into().unwrap()
        );
        assert_eq!(items.datetime.unwrap(), "2023");
        assert_eq!(
            items.fields.unwrap(),
            Fields {
                include: vec!["foo".to_string()],
                exclude: vec!["bar".to_string()],
            }
        );
        assert_eq!(
            items.sortby,
            vec![Sortby {
                field: "foo".to_string(),
                direction: Direction::Descending,
            }]
        );
        assert_eq!(
            items.filter.unwrap(),
            Filter::Cql2Text("dummy text".to_string())
        );
        assert_eq!(items.additional_fields["token"], "foobar");
    }

    #[test]
    fn items_try_from_get_items() {
        let mut additional_fields = Map::new();
        let _ = additional_fields.insert("token".to_string(), Value::String("foobar".to_string()));

        let items = Items {
            limit: Some(42),
            bbox: Some(vec![-1.0, -2.0, 1.0, 2.0].try_into().unwrap()),
            datetime: Some("2023".to_string()),
            fields: Some(Fields {
                include: vec!["foo".to_string()],
                exclude: vec!["bar".to_string()],
            }),
            sortby: vec![Sortby {
                field: "foo".to_string(),
                direction: Direction::Descending,
            }],
            filter_crs: None,
            filter: Some(Filter::Cql2Text("dummy text".to_string())),
            query: None,
            additional_fields,
        };

        let get_items: GetItems = items.try_into().unwrap();
        assert_eq!(get_items.limit.unwrap(), "42");
        assert_eq!(get_items.bbox.unwrap(), "-1,-2,1,2");
        assert_eq!(get_items.datetime.unwrap(), "2023");
        assert_eq!(get_items.fields.unwrap(), "foo,-bar");
        assert_eq!(get_items.sortby.unwrap(), "-foo");
        assert_eq!(get_items.filter.unwrap(), "dummy text");
        assert_eq!(get_items.additional_fields["token"], "\"foobar\"");
    }

    #[test]
    fn filter() {
        let value = json!({
            "filter": "eo:cloud_cover >= 5 AND eo:cloud_cover < 10",
            "filter-lang": "cql2-text",
        });
        let items: Items = serde_json::from_value(value).unwrap();
        assert!(items.filter.is_some());
    }

    #[test]
    fn datetime_year_only_expands_to_full_year() {
        let items = Items {
            datetime: Some("2023".to_string()),
            ..Default::default()
        };
        let validated = items.valid().unwrap();
        assert_eq!(
            validated.datetime.unwrap(),
            "2023-01-01T00:00:00+00:00/2023-12-31T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_year_month_expands_to_full_month() {
        let items = Items {
            datetime: Some("2023-06".to_string()),
            ..Default::default()
        };
        let validated = items.valid().unwrap();
        assert_eq!(
            validated.datetime.unwrap(),
            "2023-06-01T00:00:00+00:00/2023-06-30T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_date_expands_to_full_day() {
        let items = Items {
            datetime: Some("2023-06-10".to_string()),
            ..Default::default()
        };
        let validated = items.valid().unwrap();
        assert_eq!(
            validated.datetime.unwrap(),
            "2023-06-10T00:00:00+00:00/2023-06-10T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_rfc3339_stays_as_single_datetime() {
        let items = Items {
            datetime: Some("2023-06-01T00:00:00Z".to_string()),
            ..Default::default()
        };
        let validated = items.valid().unwrap();
        assert_eq!(validated.datetime.unwrap(), "2023-06-01T00:00:00+00:00");
    }

    #[test]
    fn datetime_range_year_to_year() {
        let items = Items {
            datetime: Some("2017/2018".to_string()),
            ..Default::default()
        };
        let validated = items.valid().unwrap();
        assert_eq!(
            validated.datetime.unwrap(),
            "2017-01-01T00:00:00+00:00/2018-12-31T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_range_year_month_to_year_month() {
        let items = Items {
            datetime: Some("2017-06/2017-07".to_string()),
            ..Default::default()
        };
        let validated = items.valid().unwrap();
        assert_eq!(
            validated.datetime.unwrap(),
            "2017-06-01T00:00:00+00:00/2017-07-31T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_range_date_to_date() {
        let items = Items {
            datetime: Some("2017-06-10/2017-06-11".to_string()),
            ..Default::default()
        };
        let validated = items.valid().unwrap();
        assert_eq!(
            validated.datetime.unwrap(),
            "2017-06-10T00:00:00+00:00/2017-06-11T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_open_end_range() {
        let items = Items {
            datetime: Some("2020-01-01/..".to_string()),
            ..Default::default()
        };
        let validated = items.valid().unwrap();
        assert_eq!(validated.datetime.unwrap(), "2020-01-01T00:00:00+00:00/..");
    }

    #[test]
    fn datetime_open_start_range() {
        let items = Items {
            datetime: Some("../2020-12-31".to_string()),
            ..Default::default()
        };
        let validated = items.valid().unwrap();
        assert_eq!(validated.datetime.unwrap(), "../2020-12-31T23:59:59+00:00");
    }

    #[test]
    fn datetime_february_leap_year() {
        let items = Items {
            datetime: Some("2024-02".to_string()),
            ..Default::default()
        };
        let validated = items.valid().unwrap();
        assert_eq!(
            validated.datetime.unwrap(),
            "2024-02-01T00:00:00+00:00/2024-02-29T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_february_non_leap_year() {
        let items = Items {
            datetime: Some("2023-02".to_string()),
            ..Default::default()
        };
        let validated = items.valid().unwrap();
        assert_eq!(
            validated.datetime.unwrap(),
            "2023-02-01T00:00:00+00:00/2023-02-28T23:59:59+00:00"
        );
    }

    #[test]
    fn datetime_range_rfc3339_to_rfc3339() {
        let items = Items {
            datetime: Some("2023-01-01T00:00:00Z/2023-12-31T23:59:59Z".to_string()),
            ..Default::default()
        };
        let validated = items.valid().unwrap();
        assert_eq!(
            validated.datetime.unwrap(),
            "2023-01-01T00:00:00+00:00/2023-12-31T23:59:59+00:00"
        );
    }
}
