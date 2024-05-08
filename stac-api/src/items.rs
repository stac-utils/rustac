use crate::{Error, Fields, Filter, Result, Search, Sortby};
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use stac::Item;
use std::collections::HashMap;

/// Parameters for the items endpoint from STAC API - Features.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Items {
    /// The maximum number of results to return (page size).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,

    /// Requested bounding box.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<Vec<f64>>,

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sortby: Option<Vec<Sortby>>,

    /// Recommended to not be passed, but server must only accept
    /// <http://www.opengis.net/def/crs/OGC/1.3/CRS84> as a valid value, may
    /// reject any others
    #[serde(skip_serializing_if = "Option::is_none", rename = "filter-crs")]
    pub filter_crs: Option<String>,

    /// CQL2 filter expression.
    #[serde(skip_serializing_if = "Option::is_none")]
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
/// This is a lot like [Search](crate::Search), but without intersects, ids, and
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

    /// CQL2 filter expression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_lang: Option<String>,

    /// CQL2 filter expression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,

    /// Additional fields.
    #[serde(flatten)]
    pub additional_fields: HashMap<String, String>,
}

impl Items {
    /// Runs a set of validity checks on this query and returns an error if it is invalid.
    ///
    /// Returns the items, unchanged, if it is valid.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_api::Items;
    ///
    /// let items = Items::default().valid().unwrap();
    /// ```
    pub fn valid(self) -> Result<Items> {
        if let Some(bbox) = self.bbox.as_ref() {
            if bbox.len() == 4 {
                if bbox[1] > bbox[3] {
                    return Err(Error::InvalidBbox(
                        bbox.clone(),
                        "min latitude is greater than max latitude",
                    ));
                }
            } else if bbox.len() == 6 {
                if bbox[1] > bbox[4] {
                    return Err(Error::InvalidBbox(
                        bbox.clone(),
                        "min latitude is greater than max latitude",
                    ));
                }
            } else {
                return Err(Error::InvalidBbox(
                    bbox.clone(),
                    "invalid number of coordinates",
                ));
            }
        }
        if let Some(datetime) = self.datetime.as_deref() {
            if let Some((start, end)) = datetime.split_once('/') {
                let (start, end) = (
                    maybe_parse_from_rfc3339(start)?,
                    maybe_parse_from_rfc3339(end)?,
                );
                if let Some(start) = start {
                    if let Some(end) = end {
                        if end < start {
                            return Err(Error::StartIsAfterEnd(start, end));
                        }
                    }
                } else if end.is_none() {
                    return Err(Error::EmptyDatetimeInterval);
                }
            } else {
                let _ = maybe_parse_from_rfc3339(datetime)?;
            }
        }
        Ok(self)
    }

    /// Returns true if this items structure matches the given item.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_api::Items;
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
    /// If **stac-api** is not built with the `geo` feature, this will return an error.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geo")]
    /// # {
    /// use stac_api::Search;
    /// use stac::Item;
    /// use geojson::{Geometry, Value};
    ///
    /// let mut search = Search::new();
    /// let mut item = Item::new("item-id");
    /// assert!(search.bbox_matches(&item).unwrap());
    /// search.bbox = Some(vec![-110.0, 40.0, -100.0, 50.0]);
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
                let bbox = stac::geo::bbox(bbox)?;
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
    /// use stac_api::Search;
    /// use stac::Item;
    ///
    /// let mut search = Search::new();
    /// let mut item = Item::new("item-id");  // default datetime is now
    /// assert!(search.datetime_matches(&item).unwrap());
    /// search.datetime = Some("../2023-10-09T00:00:00Z".to_string());
    /// assert!(!search.datetime_matches(&item).unwrap());
    /// item.properties.datetime = Some("2023-10-08T00:00:00Z".to_string());
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
    /// use stac_api::Search;
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
    /// use stac_api::Search;
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
    /// use stac_api::Items;
    /// let items = Items {
    ///     datetime: Some("2023".to_string()),
    ///     ..Default::default()
    /// };
    /// let search = items.into_search("collection-id");
    /// assert_eq!(search.collections.unwrap(), vec!["collection-id"]);
    /// ```
    pub fn into_search(self, collection_id: impl ToString) -> Search {
        Search {
            items: self,
            intersects: None,
            ids: None,
            collections: Some(vec![collection_id.to_string()]),
        }
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
                Filter::Cql2Json(json) => return Err(Error::CannotConvertCql2JsonToString(json)),
                Filter::Cql2Text(text) => Some(text),
            }
        } else {
            None
        };
        Ok(GetItems {
            limit: items.limit.map(|n| n.to_string()),
            bbox: items.bbox.map(|bbox| {
                bbox.into_iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            }),
            datetime: items.datetime,
            fields: items.fields.map(|fields| fields.to_string()),
            sortby: items.sortby.map(|sortby| {
                sortby
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            }),
            filter_crs: items.filter_crs,
            filter_lang: filter.as_ref().map(|_| "cql2-text".to_string()),
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
            Some(bbox)
        } else {
            None
        };

        let sortby = if let Some(value) = get_items.sortby {
            let mut sortby = Vec::new();
            for s in value.split(',') {
                sortby.push(s.parse().expect("infallible"));
            }
            Some(sortby)
        } else {
            None
        };

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

impl stac::Fields for Items {
    fn fields(&self) -> &Map<String, Value> {
        &self.additional_fields
    }
    fn fields_mut(&mut self) -> &mut Map<String, Value> {
        &mut self.additional_fields
    }
}

fn maybe_parse_from_rfc3339(s: &str) -> Result<Option<DateTime<FixedOffset>>> {
    if s.is_empty() || s == ".." {
        Ok(None)
    } else {
        DateTime::parse_from_rfc3339(s)
            .map(Some)
            .map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::{GetItems, Items};
    use crate::{sort::Direction, Fields, Filter, Sortby};
    use serde_json::{Map, Value};
    use std::collections::HashMap;

    #[test]
    fn get_items_try_from_items() {
        let mut additional_fields = HashMap::new();
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
        assert_eq!(items.bbox.unwrap(), vec![-1.0, -2.0, 1.0, 2.0]);
        assert_eq!(items.datetime.unwrap(), "2023");
        assert_eq!(
            items.fields.unwrap(),
            Fields {
                include: vec!["foo".to_string()],
                exclude: vec!["bar".to_string()],
            }
        );
        assert_eq!(
            items.sortby.unwrap(),
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
            bbox: Some(vec![-1.0, -2.0, 1.0, 2.0]),
            datetime: Some("2023".to_string()),
            fields: Some(Fields {
                include: vec!["foo".to_string()],
                exclude: vec!["bar".to_string()],
            }),
            sortby: Some(vec![Sortby {
                field: "foo".to_string(),
                direction: Direction::Descending,
            }]),
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
}
