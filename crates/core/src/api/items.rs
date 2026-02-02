use super::{Fields, Filter, Result, Search, Sortby};
use crate::Error;
use chrono::{DateTime, FixedOffset};
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
    /// Returns the items, unchanged, if it is valid.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::api::Items;
    ///
    /// let items = Items::default().valid().unwrap();
    /// ```
    pub fn valid(self) -> Result<Items> {
        if let Some(bbox) = self.bbox.as_ref()
            && !bbox.is_valid()
        {
            return Err(Error::InvalidBbox((*bbox).into(), "invalid bbox"));
        }
        if let Some(datetime) = self.datetime.as_deref() {
            if let Some((start, end)) = datetime.split_once('/') {
                let (start, end) = (
                    maybe_parse_from_rfc3339(start)?,
                    maybe_parse_from_rfc3339(end)?,
                );
                if let Some(start) = start {
                    if let Some(end) = end
                        && end < start
                    {
                        return Err(Error::StartIsAfterEnd(start, end));
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
                item.intersects(&bbox)
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
            item.intersects_datetime_str(datetime)
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
}
