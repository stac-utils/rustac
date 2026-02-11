use crate::Item;
use futures::Stream;
use serde::Deserialize;
use serde_json::Value;
use std::cmp::Ordering;
use stream_kmerge::kmerge_by;

#[derive(Debug, Deserialize)]
struct SortConfig {
    sortby: Vec<SortField>,
}

#[derive(Debug, Deserialize, Clone)]
struct SortField {
    field: String,
    direction: Direction,
}

#[derive(Debug, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum Direction {
    Asc,
    Desc,
}

/// A comparator for STAC Items.
///
/// This struct allows it to be used to sort items based on a configuration.
///
/// # Examples
///
/// ```
/// use stac::{Item, sort::ItemComparator};
/// use serde_json::json;
///
/// let mut items = vec![Item::new("b"), Item::new("a")];
/// let config = json!({
///    "sortby": [
///       { "field": "id", "direction": "asc" }
///   ]
/// });
/// let comparator = ItemComparator::new(config).unwrap();
/// comparator.sort(&mut items);
/// assert_eq!(items[0].id, "a");
/// ```
#[derive(Debug, Clone)]
pub struct ItemComparator {
    sort_fields: Vec<SortField>,
}

impl ItemComparator {
    /// Creates a new `ItemComparator` from a JSON configuration.
    ///
    /// The configuration should be a JSON object with a `sortby` field, which is
    /// a list of objects with `field` and `direction` fields.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::sort::ItemComparator;
    /// use serde_json::json;
    ///
    /// let config = json!({
    ///    "sortby": [
    ///       { "field": "id", "direction": "asc" }
    ///   ]
    /// });
    /// let comparator = ItemComparator::new(config).unwrap();
    /// ```
    pub fn new(config: Value) -> Result<Self, serde_json::Error> {
        let config: SortConfig = serde_json::from_value(config)?;
        Ok(Self {
            sort_fields: config.sortby,
        })
    }

    /// Sorts a mutable slice of items.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, sort::ItemComparator};
    /// use serde_json::json;
    ///
    /// let mut items = vec![Item::new("b"), Item::new("a")];
    /// let config = json!({
    ///    "sortby": [
    ///       { "field": "id", "direction": "asc" }
    ///   ]
    /// });
    /// let comparator = ItemComparator::new(config).unwrap();
    /// comparator.sort(&mut items);
    /// assert_eq!(items[0].id, "a");
    /// ```
    pub fn sort(&self, items: &mut [Item]) {
        items.sort_by(|a, b| self.compare(a, b));
    }

    /// Compares two items.
    pub fn compare(&self, l: &Item, r: &Item) -> Ordering {
        for sort_field in &self.sort_fields {
            let ord = compare_field(l, r, &sort_field.field);
            if ord != Ordering::Equal {
                return match sort_field.direction {
                    Direction::Asc => ord,
                    Direction::Desc => ord.reverse(),
                };
            }
        }
        Ordering::Equal
    }
}

impl Default for ItemComparator {
    /// Creates a new `ItemComparator` with the default sort order.
    ///
    /// The default sort order is `datetime` descending, followed by `id` ascending.
    fn default() -> Self {
        Self {
            sort_fields: vec![
                SortField {
                    field: "datetime".to_string(),
                    direction: Direction::Desc,
                },
                SortField {
                    field: "id".to_string(),
                    direction: Direction::Asc,
                },
            ],
        }
    }
}

fn compare_field(l: &Item, r: &Item, field: &str) -> Ordering {
    match field {
        "id" => l.id.cmp(&r.id),
        "datetime" => {
            let l_dt = l.properties.datetime.or(l.properties.start_datetime);
            let r_dt = r.properties.datetime.or(r.properties.start_datetime);
            l_dt.cmp(&r_dt)
        }
        "start_datetime" => {
            let l_dt = l.properties.start_datetime.or(l.properties.datetime);
            let r_dt = r.properties.start_datetime.or(r.properties.datetime);
            l_dt.cmp(&r_dt)
        }
        "end_datetime" => l.properties.end_datetime.cmp(&r.properties.end_datetime),
        "title" => l.properties.title.cmp(&r.properties.title),
        "description" => l.properties.description.cmp(&r.properties.description),
        "created" => l.properties.created.cmp(&r.properties.created),
        "updated" => l.properties.updated.cmp(&r.properties.updated),
        "collection" => l.collection.cmp(&r.collection),
        _ => compare_values(
            l.properties.additional_fields.get(field),
            r.properties.additional_fields.get(field),
        ),
    }
}

fn compare_values(l: Option<&Value>, r: Option<&Value>) -> Ordering {
    match (l, r) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(l_val), Some(r_val)) => compare_json_values(l_val, r_val),
    }
}

fn compare_json_values(l: &Value, r: &Value) -> Ordering {
    match (l, r) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Number(a), Value::Number(b)) => {
            if let (Some(a_f), Some(b_f)) = (a.as_f64(), b.as_f64()) {
                a_f.partial_cmp(&b_f).unwrap_or(Ordering::Equal)
            } else if let (Some(a_i), Some(b_i)) = (a.as_i64(), b.as_i64()) {
                a_i.cmp(&b_i)
            } else if let (Some(a_u), Some(b_u)) = (a.as_u64(), b.as_u64()) {
                a_u.cmp(&b_u)
            } else {
                Ordering::Equal
            }
        }
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Array(a), Value::Array(b)) => {
            let len = std::cmp::min(a.len(), b.len());
            for i in 0..len {
                let ord = compare_json_values(&a[i], &b[i]);
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            a.len().cmp(&b.len())
        }
        (Value::Object(_), Value::Object(_)) => Ordering::Equal,
        (Value::Bool(_), _) => Ordering::Less,
        (_, Value::Bool(_)) => Ordering::Greater,
        (Value::Number(_), _) => Ordering::Less,
        (_, Value::Number(_)) => Ordering::Greater,
        (Value::String(_), _) => Ordering::Less,
        (_, Value::String(_)) => Ordering::Greater,
        (Value::Array(_), _) => Ordering::Less,
        (_, Value::Array(_)) => Ordering::Greater,
    }
}

/// Creates a function that returns a struct that can be used to compare stac items.
///
/// # Examples
///
/// ```
/// use stac::{Item, sort::item_comparator};
/// use serde_json::json;
///
/// let mut items = vec![Item::new("b"), Item::new("a")];
/// let config = json!({
///    "sortby": [
///       { "field": "id", "direction": "asc" }
///   ]
/// });
/// let comparator = item_comparator(config).unwrap();
/// comparator.sort(&mut items);
/// assert_eq!(items[0].id, "a");
/// ```
pub fn item_comparator(config: Value) -> Result<ItemComparator, serde_json::Error> {
    ItemComparator::new(config)
}

/// Sorts multiple streams of items into a single sorted stream.
///
/// # Examples
///
/// ```
/// use stac::{Item, sort::sort_streams};
/// use serde_json::json;
/// use futures::stream::{self, StreamExt};
///
/// # tokio_test::block_on(async {
/// let stream1 = stream::iter(vec![Item::new("a"), Item::new("c")]);
/// let stream2 = stream::iter(vec![Item::new("b"), Item::new("d")]);
/// let config = json!({
///    "sortby": [
///       { "field": "id", "direction": "asc" }
///   ]
/// });
/// let mut sorted = sort_streams(vec![stream1, stream2], config).unwrap();
/// assert_eq!(sorted.next().await.unwrap().id, "a");
/// assert_eq!(sorted.next().await.unwrap().id, "b");
/// assert_eq!(sorted.next().await.unwrap().id, "c");
/// assert_eq!(sorted.next().await.unwrap().id, "d");
/// # });
/// ```
pub fn sort_streams<S, I>(
    streams: I,
    config: Value,
) -> Result<impl Stream<Item = S::Item>, serde_json::Error>
where
    S: Stream<Item = Item> + Unpin,
    I: IntoIterator<Item = S>,
{
    let comparator = ItemComparator::new(config)?;
    Ok(kmerge_by(streams, move |a, b| {
        comparator.compare(a, b).reverse()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_sort() {
        let mut items = vec![Item::new("c"), Item::new("a"), Item::new("b")];
        let config = json!({
            "sortby": [
                { "field": "id", "direction": "asc" }
            ]
        });
        let comparator = item_comparator(config).unwrap();
        items.sort_by(|a, b| comparator.compare(a, b));
        assert_eq!(items[0].id, "a");
        assert_eq!(items[1].id, "b");
        assert_eq!(items[2].id, "c");
    }

    #[test]
    fn test_sort_desc() {
        let mut items = vec![Item::new("c"), Item::new("a"), Item::new("b")];
        let config = json!({
            "sortby": [
                { "field": "id", "direction": "desc" }
            ]
        });
        let comparator = item_comparator(config).unwrap();
        items.sort_by(|a, b| comparator.compare(a, b));
        assert_eq!(items[0].id, "c");
        assert_eq!(items[1].id, "b");
        assert_eq!(items[2].id, "a");
    }

    #[test]
    fn test_sort_datetime() {
        let mut item1 = Item::new("1");
        item1.properties.datetime = Some("2023-01-01T00:00:00Z".parse().unwrap());
        let mut item2 = Item::new("2");
        item2.properties.datetime = Some("2023-01-02T00:00:00Z".parse().unwrap());

        let mut items = vec![item2.clone(), item1.clone()];
        let config = json!({
            "sortby": [
                { "field": "datetime", "direction": "asc" }
            ]
        });
        let comparator = item_comparator(config).unwrap();
        items.sort_by(|a, b| comparator.compare(a, b));
        assert_eq!(items[0].id, "1");
        assert_eq!(items[1].id, "2");
    }

    #[test]
    fn test_sort_method_direct() {
        let mut items = vec![Item::new("b"), Item::new("a")];
        let config = json!({
            "sortby": [
                { "field": "id", "direction": "asc" }
            ]
        });
        let comparator = item_comparator(config).unwrap();
        comparator.sort(&mut items);
        assert_eq!(items[0].id, "a");
        assert_eq!(items[1].id, "b");
    }

    #[test]
    fn test_default() {
        let mut item1 = Item::new("1");
        item1.properties.datetime = Some("2023-01-01T00:00:00Z".parse().unwrap());
        let mut item2 = Item::new("2");
        item2.properties.datetime = Some("2023-01-02T00:00:00Z".parse().unwrap());
        let mut item3 = Item::new("3");
        item3.properties.datetime = Some("2023-01-01T00:00:00Z".parse().unwrap());

        let mut items = vec![item1.clone(), item2.clone(), item3.clone()];
        let comparator = ItemComparator::default();
        comparator.sort(&mut items);

        // datetime desc, so item2 (Jan 2) comes first
        assert_eq!(items[0].id, "2");
        // then item1 and item3 (Jan 1). id asc, so item1 comes before item3
        assert_eq!(items[1].id, "1");
        assert_eq!(items[2].id, "3");
    }

    #[test]
    fn test_sort_datetime_fallback() {
        let mut item1 = Item::new("1");
        item1.properties.datetime = None;
        item1.properties.start_datetime = Some("2023-01-01T00:00:00Z".parse().unwrap());
        // datetime is None

        let mut item2 = Item::new("2");
        item2.properties.datetime = Some("2023-01-02T00:00:00Z".parse().unwrap());
        // start_datetime is None

        let mut items = vec![item2.clone(), item1.clone()];
        let config = json!({
            "sortby": [
                { "field": "datetime", "direction": "asc" }
            ]
        });
        let comparator = item_comparator(config).unwrap();
        comparator.sort(&mut items);

        // item1 (Jan 1 via start_datetime) < item2 (Jan 2 via datetime)
        assert_eq!(items[0].id, "1");
        assert_eq!(items[1].id, "2");
    }

    #[test]
    fn test_sort_start_datetime_fallback() {
        let mut item1 = Item::new("1");
        item1.properties.datetime = Some("2023-01-01T00:00:00Z".parse().unwrap());
        // start_datetime is None

        let mut item2 = Item::new("2");
        item2.properties.start_datetime = Some("2023-01-02T00:00:00Z".parse().unwrap());
        // datetime is None

        let mut items = vec![item2.clone(), item1.clone()];
        let config = json!({
            "sortby": [
                { "field": "start_datetime", "direction": "asc" }
            ]
        });
        let comparator = item_comparator(config).unwrap();
        comparator.sort(&mut items);

        // item1 (Jan 1 via datetime) < item2 (Jan 2 via start_datetime)
        assert_eq!(items[0].id, "1");
        assert_eq!(items[1].id, "2");
    }

    #[test]
    fn test_complex_sort() {
        let mut items = Vec::new();

        // Collection A, Date 2, ID 1
        let mut item1 = Item::new("1");
        item1.collection = Some("A".to_string());
        item1.properties.datetime = Some("2023-01-02T00:00:00Z".parse().unwrap());
        items.push(item1);

        // Collection A, Date 2, ID 2
        let mut item2 = Item::new("2");
        item2.collection = Some("A".to_string());
        item2.properties.datetime = Some("2023-01-02T00:00:00Z".parse().unwrap());
        items.push(item2);

        // Collection A, Date 1, ID 3
        let mut item3 = Item::new("3");
        item3.collection = Some("A".to_string());
        item3.properties.datetime = Some("2023-01-01T00:00:00Z".parse().unwrap());
        items.push(item3);

        // Collection B, Date 2, ID 4
        let mut item4 = Item::new("4");
        item4.collection = Some("B".to_string());
        item4.properties.datetime = Some("2023-01-02T00:00:00Z".parse().unwrap());
        items.push(item4);

        // Collection B, Date 2, ID 5
        let mut item5 = Item::new("5");
        item5.collection = Some("B".to_string());
        item5.properties.datetime = Some("2023-01-02T00:00:00Z".parse().unwrap());
        items.push(item5);

        // Sort by: Collection ASC, Datetime DESC, ID DESC
        let config = json!({
            "sortby": [
                { "field": "collection", "direction": "asc" },
                { "field": "datetime", "direction": "desc" },
                { "field": "id", "direction": "desc" }
            ]
        });

        let comparator = item_comparator(config).unwrap();
        comparator.sort(&mut items);

        // Expected order:
        // Collection A:
        //   Date 2:
        //     ID 2 (desc)
        //     ID 1
        //   Date 1:
        //     ID 3
        // Collection B:
        //   Date 2:
        //     ID 5 (desc)
        //     ID 4

        assert_eq!(items[0].id, "2");
        assert_eq!(items[1].id, "1");
        assert_eq!(items[2].id, "3");
        assert_eq!(items[3].id, "5");
        assert_eq!(items[4].id, "4");
    }

    #[test]
    fn test_three_fields_with_custom_property() {
        let mut items = Vec::new();

        // Create items with custom property "cloud_cover"
        for i in 0..6 {
            let mut item = Item::new(i.to_string());
            let _ = item
                .properties
                .additional_fields
                .insert("cloud_cover".to_string(), json!(i % 2)); // 0, 1, 0, 1, 0, 1
            item.properties.datetime = Some("2023-01-01T00:00:00Z".parse().unwrap());
            items.push(item);
        }
        // Items:
        // 0: cloud=0
        // 1: cloud=1
        // 2: cloud=0
        // 3: cloud=1
        // 4: cloud=0
        // 5: cloud=1

        // Sort by: cloud_cover ASC, id DESC
        let config = json!({
            "sortby": [
                { "field": "cloud_cover", "direction": "asc" },
                { "field": "id", "direction": "desc" }
            ]
        });

        let comparator = item_comparator(config).unwrap();
        comparator.sort(&mut items);

        // Expected:
        // cloud=0: 4, 2, 0
        // cloud=1: 5, 3, 1

        assert_eq!(items[0].id, "4");
        assert_eq!(items[1].id, "2");
        assert_eq!(items[2].id, "0");
        assert_eq!(items[3].id, "5");
        assert_eq!(items[4].id, "3");
        assert_eq!(items[5].id, "1");
    }

    #[test]
    fn test_sort_streams() {
        use futures::stream::{self, StreamExt};

        let stream1 = stream::iter(vec![Item::new("a"), Item::new("c")]);
        let stream2 = stream::iter(vec![Item::new("b"), Item::new("d")]);
        let config = json!({
            "sortby": [
                { "field": "id", "direction": "asc" }
            ]
        });
        let mut sorted = sort_streams(vec![stream1, stream2], config).unwrap();

        let mut items = Vec::new();
        tokio_test::block_on(async {
            while let Some(item) = sorted.next().await {
                items.push(item);
            }
        });

        assert_eq!(items[0].id, "a");
        assert_eq!(items[1].id, "b");
        assert_eq!(items[2].id, "c");
        assert_eq!(items[3].id, "d");
    }
}
