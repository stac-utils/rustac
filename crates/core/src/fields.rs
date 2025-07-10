use crate::{Error, Result};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};

/// Trait for structures that have gettable and settable fields.
///
/// # Examples
///
/// ```
/// use stac::{Catalog, Item, Fields};
///
/// let mut catalog = Catalog::new("an-id", "a description");
/// catalog.set_field("foo", "bar");
/// assert_eq!(catalog.additional_fields.get("foo").unwrap(), "bar");
///
/// let mut item = Item::new("an-id");
/// item.set_field("foo", "bar");
/// assert_eq!(item.properties.additional_fields.get("foo").unwrap(), "bar");
/// assert!(item.additional_fields.is_empty());
/// ```
pub trait Fields {
    /// Gets the fields value.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, Fields};
    /// let item = Item::new("an-id");
    /// assert!(item.fields().is_empty());
    /// ```
    fn fields(&self) -> &Map<String, Value>;

    /// Gets a mutable reference to the fields value.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, Fields};
    /// let mut item = Item::new("an-id");
    /// item.fields_mut().insert("foo".to_string(), "bar".into());
    /// ```
    fn fields_mut(&mut self) -> &mut Map<String, Value>;

    /// Gets the value of a field.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, Fields};
    /// let mut item = Item::new("an-id");
    /// item.set_field("foo", "bar").unwrap();
    /// assert_eq!(item.properties.additional_fields.get("foo"), item.field("foo"));
    /// ```
    fn field(&self, key: &str) -> Option<&Value> {
        self.fields().get(key)
    }

    /// Sets the value of a field.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, Fields};
    /// let mut item = Item::new("an-id");
    /// item.set_field("foo", "bar").unwrap();
    /// assert_eq!(item.properties.additional_fields["foo"], "bar");
    /// ```
    fn set_field<S: Serialize>(&mut self, key: impl ToString, value: S) -> Result<Option<Value>> {
        let value = serde_json::to_value(value)?;
        Ok(self.fields_mut().insert(key.to_string(), value))
    }

    /// Gets values with a prefix.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Fields, Item};
    /// use serde_json::Value;
    ///
    /// let item: Item = stac::read("examples/extensions-collection/proj-example/proj-example.json").unwrap();
    /// let projection: Value = item.fields_with_prefix("proj").unwrap();
    /// ```
    fn fields_with_prefix<D: DeserializeOwned>(&self, prefix: &str) -> Result<D> {
        let mut map = Map::new();
        let prefix = format!("{prefix}:");
        for (key, value) in self.fields().iter() {
            if key.starts_with(&prefix) && key.len() > prefix.len() {
                let _ = map.insert(key[prefix.len()..].to_string(), value.clone());
            }
        }
        serde_json::from_value(json!(map)).map_err(Error::from)
    }

    /// Sets values with a prefix.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Fields, Item};
    /// use serde_json::json;
    ///
    /// let projection = json!({ "code": "EPSG:4326" });
    /// let mut item = Item::new("an-id");
    /// item.set_fields_with_prefix("proj", projection);
    /// ```
    fn set_fields_with_prefix<S: Serialize>(&mut self, prefix: &str, value: S) -> Result<()> {
        let value = serde_json::to_value(value)?;
        if let Value::Object(object) = value {
            for (key, value) in object.into_iter() {
                let _ = self.set_field(format!("{prefix}:{key}"), value);
            }
            Ok(())
        } else {
            Err(Error::NotAnObject(value))
        }
    }

    /// Removes values with a prefix.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Fields, Item};
    ///
    /// let mut item: Item = stac::read("examples/extensions-collection/proj-example/proj-example.json").unwrap();
    /// item.remove_fields_with_prefix("proj");
    /// ```
    fn remove_fields_with_prefix(&mut self, prefix: &str) {
        let prefix = format!("{prefix}:");
        self.fields_mut()
            .retain(|key, _| !(key.starts_with(&prefix) && key.len() > prefix.len()));
    }
}
