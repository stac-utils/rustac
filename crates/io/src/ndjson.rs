use crate::{Error, FromJsonPath, Result};
use stac::{Catalog, Collection, FromNdjson, Item, ItemCollection, SelfHref, ToNdjson, Value};
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter},
    path::Path,
};

/// Create a STAC object from newline-delimited JSON.
pub trait FromNdjsonPath: FromNdjson + FromJsonPath + SelfHref {
    /// Reads newline-delimited JSON data from a file.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::ItemCollection;
    /// use stac_io::FromNdjsonPath;
    ///
    /// let item_collection = ItemCollection::from_ndjson_path("data/items.ndjson").unwrap();
    /// ```
    fn from_ndjson_path(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_json_path(path)
    }
}

/// Write a STAC object to newline-delimited JSON.
pub trait ToNdjsonPath: ToNdjson {
    /// Writes a value to a path as newline-delimited JSON.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac::{ItemCollection, Item};
    /// use stac_io::ToNdjsonPath;
    ///
    /// let item_collection: ItemCollection = vec![Item::new("a"), Item::new("b")].into();
    /// item_collection.to_ndjson_path("items.ndjson").unwrap();
    /// ```
    fn to_ndjson_path(&self, path: impl AsRef<Path>) -> Result<()> {
        let file = File::create(path)?;
        self.to_ndjson_writer(file)?;
        Ok(())
    }
}

impl FromNdjsonPath for Item {}
impl FromNdjsonPath for Catalog {}
impl FromNdjsonPath for Collection {}
impl FromNdjsonPath for ItemCollection {
    fn from_ndjson_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let reader = BufReader::new(File::open(path)?);
        let mut items = Vec::new();
        for line in reader.lines() {
            items.push(serde_json::from_str(&line?)?);
        }
        let mut item_collection = ItemCollection::from(items);
        *item_collection.self_href_mut() = Some(path.into());
        Ok(item_collection)
    }
}
impl FromNdjsonPath for Value {
    fn from_ndjson_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let reader = BufReader::new(File::open(path)?);
        let mut values: Vec<Value> = Vec::new();
        for line in reader.lines() {
            values.push(serde_json::from_str(&line?)?);
        }
        vec_into_value(values)
    }
}

fn vec_into_value(mut values: Vec<Value>) -> Result<Value> {
    if values.len() == 1 {
        Ok(values.pop().unwrap())
    } else {
        Ok(ItemCollection::from(
            values
                .into_iter()
                .map(|v| Item::try_from(v).map_err(Error::from))
                .collect::<Result<Vec<_>>>()?,
        )
        .into())
    }
}

impl ToNdjsonPath for Item {}
impl ToNdjsonPath for Catalog {}
impl ToNdjsonPath for Collection {}

impl ToNdjsonPath for ItemCollection {
    fn to_ndjson_path(&self, path: impl AsRef<Path>) -> Result<()> {
        let file = BufWriter::new(File::create(path)?);
        self.to_ndjson_writer(file)?;
        Ok(())
    }
}

impl ToNdjsonPath for Value {
    fn to_ndjson_path(&self, path: impl AsRef<Path>) -> Result<()> {
        match self {
            Value::Item(item) => item.to_ndjson_path(path),
            Value::Catalog(catalog) => catalog.to_ndjson_path(path),
            Value::Collection(collection) => collection.to_ndjson_path(path),
            Value::ItemCollection(item_collection) => item_collection.to_ndjson_path(path),
        }
    }
}

impl ToNdjsonPath for serde_json::Value {
    fn to_ndjson_path(&self, path: impl AsRef<Path>) -> Result<()> {
        let file = File::create(path)?;
        self.to_ndjson_writer(file)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::FromNdjsonPath;
    use stac::{ItemCollection, SelfHref, Value};

    #[test]
    fn item_collection_read() {
        let item_collection = ItemCollection::from_ndjson_path("data/items.ndjson").unwrap();
        assert_eq!(item_collection.items.len(), 2);
        assert!(
            item_collection
                .self_href()
                .unwrap()
                .as_str()
                .ends_with("data/items.ndjson")
        );
    }

    #[test]
    fn value_read() {
        let _ = Value::from_ndjson_path("data/items.ndjson").unwrap();
    }
}
