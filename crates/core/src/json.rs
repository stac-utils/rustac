use crate::{Error, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::io::Write;

/// Create a STAC object from JSON.
pub trait FromJson: DeserializeOwned {
    /// Creates an object from JSON bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::{fs::File, io::Read};
    /// use stac::{Item, FromJson};
    ///
    /// let mut buf = Vec::new();
    /// File::open("examples/simple-item.json").unwrap().read_to_end(&mut buf).unwrap();
    /// let item = Item::from_json_slice(&buf).unwrap();
    /// ```
    fn from_json_slice(slice: &[u8]) -> Result<Self> {
        serde_json::from_slice(slice).map_err(Error::from)
    }
}

/// Writes a STAC object to JSON bytes.
pub trait ToJson: Serialize {
    /// Writes a value as JSON.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{ToJson, Item};
    ///
    /// let mut buf = Vec::new();
    /// Item::new("an-id").to_json_writer(&mut buf, true).unwrap();
    /// ```
    fn to_json_writer(&self, writer: impl Write, pretty: bool) -> Result<()> {
        if pretty {
            serde_json::to_writer_pretty(writer, self).map_err(Error::from)
        } else {
            serde_json::to_writer(writer, self).map_err(Error::from)
        }
    }

    /// Writes a value as JSON bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{ToJson, Item};
    ///
    /// Item::new("an-id").to_json_vec(true).unwrap();
    /// ```
    fn to_json_vec(&self, pretty: bool) -> Result<Vec<u8>> {
        if pretty {
            serde_json::to_vec_pretty(self).map_err(Error::from)
        } else {
            serde_json::to_vec(self).map_err(Error::from)
        }
    }
}

impl<T: DeserializeOwned> FromJson for T {}
impl<T: Serialize> ToJson for T {}
