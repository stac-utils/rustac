use crate::Result;
use serde::Serialize;
use stac::{FromJson, SelfHref, ToJson};
use std::{fs::File, io::Read, path::Path};

/// Create a STAC object from JSON.
pub trait FromJsonPath: FromJson + SelfHref {
    /// Reads JSON data from a file.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_io::FromJsonPath;
    ///
    /// let item = Item::from_json_path("examples/simple-item.json").unwrap();
    /// ```
    fn from_json_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let mut buf = Vec::new();
        let _ = File::open(path)?.read_to_end(&mut buf)?;
        let mut value = Self::from_json_slice(&buf)?;
        *value.self_href_mut() = Some(path.into());
        Ok(value)
    }
}

pub trait ToJsonPath: ToJson {
    /// Writes a value to a path as JSON.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac::Item;
    /// use stac_io::ToJsonPath;
    ///
    /// Item::new("an-id").to_json_path("an-id.json", true).unwrap();
    /// ```
    fn to_json_path(&self, path: impl AsRef<Path>, pretty: bool) -> Result<()> {
        let file = File::create(path)?;
        self.to_json_writer(file, pretty)?;
        Ok(())
    }
}

impl<T: FromJson + SelfHref> FromJsonPath for T {}
impl<T: Serialize> ToJsonPath for T {}

#[cfg(test)]
mod tests {
    use super::FromJsonPath;
    use stac::{Item, SelfHref};

    #[test]
    fn set_href() {
        let item = Item::from_json_path("examples/simple-item.json").unwrap();
        assert!(
            item.self_href()
                .unwrap()
                .as_str()
                .ends_with("examples/simple-item.json")
        );
    }
}
