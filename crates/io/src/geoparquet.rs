use crate::Result;
use stac::{FromGeoparquet, IntoGeoparquet, geoparquet::Compression};
use std::{fs::File, io::Read, path::Path};

/// Create a STAC object from geoparquet data.
pub trait FromGeoparquetPath: FromGeoparquet {
    /// Reads geoparquet data from a file.
    ///
    /// If the `geoparquet` feature is not enabled, or if `Self` is anything
    /// other than an item collection, this function returns an error.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::ItemCollection;
    /// use stac_io::FromGeoparquetPath;
    ///
    /// let item_collection = ItemCollection::from_geoparquet_path("data/extended-item.parquet").unwrap();
    /// ```
    fn from_geoparquet_path(path: impl AsRef<Path>) -> Result<Self> {
        let mut buf = Vec::new();
        let _ = File::open(path)?.read_to_end(&mut buf)?;
        let value = Self::from_geoparquet_bytes(buf)?;
        Ok(value)
    }
}

/// Write a STAC object to geoparquet.
pub trait IntoGeoparquetPath: IntoGeoparquet {
    /// Writes a value to a path as stac-geoparquet.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac::{ItemCollection, Item};
    /// use stac_io::IntoGeoparquetPath;
    ///
    /// let item_collection: ItemCollection = vec![Item::new("a"), Item::new("b")].into();
    /// item_collection.into_geoparquet_path("items.geoparquet", None).unwrap();
    /// ```
    fn into_geoparquet_path(
        self,
        path: impl AsRef<Path>,
        compression: Option<Compression>,
    ) -> Result<()> {
        let file = File::create(path)?;
        self.into_geoparquet_writer(file, compression)?;
        Ok(())
    }
}

impl<T> FromGeoparquetPath for T where T: FromGeoparquet {}
impl<T> IntoGeoparquetPath for T where T: IntoGeoparquet {}

#[cfg(test)]
mod tests {
    use super::FromGeoparquetPath;
    use stac::{ItemCollection, Value};

    #[test]
    fn read() {
        let _ = ItemCollection::from_geoparquet_path("data/extended-item.parquet");
    }

    #[test]
    fn read_value() {
        let _ = Value::from_geoparquet_path("data/extended-item.parquet").unwrap();
    }
}
