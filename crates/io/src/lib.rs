mod error;
mod format;
#[cfg(feature = "geoparquet")]
mod geoparquet;
mod json;
mod ndjson;
mod read;
mod realized_href;
#[cfg(feature = "store")]
mod store;
#[cfg(feature = "validate")]
mod validate;
mod write;

#[cfg(feature = "geoparquet")]
pub use geoparquet::{FromGeoparquetPath, IntoGeoparquetPath};
#[cfg(feature = "store")]
pub use store::{StacStore, parse_href, parse_href_opts};
#[cfg(feature = "validate")]
pub use validate::{Validate, Validator};
pub use {
    error::Error,
    format::Format,
    json::{FromJsonPath, ToJsonPath},
    ndjson::{FromNdjsonPath, ToNdjsonPath},
    read::read,
    realized_href::RealizedHref,
    write::write,
};

/// Crate-specific result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Composite trait for all formats readable by stac-io.
#[cfg(feature = "geoparquet")]
pub trait Readable: FromJsonPath + FromNdjsonPath + FromGeoparquetPath {}
#[cfg(not(feature = "geoparquet"))]
pub trait Readable: FromJsonPath + FromNdjsonPath {}

#[cfg(feature = "geoparquet")]
impl<T> Readable for T where T: FromJsonPath + FromNdjsonPath + FromGeoparquetPath {}
#[cfg(not(feature = "geoparquet"))]
impl<T> Readable for T where T: FromJsonPath + FromNdjsonPath {}

/// Composite trait for all formats writeable by stac-io.
#[cfg(feature = "geoparquet")]
pub trait Writeable: ToJsonPath + ToNdjsonPath + IntoGeoparquetPath {}
#[cfg(not(feature = "geoparquet"))]
pub trait Writeable: ToJsonPath + ToNdjsonPath {}

#[cfg(feature = "geoparquet")]
impl<T> Writeable for T where T: ToJsonPath + ToNdjsonPath + IntoGeoparquetPath {}
#[cfg(not(feature = "geoparquet"))]
impl<T> Writeable for T where T: ToJsonPath + ToNdjsonPath {}

/// Returns a string suitable for use as a HTTP user agent.
pub fn user_agent() -> &'static str {
    concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"))
}

#[cfg(test)]
mod tests {
    use stac::{Catalog, Collection, Item, ItemCollection};
    use tempfile::TempDir;

    macro_rules! read {
        ($function:ident, $filename:expr_2021, $value:ty $(, $meta:meta)?) => {
            #[test]
            $(#[$meta])?
            fn $function() {
                use stac::SelfHref;

                let value: $value = crate::read($filename).unwrap();
                assert!(value.self_href().is_some());
            }
        };
    }

    read!(read_item_from_path, "examples/simple-item.json", Item);
    read!(read_catalog_from_path, "examples/catalog.json", Catalog);
    read!(
        read_collection_from_path,
        "examples/collection.json",
        Collection
    );
    read!(
        read_item_collection_from_path,
        "data/item-collection.json",
        ItemCollection
    );

    #[cfg(feature = "reqwest")]
    mod read_with_reqwest {
        use stac::{Catalog, Collection, Item};

        read!(
            read_item_from_url,
            "https://raw.githubusercontent.com/radiantearth/stac-spec/master/examples/simple-item.json",
            Item
        );
        read!(
            read_catalog_from_url,
            "https://raw.githubusercontent.com/radiantearth/stac-spec/master/examples/catalog.json",
            Catalog
        );
        read!(
            read_collection_from_url,
            "https://raw.githubusercontent.com/radiantearth/stac-spec/master/examples/collection.json",
            Collection
        );
    }

    #[test]
    #[cfg(not(feature = "reqwest"))]
    fn read_without_reqwest() {
        assert!(matches!(
            super::read::<Item>("http://rustac.test/item.json").unwrap_err(),
            crate::Error::FeatureNotEnabled("reqwest")
        ));
    }

    #[test]
    #[cfg(feature = "geoparquet")]
    fn read_geoparquet() {
        let _: ItemCollection = super::read("data/extended-item.parquet").unwrap();
    }

    #[test]
    #[cfg(not(feature = "geoparquet"))]
    fn read_geoparquet_without_geoparquet() {
        let _ = super::read::<ItemCollection>("data/extended-item.parquet").unwrap_err();
    }

    #[test]
    fn write() {
        let tempdir = TempDir::new().unwrap();
        let item = Item::new("an-id");
        super::write(tempdir.path().join("item.json"), item).unwrap();
        let item: Item = super::read(tempdir.path().join("item.json")).unwrap();
        assert_eq!(item.id, "an-id");
    }
}
