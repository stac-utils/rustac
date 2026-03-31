//! The [file](https://github.com/stac-extensions/file) extension.

use crate::Extension;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct File {
    /// The byte order of integer values in the file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub byte_order: Option<Endian>,

    /// Provides a way to specify file checksums (e.g. BLAKE2, MD5, SHA1, SHA2, SHA3).
    /// The hashes are self-identifying hashes as described in the Multihash specification
    /// and must be encoded as hexadecimal (base 16) string with lowercase letters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,

    /// The file header size, specified in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header_size: Option<usize>,

    /// The file size, specified in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<usize>,

    // Implementation of file:values is omitted, since its deprecated
    /// A relative local path for the asset/link.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_path: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum Endian {
    /// Little endian
    #[serde(rename = "little-endian")]
    LittleEndian,

    /// Big endian
    #[serde(rename = "big-endian")]
    BigEndian,
}

impl Extension for File {
    const IDENTIFIER: &'static str =
        "https://https://stac-extensions.github.io/file/v2.1.0/schema.json";
    const PREFIX: &'static str = "file";
}

#[cfg(test)]
mod tests {
    use crate::{Extensions as _, file::Endian};
    use stac::{Assets, Catalog, Collection, Item};

    use super::File;

    #[test]
    fn catalog() {
        let catalog: Catalog = stac::read("data/file/catalog.json").unwrap();
        let links = catalog.links;
        let link_0_file: File = links.get(0).unwrap().extension::<File>().unwrap();
        assert!(link_0_file.byte_order.is_none());
        assert!(link_0_file.checksum.is_none());
        assert!(link_0_file.header_size.is_none());
        assert!(link_0_file.size.is_none());
        assert!(link_0_file.local_path.is_none());
        let link_1_file: File = links.get(1).unwrap().extension::<File>().unwrap();
        assert!(link_1_file.byte_order.is_none());
        assert!(link_1_file.checksum.is_none());
        assert!(link_1_file.header_size.is_none());
        assert!(link_1_file.size.is_some());
        assert_eq!(link_1_file.size.unwrap(), 8675309);
        assert!(link_1_file.local_path.is_none());
        let link_2_file: File = links.get(2).unwrap().extension::<File>().unwrap();
        assert!(link_2_file.byte_order.is_none());
        assert!(link_2_file.checksum.is_none());
        assert!(link_2_file.header_size.is_none());
        assert!(link_2_file.size.is_none());
        assert!(link_2_file.local_path.is_none());
    }

    #[test]
    fn collection() {
        let collection: Collection = stac::read("data/file/collection.json").unwrap();
        let thumbnail = collection.assets().get("thumbnail").unwrap();
        let file: File = thumbnail.extension::<File>().unwrap();
        assert!(file.byte_order.is_none());
        assert!(file.checksum.is_some());
        assert_eq!(
            file.checksum.unwrap(),
            "90e4021044a8995dd50b6657a037a7839304535b"
        );
        assert!(file.header_size.is_none());
        assert!(file.size.is_some());
        assert_eq!(file.size.unwrap(), 153600);
        assert!(file.local_path.is_none());
    }

    #[test]
    fn noises_asset() {
        let item: Item = stac::read("data/file/item.json").unwrap();
        let noises = item.assets().get("noises").unwrap();
        let file: File = noises.extension::<File>().unwrap();
        assert!(file.byte_order.is_none());
        assert!(file.checksum.is_some());
        assert_eq!(
            file.checksum.unwrap(),
            "90e40210a30d1711e81a4b11ef67b28744321659"
        );
        assert!(file.header_size.is_none());
        assert!(file.size.is_none());
        assert!(file.local_path.is_some());
        assert_eq!(
            file.local_path.unwrap(),
            "S1A_EW_GRDM_1SSH_20181103T235855_20181103T235955_024430_02AD5D_5616.SAFE/annotation/calibration/noise-s1a-ew-grd-hh-20181103t235855-20181103t235955-024430-02ad5d-001.xml"
        );
    }

    #[test]
    fn calibration_asset() {
        let item: Item = stac::read("data/file/item.json").unwrap();
        let calibrations = item.assets().get("calibrations").unwrap();
        let file: File = calibrations.extension::<File>().unwrap();
        assert!(file.byte_order.is_none());
        assert!(file.checksum.is_some());
        assert_eq!(
            file.checksum.unwrap(),
            "90e402104fc5351af67db0b8f1746efe421a05e4"
        );
        assert!(file.header_size.is_none());
        assert!(file.size.is_none());
        assert!(file.local_path.is_some());
        assert_eq!(
            file.local_path.unwrap(),
            "S1A_EW_GRDM_1SSH_20181103T235855_20181103T235955_024430_02AD5D_5616.SAFE/annotation/calibration/calibration-s1a-ew-grd-hh-20181103t235855-20181103t235955-024430-02ad5d-001.xml"
        );
    }

    #[test]
    fn products_asset() {
        let item: Item = stac::read("data/file/item.json").unwrap();
        let products = item.assets().get("products").unwrap();
        let file: File = products.extension::<File>().unwrap();
        assert!(file.byte_order.is_none());
        assert!(file.checksum.is_some());
        assert_eq!(
            file.checksum.unwrap(),
            "90e402107a7f2588a85362b9beea2a12d4514d45"
        );
        assert!(file.header_size.is_none());
        assert!(file.size.is_none());
        assert!(file.local_path.is_none());
    }

    #[test]
    fn measurement_asset() {
        let item: Item = stac::read("data/file/item.json").unwrap();
        let measurement = item.assets().get("measurement").unwrap();
        let file: File = measurement.extension::<File>().unwrap();
        assert!(file.byte_order.is_some());
        assert_eq!(file.byte_order.unwrap(), Endian::LittleEndian);
        assert!(file.checksum.is_some());
        assert_eq!(
            file.checksum.unwrap(),
            "90e40210163700a8a6501eccd00b6d3b44ddaed0"
        );
        assert!(file.header_size.is_some());
        assert_eq!(file.header_size.unwrap(), 4096);
        assert!(file.size.is_some());
        assert_eq!(file.size.unwrap(), 209715200);
        assert!(file.local_path.is_none());
    }

    #[test]
    fn thumbnail_asset() {
        let item: Item = stac::read("data/file/item.json").unwrap();
        let thumbnail = item.assets().get("thumbnail").unwrap();
        let file: File = thumbnail.extension::<File>().unwrap();
        assert!(file.byte_order.is_some());
        assert_eq!(file.byte_order.unwrap(), Endian::BigEndian);
        assert!(file.checksum.is_some());
        assert_eq!(
            file.checksum.unwrap(),
            "90e40210f52acd32b09769d3b1871b420789456c"
        );
        assert!(file.header_size.is_none());
        assert!(file.size.is_some());
        assert_eq!(file.size.unwrap(), 146484);
        assert!(file.local_path.is_none());
    }

    #[test]
    fn links() {
        let item: Item = stac::read("data/file/item.json").unwrap();
        let links = item.links;
        let link_0_file: File = links.get(0).unwrap().extension::<File>().unwrap();
        assert!(link_0_file.byte_order.is_none());
        assert!(link_0_file.checksum.is_none());
        assert!(link_0_file.header_size.is_none());
        assert!(link_0_file.size.is_none());
        assert!(link_0_file.local_path.is_none());
        let link_1_file: File = links.get(1).unwrap().extension::<File>().unwrap();
        assert!(link_1_file.byte_order.is_none());
        assert!(link_1_file.checksum.is_some());
        assert_eq!(
            link_1_file.checksum.unwrap(),
            "11146d97123fd2c02dec9a1b6d3b13136dbe600cf966"
        );
        assert!(link_1_file.header_size.is_none());
        assert!(link_1_file.size.is_none());
        assert!(link_1_file.local_path.is_none());
        let link_2_file: File = links.get(2).unwrap().extension::<File>().unwrap();
        assert!(link_2_file.byte_order.is_none());
        assert!(link_2_file.checksum.is_some());
        assert_eq!(
            link_2_file.checksum.unwrap(),
            "1114fa4b9d69fdddc7c1be7bed9440621400b383b43f"
        );
        assert!(link_2_file.header_size.is_none());
        assert!(link_2_file.size.is_none());
        assert!(link_2_file.local_path.is_none());
    }
}
