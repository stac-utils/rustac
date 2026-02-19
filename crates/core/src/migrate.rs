use crate::{Error, Result, Version};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use std::collections::HashMap;
#[cfg(feature = "std")]
use url::Url;

/// Migrates a STAC object from one version to another.
pub trait Migrate: Sized + Serialize + DeserializeOwned + std::fmt::Debug {
    /// Migrates this object to another version.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, Migrate, Version};
    ///
    /// let mut item: Item = stac::read("../../spec-examples/v1.0.0/simple-item.json").unwrap();
    /// let item = item.migrate(&Version::v1_1_0).unwrap();
    /// assert_eq!(item.version, Version::v1_1_0);
    /// ```
    fn migrate(self, to: &Version) -> Result<Self> {
        let mut value = serde_json::to_value(self)?;
        if let Some(version) = value
            .as_object()
            .and_then(|object| object.get("stac_version"))
            .and_then(|version| version.as_str())
        {
            let from: Version = version.parse().unwrap(); // infallible
            let steps = from.steps(to)?;
            for step in steps {
                value = step.migrate(value)?;
            }
            let _ = value
                .as_object_mut()
                .unwrap()
                .insert("stac_version".into(), to.to_string().into());
        } else {
            tracing::warn!("no stac_version attribute found, skipping any migrations");
        }
        serde_json::from_value(value).map_err(Error::from)
    }
}

#[allow(non_camel_case_types)]
enum Step {
    v1_0_0_to_v1_1_0_beta_1,
    v1_0_0_to_v1_1_0,
}

impl Version {
    fn steps(self, to: &Version) -> Result<Vec<Step>> {
        match self {
            Version::v1_0_0 => match to {
                Version::v1_0_0 => Ok(Vec::new()),
                Version::v1_1_0_beta_1 => Ok(vec![Step::v1_0_0_to_v1_1_0_beta_1]),
                Version::v1_1_0 => Ok(vec![Step::v1_0_0_to_v1_1_0]),
                _ => Err(Error::UnsupportedMigration(self, to.clone())),
            },
            Version::v1_1_0_beta_1 => match to {
                Version::v1_1_0_beta_1 => Ok(Vec::new()),
                _ => Err(Error::UnsupportedMigration(self, to.clone())),
            },
            Version::v1_1_0 => match to {
                Version::v1_1_0 => Ok(Vec::new()),
                _ => Err(Error::UnsupportedMigration(self, to.clone())),
            },
            Version::Unknown(ref from) => match to {
                Version::Unknown(to_str) => {
                    if from == to_str {
                        Ok(Vec::new())
                    } else {
                        Err(Error::UnsupportedMigration(self, to.clone()))
                    }
                }
                _ => Err(Error::UnsupportedMigration(self, to.clone())),
            },
        }
    }
}

impl Step {
    fn migrate(&self, mut value: Value) -> Result<Value> {
        if let Some(mut object) = value.as_object_mut() {
            match self {
                Step::v1_0_0_to_v1_1_0_beta_1 | Step::v1_0_0_to_v1_1_0 => {
                    tracing::debug!("migrating from v1.0.0 to v1.1.0");
                    if let Some(assets) = object.get_mut("assets").and_then(|v| v.as_object_mut()) {
                        for asset in assets.values_mut().filter_map(|v| v.as_object_mut()) {
                            migrate_bands(asset)?;
                        }
                    }
                    migrate_links(object);
                    if object
                        .get("type")
                        .and_then(|t| t.as_str())
                        .map(|t| t == "Feature")
                        .unwrap_or_default()
                    {
                        if object
                            .get("properties")
                            .and_then(|p| p.as_object())
                            .is_none()
                        {
                            let _ = object.insert(
                                "properties".to_string(),
                                Value::Object(Default::default()),
                            );
                        }
                        object = object
                            .get_mut("properties")
                            .and_then(|v| v.as_object_mut())
                            .unwrap();
                    }
                    migrate_license(object);
                }
            }
        }
        Ok(value)
    }
}

fn migrate_bands(asset: &mut Map<String, Value>) -> Result<()> {
    let mut bands: Vec<Map<String, Value>> = Vec::new();
    if let Some(Value::Array(eo)) = asset.remove("eo:bands") {
        bands.resize_with(eo.len(), Default::default);
        for (eo_band, band) in eo.into_iter().zip(bands.iter_mut()) {
            if let Value::Object(eo_band) = eo_band {
                for (key, value) in eo_band.into_iter() {
                    if key == "name" {
                        let _ = band.insert(key, value);
                    } else {
                        let _ = band.insert(format!("eo:{key}"), value);
                    }
                }
            }
        }
    }
    if let Some(Value::Array(raster)) = asset.remove("raster:bands") {
        if raster.len() > bands.len() {
            bands.resize_with(raster.len(), Default::default);
        }
        for (raster_band, band) in raster.into_iter().zip(bands.iter_mut()) {
            if let Value::Object(raster_band) = raster_band {
                for (key, value) in raster_band.into_iter() {
                    if key == "nodata" || key == "data_type" || key == "statistics" || key == "unit"
                    {
                        let _ = band.insert(key, value);
                    } else {
                        let _ = band.insert(format!("raster:{key}"), value);
                    }
                }
            }
        }
    }
    let mut counts: HashMap<String, HashMap<String, u64>> = HashMap::new();
    let mut values: HashMap<String, Value> = HashMap::new();
    for band in &bands {
        for (key, value) in band {
            let value_as_string = serde_json::to_string(value)?;
            if !values.contains_key(&value_as_string) {
                let _ = values.insert(value_as_string.clone(), value.clone());
            }
            *counts
                .entry(key.to_string())
                .or_default()
                .entry(value_as_string)
                .or_default() += 1;
        }
    }
    for (key, count) in counts {
        if let Some((value_as_string, &count)) = count.iter().max_by_key(|&(_, &count)| count)
            && count > 1
        {
            let value = values
                .get(value_as_string)
                .expect("every value should be in the lookup hash")
                .clone();
            for band in &mut bands {
                if band.get(&key).map(|v| v == &value).unwrap_or_default() {
                    let value = band.remove(&key).unwrap();
                    let _ = asset.insert(key.clone(), value);
                }
            }
        }
    }
    if bands.iter().any(|band| !band.is_empty()) {
        let _ = asset.insert(
            "bands".into(),
            Value::Array(bands.into_iter().map(Value::Object).collect()),
        );
    }
    Ok(())
}

fn migrate_links(object: &mut Map<String, Value>) {
    if let Some(links) = object.get_mut("links").and_then(|v| v.as_array_mut()) {
        for link in links {
            let is_self_link = link
                .as_object()
                .and_then(|l| l.get("rel"))
                .and_then(|v| v.as_str())
                .map(|s| s == "self")
                .unwrap_or_default();
            if !is_self_link {
                continue;
            }
            let href = link
                .as_object()
                .and_then(|l| l.get("href"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if let Some(href) = href {
                let new_href = if href.starts_with('/') {
                    Some(format!("file://{href}"))
                } else if crate::href::is_windows_absolute_path(&href) {
                    #[cfg(feature = "std")]
                    {
                        Url::from_file_path(&href).ok().map(|u| u.to_string())
                    }
                    #[cfg(not(feature = "std"))]
                    {
                        None
                    }
                } else {
                    None
                };
                if let Some(new_href) = new_href {
                    if let Some(link) = link.as_object_mut() {
                        let _ = link.insert("href".to_string(), new_href.into());
                    }
                }
            }
        }
    }
}

fn migrate_license(object: &mut Map<String, Value>) {
    if object
        .get("license")
        .and_then(|v| v.as_str())
        .map(|l| l == "proprietary" || l == "various")
        .unwrap_or_default()
    {
        let _ = object.insert("license".into(), "other".to_string().into());
    }
}

#[cfg(test)]
mod tests {
    use crate::{Collection, DataType, Item, Link, Links, Migrate, Version};
    use assert_json_diff::assert_json_eq;
    use serde_json::Value;

    #[test]
    fn migrate_v1_0_0_to_v1_1_0() {
        let item: Item = crate::read("data/bands-v1.0.0.json").unwrap();
        let item = item.migrate(&Version::v1_1_0).unwrap();
        let asset = &item.assets["example"];
        assert_eq!(asset.data_type.as_ref().unwrap(), &DataType::UInt16);
        assert_eq!(asset.bands[0].name.as_ref().unwrap(), "r");
        assert_eq!(asset.bands[1].name.as_ref().unwrap(), "g");
        assert_eq!(asset.bands[2].name.as_ref().unwrap(), "b");
        assert_eq!(asset.bands[3].name.as_ref().unwrap(), "nir");

        let expected: Value =
            serde_json::to_value(crate::read::<Item>("data/bands-v1.1.0.json").unwrap()).unwrap();
        assert_json_eq!(expected, serde_json::to_value(item).unwrap());

        let mut collection = Collection::new("an-id", "a description");
        collection.version = Version::v1_0_0;
        let collection = collection.migrate(&Version::v1_1_0).unwrap();
        assert_eq!(collection.license, "other");

        let mut item = Item::new("an-id");
        item.version = Version::v1_0_0;
        item.set_link(Link::self_("/an/absolute/href"));
        let item = item.migrate(&Version::v1_1_0).unwrap();
        assert_eq!(item.link("self").unwrap().href, "file:///an/absolute/href");
    }

    #[test]
    fn remove_empty_bands() {
        // https://github.com/stac-utils/rustac/issues/350
        let item: Item = crate::read("data/20201211_223832_CS2.json").unwrap();
        let item = item.migrate(&Version::v1_1_0).unwrap();
        let asset = &item.assets["data"];
        assert!(asset.bands.is_empty());
    }

    #[test]
    fn migrate_v1_1_0_to_v1_1_0() {
        let item: Item = crate::read("../../spec-examples/v1.1.0/simple-item.json").unwrap();
        let _ = item.migrate(&Version::v1_1_0).unwrap();
    }
}
