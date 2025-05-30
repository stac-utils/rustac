use crate::{Error, Result};
use fluent_uri::Uri;
use jsonschema::{Resource, Retrieve, ValidationOptions, Validator as JsonschemaValidator};
use reqwest::blocking::Client;
use serde::Serialize;
use serde_json::{Map, Value};
use stac::{Type, Version};
use std::collections::HashMap;

const SCHEMA_BASE: &str = "https://schemas.stacspec.org";

/// A structure for validating STAC.
#[derive(Debug)]
pub struct Validator {
    validators: HashMap<Uri<String>, JsonschemaValidator>,
    validation_options: ValidationOptions,
}

#[derive(Debug)]
struct Retriever(Client);

impl Validator {
    /// Creates a new validator.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_validate::Validator;
    ///
    /// let validator = Validator::new().unwrap();
    /// ```
    pub fn new() -> Result<Validator> {
        let validation_options = jsonschema::options();
        let validation_options = validation_options
            .with_resources(prebuild_resources().into_iter())
            .with_retriever(Retriever(
                Client::builder().user_agent(crate::user_agent()).build()?,
            ));
        Ok(Validator {
            validators: prebuild_validators(&validation_options),
            validation_options,
        })
    }

    /// Validates a single value.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_validate::Validator;
    ///
    /// let item = Item::new("an-id");
    /// let mut validator = Validator::new().unwrap();
    /// validator.validate(&item).unwrap();
    /// ```
    pub fn validate<T>(&mut self, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        let value = serde_json::to_value(value)?;
        let _ = self.validate_value(value)?;
        Ok(())
    }

    /// If you have a [serde_json::Value], you can skip a deserialization step by using this method.
    pub fn validate_value(&mut self, value: Value) -> Result<Value> {
        if let Value::Object(object) = value {
            self.validate_object(object).map(Value::Object)
        } else if let Value::Array(array) = value {
            self.validate_array(array).map(Value::Array)
        } else {
            Err(Error::ScalarJson(value))
        }
    }

    fn validate_array(&mut self, array: Vec<Value>) -> Result<Vec<Value>> {
        let mut errors = Vec::new();
        let mut new_array = Vec::with_capacity(array.len());
        for value in array {
            match self.validate_value(value) {
                Ok(value) => new_array.push(value),
                Err(error) => {
                    if let Error::Validation(e) = error {
                        errors.extend(e);
                    } else {
                        return Err(error);
                    }
                }
            }
        }
        if errors.is_empty() {
            Ok(new_array)
        } else {
            Err(Error::Validation(errors))
        }
    }

    fn validate_object(&mut self, mut object: Map<String, Value>) -> Result<Map<String, Value>> {
        let r#type = if let Some(r#type) = object.get("type").and_then(|v| v.as_str()) {
            let r#type: Type = r#type.parse()?;
            if r#type == Type::ItemCollection {
                if let Some(features) = object.remove("features") {
                    let features = self.validate_value(features)?;
                    let _ = object.insert("features".to_string(), features);
                }
                return Ok(object);
            }
            r#type
        } else {
            match object.remove("collections") {
                Some(collections) => {
                    let collections = self.validate_value(collections)?;
                    let _ = object.insert("collections".to_string(), collections);
                    return Ok(object);
                }
                _ => {
                    return Err(stac::Error::MissingField("type").into());
                }
            }
        };

        let version: Version = object
            .get("stac_version")
            .and_then(|v| v.as_str())
            .map(|v| v.parse::<Version>())
            .transpose()
            .unwrap()
            .ok_or(stac::Error::MissingField("stac_version"))?;

        let uri = build_uri(r#type, &version);
        let validator = self.validator(uri)?;
        let value = Value::Object(object);
        let errors: Vec<_> = validator.iter_errors(&value).collect();
        let object = if errors.is_empty() {
            if let Value::Object(object) = value {
                object
            } else {
                unreachable!()
            }
        } else {
            return Err(Error::from_validation_errors(
                errors.into_iter(),
                Some(&value),
            ));
        };

        self.validate_extensions(object)
    }

    fn validate_extensions(&mut self, object: Map<String, Value>) -> Result<Map<String, Value>> {
        match object
            .get("stac_extensions")
            .and_then(|value| value.as_array())
            .cloned()
        {
            Some(stac_extensions) => {
                let uris = stac_extensions
                    .into_iter()
                    .filter_map(|value| {
                        if let Value::String(s) = value {
                            Some(Uri::parse(s))
                        } else {
                            None
                        }
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                self.ensure_validators(&uris)?;

                let mut errors = Vec::new();
                let value = Value::Object(object);
                for uri in uris {
                    let validator = self
                        .validator_opt(&uri)
                        .expect("We already ensured they're present");
                    errors.extend(validator.iter_errors(&value));
                }
                if errors.is_empty() {
                    if let Value::Object(object) = value {
                        Ok(object)
                    } else {
                        unreachable!()
                    }
                } else {
                    Err(Error::from_validation_errors(
                        errors.into_iter(),
                        Some(&value),
                    ))
                }
            }
            _ => Ok(object),
        }
    }

    fn validator(&mut self, uri: Uri<String>) -> Result<&JsonschemaValidator> {
        self.ensure_validator(&uri)?;
        Ok(self.validator_opt(&uri).unwrap())
    }

    fn ensure_validators(&mut self, uris: &[Uri<String>]) -> Result<()> {
        for uri in uris {
            self.ensure_validator(uri)?;
        }
        Ok(())
    }

    fn ensure_validator(&mut self, uri: &Uri<String>) -> Result<()> {
        if !self.validators.contains_key(uri) {
            let response = reqwest::blocking::get(uri.as_str())?.error_for_status()?;
            let validator = self
                .validation_options
                .build(&response.json()?)
                .map_err(Box::new)?;
            let _ = self.validators.insert(uri.clone(), validator);
        }
        Ok(())
    }

    fn validator_opt(&self, uri: &Uri<String>) -> Option<&JsonschemaValidator> {
        self.validators.get(uri)
    }
}

impl Retrieve for Retriever {
    fn retrieve(
        &self,
        uri: &Uri<String>,
    ) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let response = self.0.get(uri.as_str()).send()?.error_for_status()?;
        let value = response.json()?;
        Ok(value)
    }
}

fn build_uri(r#type: Type, version: &Version) -> Uri<String> {
    Uri::parse(format!(
        "{}{}",
        SCHEMA_BASE,
        r#type
            .spec_path(version)
            .expect("we shouldn't get here with an item collection")
    ))
    .unwrap()
}

fn prebuild_validators(
    validation_options: &ValidationOptions,
) -> HashMap<Uri<String>, JsonschemaValidator> {
    use Type::*;
    use Version::*;

    let mut schemas = HashMap::new();

    macro_rules! schema {
        ($t:expr_2021, $v:expr_2021, $path:expr_2021, $schemas:expr_2021) => {
            let url = build_uri($t, &$v);
            let value = serde_json::from_str(include_str!($path)).unwrap();
            let validator = validation_options.build(&value).unwrap();
            let _ = schemas.insert(url, validator);
        };
    }

    schema!(Item, v1_0_0, "schemas/v1.0.0/item.json", schemas);
    schema!(Catalog, v1_0_0, "schemas/v1.0.0/catalog.json", schemas);
    schema!(
        Collection,
        v1_0_0,
        "schemas/v1.0.0/collection.json",
        schemas
    );
    schema!(Item, v1_1_0, "schemas/v1.1.0/item.json", schemas);
    schema!(Catalog, v1_1_0, "schemas/v1.1.0/catalog.json", schemas);
    schema!(
        Collection,
        v1_1_0,
        "schemas/v1.1.0/collection.json",
        schemas
    );

    schemas
}

fn prebuild_resources() -> Vec<(String, Resource)> {
    let mut resources = Vec::new();

    macro_rules! resolve {
        ($url:expr_2021, $path:expr_2021) => {
            let _ = resources.push((
                $url.to_string(),
                Resource::from_contents(serde_json::from_str(include_str!($path)).unwrap())
                    .unwrap(),
            ));
        };
    }

    // General
    resolve!(
        "https://geojson.org/schema/Feature.json",
        "schemas/geojson/Feature.json"
    );
    resolve!(
        "https://geojson.org/schema/Geometry.json",
        "schemas/geojson/Geometry.json"
    );
    resolve!(
        "http://json-schema.org/draft-07/schema",
        "schemas/json-schema/draft-07.json"
    );

    // STAC v1.0.0
    resolve!(
        "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/basics.json",
        "schemas/v1.0.0/basics.json"
    );
    resolve!(
        "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/datetime.json",
        "schemas/v1.0.0/datetime.json"
    );
    resolve!(
        "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/instrument.json",
        "schemas/v1.0.0/instrument.json"
    );
    resolve!(
        "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json",
        "schemas/v1.0.0/item.json"
    );
    resolve!(
        "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/licensing.json",
        "schemas/v1.0.0/licensing.json"
    );
    resolve!(
        "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/provider.json",
        "schemas/v1.0.0/provider.json"
    );

    // STAC v1.1.0
    resolve!(
        "https://schemas.stacspec.org/v1.1.0/item-spec/json-schema/bands.json",
        "schemas/v1.1.0/bands.json"
    );
    resolve!(
        "https://schemas.stacspec.org/v1.1.0/item-spec/json-schema/basics.json",
        "schemas/v1.1.0/basics.json"
    );
    resolve!(
        "https://schemas.stacspec.org/v1.1.0/item-spec/json-schema/common.json",
        "schemas/v1.1.0/common.json"
    );
    resolve!(
        "https://schemas.stacspec.org/v1.1.0/item-spec/json-schema/data-values.json",
        "schemas/v1.1.0/data-values.json"
    );
    resolve!(
        "https://schemas.stacspec.org/v1.1.0/item-spec/json-schema/datetime.json",
        "schemas/v1.1.0/datetime.json"
    );
    resolve!(
        "https://schemas.stacspec.org/v1.1.0/item-spec/json-schema/instrument.json",
        "schemas/v1.1.0/instrument.json"
    );
    resolve!(
        "https://schemas.stacspec.org/v1.1.0/item-spec/json-schema/item.json",
        "schemas/v1.1.0/item.json"
    );
    resolve!(
        "https://schemas.stacspec.org/v1.1.0/item-spec/json-schema/licensing.json",
        "schemas/v1.1.0/licensing.json"
    );
    resolve!(
        "https://schemas.stacspec.org/v1.1.0/item-spec/json-schema/provider.json",
        "schemas/v1.1.0/provider.json"
    );

    resources
}

#[cfg(test)]
mod tests {
    use super::Validator;
    use crate::Validate;
    use serde_json::json;
    use stac::{Collection, Item};

    #[test]
    fn validate_simple_item() {
        let item: Item = stac_io::read("examples/simple-item.json").unwrap();
        item.validate().unwrap();
    }

    #[tokio::test]
    #[ignore = "can't validate in a tokio runtime yet: https://github.com/Stranger6667/jsonschema/issues/385"]
    async fn validate_inside_tokio_runtime() {
        let item: Item = stac_io::read("examples/extended-item.json").unwrap();
        item.validate().unwrap();
    }

    #[test]
    fn validate_array() {
        let items: Vec<_> = (0..100)
            .map(|i| Item::new(format!("item-{}", i)))
            .map(|i| serde_json::to_value(i).unwrap())
            .collect();
        let mut validator = Validator::new().unwrap();
        validator.validate(&items).unwrap();
    }

    #[test]
    fn validate_collections() {
        let collection: Collection = stac_io::read("examples/collection.json").unwrap();
        let collections = json!({
            "collections": [collection]
        });
        collections.validate().unwrap();
    }
}
