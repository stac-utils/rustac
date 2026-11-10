use crate::{Error, Result};
use async_recursion::async_recursion;
use async_trait::async_trait;
use fluent_uri::Uri;
use jsonschema::{AsyncRetrieve, Registry, Resource, Validator as JsonschemaValidator};
use reqwest::Client;
use serde::Serialize;
use serde_json::{Map, Value};
use stac::{Type, Version};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::OnceCell;

const SCHEMA_BASE: &str = "https://schemas.stacspec.org";

type Cache = HashMap<Uri<String>, Arc<OnceCell<Arc<JsonschemaValidator>>>>;

/// A structure for validating STAC.
///
/// Compiled schemas are cached and shared between clones, so a single
/// [Validator] (or its clones) can be used to validate many objects
/// concurrently.
#[derive(Clone)]
pub struct Validator {
    validators: Arc<Mutex<Cache>>,
    registry: Arc<Registry<'static>>,
    retriever: Retriever,
}

#[derive(Debug, Clone)]
struct Retriever(Client);

impl Validator {
    /// Creates a new validator.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_validate::Validator;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let validator = Validator::new().await.unwrap();
    /// }
    /// ```
    pub async fn new() -> Result<Validator> {
        let retriever = Retriever(Client::builder().user_agent(crate::user_agent()).build()?);
        let registry = Registry::new()
            .extend(prebuild_resources())
            .expect("prebuild resource URIs should be valid")
            .prepare()
            .expect("prebuild registry should build");
        let validators = prebuild_validators(&registry, retriever.clone())
            .await
            .into_iter()
            .map(|(uri, validator)| (uri, Arc::new(OnceCell::new_with(Some(Arc::new(validator))))))
            .collect();
        Ok(Validator {
            validators: Arc::new(Mutex::new(validators)),
            registry: Arc::new(registry),
            retriever,
        })
    }

    /// Validates a single value.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_validate::Validate;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut item = Item::new("an-id");
    ///     item.validate().await.unwrap();
    /// }
    /// ```
    pub async fn validate<T>(&self, value: &T) -> Result<()>
    where
        T: Serialize,
    {
        let value = serde_json::to_value(value)?;
        let _ = self.validate_value(value).await?;
        Ok(())
    }

    /// If you have a [serde_json::Value], you can skip a deserialization step by using this method.
    #[async_recursion]
    pub async fn validate_value(&self, value: Value) -> Result<Value> {
        if let Value::Object(object) = value {
            self.validate_object(object).await.map(Value::Object)
        } else if let Value::Array(array) = value {
            self.validate_array(array).await.map(Value::Array)
        } else {
            Err(Error::ScalarJson(value))
        }
    }

    #[async_recursion]
    async fn validate_array(&self, array: Vec<Value>) -> Result<Vec<Value>> {
        let mut errors = Vec::new();
        let mut new_array = Vec::with_capacity(array.len());
        for value in array {
            match self.validate_value(value).await {
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

    #[async_recursion]
    async fn validate_object(&self, mut object: Map<String, Value>) -> Result<Map<String, Value>> {
        let r#type = if let Some(r#type) = object.get("type").and_then(|v| v.as_str()) {
            let r#type: Type = r#type.parse()?;
            if r#type == Type::ItemCollection {
                if let Some(features) = object.remove("features") {
                    let features = self.validate_value(features).await?;
                    let _ = object.insert("features".to_string(), features);
                }
                return Ok(object);
            }
            r#type
        } else {
            match object.remove("collections") {
                Some(collections) => {
                    let collections = self.validate_value(collections).await?;
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

        let uri =
            build_uri(r#type, &version).ok_or_else(|| Error::CannotValidateVersion(version))?;
        let validator = self.validator(&uri).await?;
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

        self.validate_extensions(object).await
    }

    async fn validate_extensions(&self, object: Map<String, Value>) -> Result<Map<String, Value>> {
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
                            Some(Uri::parse(s).map_err(|(err, _)| err))
                        } else {
                            None
                        }
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let validators = self.validators(&uris).await?;

                let mut errors = Vec::new();
                let value = Value::Object(object);
                for validator in &validators {
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

    async fn validators(&self, uris: &[Uri<String>]) -> Result<Vec<Arc<JsonschemaValidator>>> {
        let mut validators = Vec::with_capacity(uris.len());
        for uri in uris {
            validators.push(self.validator(uri).await?);
        }
        Ok(validators)
    }

    async fn validator(&self, uri: &Uri<String>) -> Result<Arc<JsonschemaValidator>> {
        let cell = self
            .validators
            .lock()
            .expect("validator cache lock should not be poisoned")
            .entry(uri.clone())
            .or_default()
            .clone();
        cell.get_or_try_init(|| self.build_validator(uri))
            .await
            .cloned()
    }

    async fn build_validator(&self, uri: &Uri<String>) -> Result<Arc<JsonschemaValidator>> {
        let response = self
            .retriever
            .0
            .get(uri.as_str())
            .send()
            .await?
            .error_for_status()?;
        let json_data: Value = response.json().await?;
        let validator = jsonschema::async_options()
            .with_registry(&self.registry)
            .with_retriever(self.retriever.clone())
            .build(&json_data)
            .await
            .map_err(Box::new)?;
        Ok(Arc::new(validator))
    }
}

#[async_trait]
impl AsyncRetrieve for Retriever {
    async fn retrieve(
        &self,
        uri: &Uri<String>,
    ) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let response = self.0.get(uri.as_str()).send().await?.error_for_status()?;
        let value = response.json().await?;
        Ok(value)
    }
}

fn build_uri(r#type: Type, version: &Version) -> Option<Uri<String>> {
    r#type
        .spec_path(version)
        .and_then(|path| Uri::parse(format!("{}{}", SCHEMA_BASE, path)).ok())
}

async fn prebuild_validators(
    registry: &Registry<'static>,
    retriever: Retriever,
) -> HashMap<Uri<String>, JsonschemaValidator> {
    use Type::*;
    use Version::*;

    let mut schemas = HashMap::new();

    macro_rules! schema {
        ($t:expr_2021, $v:expr_2021, $path:expr_2021, $schemas:expr_2021) => {
            let url = build_uri($t, &$v).unwrap();
            let value = serde_json::from_str(include_str!($path)).unwrap();
            let validator = jsonschema::async_options()
                .with_registry(registry)
                .with_retriever(retriever.clone())
                .build(&value)
                .await
                .unwrap();
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
                Resource::from_contents(serde_json::from_str(include_str!($path)).unwrap()),
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

    #[tokio::test]
    async fn validate_simple_item() {
        let item: Item = stac_io::read("examples/simple-item.json").unwrap();
        item.validate().await.unwrap();
    }

    #[tokio::test]
    async fn validate_inside_tokio_runtime() {
        let item: Item = stac_io::read("examples/extended-item.json").unwrap();
        item.validate().await.unwrap();
    }

    #[tokio::test]
    async fn validate_array() {
        let items: Vec<_> = (0..100)
            .map(|i| Item::new(format!("item-{i}")))
            .map(|i| serde_json::to_value(i).unwrap())
            .collect();
        let validator = Validator::new().await.unwrap();
        validator.validate(&items).await.unwrap();
    }

    #[tokio::test]
    async fn validate_concurrently() {
        let validator = Validator::new().await.unwrap();
        let item: Item = stac_io::read("examples/extended-item.json").unwrap();
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let validator = validator.clone();
                let item = item.clone();
                tokio::spawn(async move { validator.validate(&item).await })
            })
            .collect();
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn validate_collections() {
        let collection: Collection = stac_io::read("examples/collection.json").unwrap();
        let collections = json!({
            "collections": [collection]
        });
        collections.validate().await.unwrap();
    }

    #[tokio::test]
    async fn validate_invalid_version() {
        // https://github.com/stac-utils/rustac/issues/1168
        let validator = Validator::new().await.unwrap();
        validator
            .validate_value(serde_json::json!({"type": "Feature", "stac_version": "not a version"}))
            .await
            .unwrap_err();
    }
}
