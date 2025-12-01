use rstest::rstest;
use stac::Value;
use stac_validate::Validate;
use std::path::PathBuf;

#[rstest]
#[tokio::test]
async fn v1_0_0(#[files("../../spec-examples/v1.0.0/**/*.json")] path: PathBuf) {
    let value: Value = stac::read(path.to_str().unwrap()).unwrap();
    value.validate().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn v1_1_0(#[files("../../spec-examples/v1.1.0/**/*.json")] path: PathBuf) {
    let value: Value = stac::read(path.to_str().unwrap()).unwrap();
    value.validate().await.unwrap();
}
