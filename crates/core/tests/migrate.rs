use rstest::rstest;
use stac::{Migrate, Value, Version};
use stac_validate::Validate;
use std::path::PathBuf;

#[rstest]
fn v1_0_0_to_v1_1_0(#[files("../../spec-examples/v1.0.0/**/*.json")] path: PathBuf) {
    let value: Value = stac::read(path.to_str().unwrap()).unwrap();
    let value = value.migrate(&Version::v1_1_0).unwrap();
    value.validate().unwrap();
}
