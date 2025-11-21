use assert_cmd::Command;
use clap::Parser;
use rstest::{fixture, rstest};
use rustac::Rustac;
use stac::geoparquet::{Compression, WriterOptions};
use stac_io::Format;

#[fixture]
fn command() -> Command {
    assert_cmd::cargo::cargo_bin_cmd!()
}

#[rstest]
fn translate_json(mut command: Command) {
    command
        .arg("translate")
        .arg("examples/simple-item.json")
        .assert()
        .success();
}

#[rstest]
fn migrate(mut command: Command) {
    command
        .arg("translate")
        .arg("../../spec-examples/v1.0.0/simple-item.json")
        .arg("--migrate")
        .assert()
        .success();
}

#[rstest]
fn translate_to_file(mut command: Command) {
    let temp_dir = tempfile::env::temp_dir();
    command
        .arg("translate")
        .arg("examples/simple-item.json")
        .arg(temp_dir.join("simple-item.json"))
        .assert()
        .success();
}

#[test]
fn input_format() {
    let rustac = Rustac::parse_from(["rustac", "translate"]);
    assert_eq!(rustac.input_format(None), Format::Json(false));

    let rustac = Rustac::parse_from(["rustac", "translate"]);
    assert_eq!(rustac.input_format(Some("file.json")), Format::Json(false));

    let rustac = Rustac::parse_from(["rutsac", "translate"]);
    assert_eq!(rustac.input_format(Some("file.ndjson")), Format::NdJson);

    let rustac = Rustac::parse_from(["Rustac", "translate"]);
    assert_eq!(
        rustac.input_format(Some("file.parquet")),
        Format::Geoparquet(WriterOptions::new())
    );

    let rustac = Rustac::parse_from(["rutsac", "--input-format", "json", "translate"]);
    assert_eq!(rustac.input_format(None), Format::Json(false));

    let rustac = Rustac::parse_from(["rustac", "--input-format", "ndjson", "translate"]);
    assert_eq!(rustac.input_format(None), Format::NdJson);

    let rustac = Rustac::parse_from(["rustac", "--input-format", "parquet", "translate"]);
    assert_eq!(
        rustac.input_format(None),
        Format::Geoparquet(WriterOptions::new())
    );

    let rustac = Rustac::parse_from([
        "rustac",
        "--input-format",
        "json",
        "--compact-json",
        "false",
        "translate",
    ]);
    assert_eq!(rustac.input_format(None), Format::Json(false));
}

#[test]
fn output_format() {
    let rustac = Rustac::parse_from(["rustac", "translate"]);
    assert_eq!(rustac.output_format(None), Format::Json(true));

    let rustac = Rustac::parse_from(["rustac", "translate"]);
    assert_eq!(rustac.output_format(Some("file.json")), Format::Json(false));

    let rustac = Rustac::parse_from(["rustac", "translate"]);
    assert_eq!(rustac.output_format(Some("file.ndjson")), Format::NdJson);

    let rustac = Rustac::parse_from(["rustac", "translate"]);
    assert_eq!(
        rustac.output_format(Some("file.parquet")),
        Format::Geoparquet(WriterOptions::new())
    );

    let rustac = Rustac::parse_from(["rustac", "--output-format", "json", "translate"]);
    assert_eq!(rustac.output_format(None), Format::Json(false));

    let rustac = Rustac::parse_from(["rustac", "--output-format", "ndjson", "translate"]);
    assert_eq!(rustac.output_format(None), Format::NdJson);

    let rustac = Rustac::parse_from(["rustac", "--output-format", "parquet", "translate"]);
    assert_eq!(
        rustac.output_format(None),
        Format::Geoparquet(WriterOptions::new())
    );

    let rustac = Rustac::parse_from([
        "rustac",
        "--output-format",
        "json",
        "--compact-json",
        "false",
        "translate",
    ]);
    assert_eq!(rustac.output_format(None), Format::Json(true));

    let rustac = Rustac::parse_from([
        "rustac",
        "--output-format",
        "parquet",
        "--parquet-compression",
        "lzo",
        "translate",
    ]);
    assert_eq!(
        rustac.output_format(None),
        Format::Geoparquet(WriterOptions::new().with_compression(Some(Compression::LZO)))
    );

    let rustac = Rustac::parse_from([
        "rustac",
        "--output-format",
        "parquet",
        "--parquet-max-row-group-size",
        "50000",
        "translate",
    ]);
    assert_eq!(
        rustac.output_format(None),
        Format::Geoparquet(WriterOptions::new().with_max_row_group_size(50000))
    );

    let rustac = Rustac::parse_from([
        "rustac",
        "--output-format",
        "parquet",
        "--parquet-compression",
        "snappy",
        "--parquet-max-row-group-size",
        "100000",
        "translate",
    ]);
    assert_eq!(
        rustac.output_format(None),
        Format::Geoparquet(
            WriterOptions::new()
                .with_compression(Some(Compression::SNAPPY))
                .with_max_row_group_size(100000)
        )
    );
}

#[rstest]
fn validate(mut command: Command) {
    command
        .arg("validate")
        .arg("examples/simple-item.json")
        .assert()
        .success();
    command
        .arg("validate")
        .arg("data/invalid-item.json")
        .assert()
        .failure();
}
