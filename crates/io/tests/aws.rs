#[test]
fn read_from_s3() {
    tokio_test::block_on(async {
        stac_io::get_opts::<stac::Catalog, _, _, _>(
            "s3://nz-elevation/catalog.json",
            [("skip_signature", "true"), ("region", "ap-southeast-2")],
        )
        .await
        .unwrap();
    });
}
