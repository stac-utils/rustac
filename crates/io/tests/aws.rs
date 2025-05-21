use stac::Catalog;

#[test]
fn read_from_s3() {
    tokio_test::block_on(async {
        let (store, path) = stac_io::parse_href_opts(
            "s3://nz-elevation/catalog.json",
            [("skip_signature", "true"), ("region", "ap-southeast-2")],
        )
        .unwrap();
        let _: Catalog = store.get(path).await.unwrap();
    });
}
