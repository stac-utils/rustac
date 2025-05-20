use stac::Catalog;
use stac_io::StacStore;

#[test]
fn read_from_s3() {
    tokio_test::block_on(async {
        let (store, path) = object_store::parse_url_opts(
            &"s3://nz-elevation/catalog.json".parse().unwrap(),
            [("skip_signature", "true"), ("region", "ap-southeast-2")],
        )
        .unwrap();
        let store = StacStore::from(store);
        let _: Catalog = store.get(path).await.unwrap();
    });
}
