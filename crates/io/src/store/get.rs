use crate::{Format, Readable, Result};
use stac::{Href, SelfHref};

/// Gets a value, maybe from an object store.
///
/// # Examples
///
/// ```no_run
/// use stac::Item;
///
/// #[cfg(feature = "object-store-aws")]
/// {
/// # tokio_test::block_on(async {
///     let item: Item = stac_io::get("s3://bucket/item.json").await.unwrap();
/// # })
/// }
/// ```
pub async fn get<T: SelfHref + Readable>(href: impl Into<Href>) -> Result<T> {
    let options: [(&str, &str); 0] = [];
    get_opts(href, options).await
}

/// Gets a value, maybe from an object store with the provided options.
///
/// If `href` is a url, [object_store::parse_url_opts] will be used to build the object store to get the value.
///
/// # Examples
///
/// ```no_run
/// use stac::Item;
///
/// #[cfg(feature = "object-store-aws")]
/// {
/// # tokio_test::block_on(async {
///     let item: Item = stac_io::get_opts("s3://bucket/item.json", [("aws_access_key_id", "...")]).await.unwrap();
/// # })
/// }
/// ```
pub async fn get_opts<T, I, K, V>(href: impl Into<Href>, options: I) -> Result<T>
where
    T: SelfHref + Readable,
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let href = href.into();
    let format = Format::infer_from_href(href.as_str()).unwrap_or_default();
    format.get_opts(href, options).await
}
