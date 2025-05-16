use crate::{Format, Result, Writeable};
use object_store::PutResult;

/// Puts a value, maybe to an object store.
///
/// # Examples
///
/// ```no_run
/// use stac::Item;
///
/// #[cfg(feature = "object-store-aws")]
/// {
/// let item = Item::new("an-item");
/// # tokio_test::block_on(async {
///     stac_io::put("s3://bucket/an-item.json", item).await.unwrap();
/// # })
/// }
/// ```
pub async fn put<T>(href: impl ToString, value: T) -> Result<Option<PutResult>>
where
    T: Writeable,
{
    let options: [(&str, &str); 0] = [];
    put_opts(href, value, options).await
}

/// Puts a value, maybe to an object store with options.
///
/// # Examples
///
/// ```no_run
/// use stac::Item;
///
/// #[cfg(feature = "object-store-aws")]
/// {
/// let item = Item::new("an-item");
/// # tokio_test::block_on(async {
///     stac_io::put_opts("s3://bucket/an-item.json", item, [("aws_access_key_id", "...")]).await.unwrap();
/// # })
/// }
/// ```
pub async fn put_opts<T, I, K, V>(
    href: impl ToString,
    value: T,
    options: I,
) -> Result<Option<PutResult>>
where
    T: Writeable,
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let href = href.to_string();
    let format = Format::infer_from_href(&href).unwrap_or_default();
    format.put_opts(href, value, options).await
}
