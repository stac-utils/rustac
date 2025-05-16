use crate::{Format, Readable, Result};
use stac::{Href, SelfHref};

/// Reads a STAC value from an href.
///
/// The format will be inferred from the href's extension. If you want to
/// specify the format, use [Format::read].
///
/// # Examples
///
/// ```
/// let item: stac::Item = stac_io::read("examples/simple-item.json").unwrap();
/// ```
pub fn read<T: SelfHref + Readable>(href: impl Into<Href>) -> Result<T> {
    let href = href.into();
    let format = Format::infer_from_href(href.as_str()).unwrap_or_default();
    format.read(href)
}
