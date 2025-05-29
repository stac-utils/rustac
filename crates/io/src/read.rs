use crate::{Format, Readable, Result};
use stac::SelfHref;

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
pub fn read<T: SelfHref + Readable>(href: impl ToString) -> Result<T> {
    let href = href.to_string();
    let format = Format::infer_from_href(&href).unwrap_or_default();
    format.read(href)
}
