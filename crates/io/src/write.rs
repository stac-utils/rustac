use crate::{Format, Result, Writeable};
use std::path::Path;

/// Writes a STAC value to a path.
///
/// The format will be inferred from the href's extension. If you want to
/// specify the format, use [Format::write].
///
/// # Examples
///
/// ```no_run
/// use stac::Item;
///
/// let item = Item::new("an-id");
/// stac_io::write("an-id.json", item).unwrap();
/// ```
pub fn write<T: Writeable>(path: impl AsRef<Path>, value: T) -> Result<()> {
    let path = path.as_ref();
    let format = path
        .to_str()
        .and_then(Format::infer_from_href)
        .unwrap_or_default();
    format.write(path, value)
}
