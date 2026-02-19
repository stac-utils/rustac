//! Utilities and structures for working with hrefs.

#[cfg(feature = "std")]
use crate::Error;
use crate::Result;
use std::borrow::Cow;
use url::Url;

/// Implemented by all three STAC objects, the [SelfHref] trait allows getting
/// and setting an object's href.
///
/// Though the self href isn't part of the data structure, it is useful to know
/// where a given STAC object was read from.  Objects created from scratch don't
/// have an href.
///
/// # Examples
///
/// ```
/// use stac::{Item, SelfHref};
///
/// let item = Item::new("an-id");
/// assert!(item.self_href().is_none());
/// let item: Item = stac::read("examples/simple-item.json").unwrap();
/// assert!(item.self_href().is_some());
/// ```
pub trait SelfHref {
    /// Gets this object's href.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{SelfHref, Item};
    ///
    /// let item: Item = stac::read("examples/simple-item.json").unwrap();
    /// assert!(item.self_href().unwrap().to_string().ends_with("simple-item.json"));
    /// ```
    fn self_href(&self) -> Option<&str>;

    /// Returns a mutable reference to this object's self href.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::{Item, SelfHref};
    ///
    /// let mut item = Item::new("an-id");
    /// *item.self_href_mut() = Option::Some("./a/relative/path.json".into());
    /// ```
    fn self_href_mut(&mut self) -> &mut Option<String>;

    /// Sets this object's self href.
    fn set_self_href(&mut self, href: impl ToString) {
        *self.self_href_mut() = Some(href.to_string())
    }

    /// Clear's this object's self href.
    fn clear_self_href(&mut self) {
        *self.self_href_mut() = None
    }
}

/// Returns `true` if the href looks like a Windows absolute path (e.g. `C:\foo` or `D:/bar`).
pub fn is_windows_absolute_path(s: &str) -> bool {
    let bytes = s.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && (bytes[2] == b'\\' || bytes[2] == b'/')
}

/// Returns `true` if the href is absolute.
///
/// An href is absolute if it can be parsed to a url, starts with a `/`, or is
/// a Windows absolute path.
pub fn is_absolute(href: &str) -> bool {
    is_windows_absolute_path(href) || Url::parse(href).is_ok() || href.starts_with('/')
}

/// Makes an href absolute relative to a base.
///
/// # Examples
///
/// ```
/// assert_eq!(stac::href::make_absolute("./item.json", "/a/b").unwrap(), "/a/item.json");
/// assert_eq!(stac::href::make_absolute("./item.json", "/a/b/").unwrap(), "/a/b/item.json");
/// assert_eq!(stac::href::make_absolute("http://stac.test/item.json", "/a/b/").unwrap(), "http://stac.test/item.json");
/// ```
pub fn make_absolute<'a>(href: &'a str, base: &str) -> Result<Cow<'a, str>> {
    if is_absolute(href) {
        Ok(href.into())
    } else if let Ok(url) = Url::parse(base) {
        let url = url.join(href)?;
        Ok(url.to_string().into())
    } else {
        let (base, _) = base.split_at(base.rfind('/').unwrap_or(0));
        if base.is_empty() {
            Ok(normalize_path(href).into())
        } else {
            Ok(normalize_path(&format!("{base}/{href}")).into())
        }
    }
}

/// Makes an href relative to a base.
pub fn make_relative(href: &str, base: &str) -> String {
    // Cribbed from `Url::make_relative`
    let mut relative = String::new();

    fn extract_path_filename(s: &str) -> (&str, &str) {
        let last_slash_idx = s.rfind('/').unwrap_or(0);
        let (path, filename) = s.split_at(last_slash_idx);
        if filename.is_empty() {
            (path, "")
        } else {
            (path, &filename[1..])
        }
    }

    let (base_path, base_filename) = extract_path_filename(base);
    let (href_path, href_filename) = extract_path_filename(href);

    let mut base_path = base_path.split('/').peekable();
    let mut href_path = href_path.split('/').peekable();

    while base_path.peek().is_some() && base_path.peek() == href_path.peek() {
        let _ = base_path.next();
        let _ = href_path.next();
    }

    for base_path_segment in base_path {
        if base_path_segment.is_empty() {
            break;
        }

        if !relative.is_empty() {
            relative.push('/');
        }

        relative.push_str("..");
    }

    for href_path_segment in href_path {
        if relative.is_empty() {
            relative.push_str("./");
        } else {
            relative.push('/');
        }

        relative.push_str(href_path_segment);
    }

    if !relative.is_empty() || base_filename != href_filename {
        if href_filename.is_empty() {
            relative.push('/');
        } else {
            if relative.is_empty() {
                relative.push_str("./");
            } else {
                relative.push('/');
            }
            relative.push_str(href_filename);
        }
    }

    relative
}

/// Converts this href to a Url.
///
/// Handles adding a `file://` prefix and making it absolute, if needed.
#[cfg(feature = "std")]
pub fn make_url(href: &str) -> Result<Url> {
    if is_windows_absolute_path(href) || href.starts_with('/') {
        Url::from_file_path(href).map_err(|_| Error::InvalidFilePath(href.to_string()))
    } else if let Ok(url) = Url::parse(href) {
        Ok(url)
    } else {
        let current_dir = std::env::current_dir()?;
        let url = Url::from_directory_path(&current_dir)
            .map_err(|_| Error::InvalidFilePath(current_dir.to_string_lossy().into_owned()))?;
        Ok(url.join(href)?)
    }
}

fn normalize_path(path: &str) -> String {
    let mut parts = if path.starts_with('/') {
        Vec::new()
    } else {
        vec![""]
    };
    for part in path.split('/') {
        match part {
            "." => {}
            ".." => {
                let _ = parts.pop();
            }
            s => parts.push(s),
        }
    }
    parts.join("/")
}
