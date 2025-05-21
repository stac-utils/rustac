//! Utilities and structures for dealing with hrefs.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Display,
    path::{Path, PathBuf},
};
use url::Url;

/// An href.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Href {
    /// A url href.
    ///
    /// This _can_ have a `file:` scheme.
    Url(Url),

    /// A string href.
    ///
    /// This is expected to have `/` delimiters. Windows-style `\` delimiters are not supported.
    String(String),
}

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
    fn self_href(&self) -> Option<&Href>;

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
    fn self_href_mut(&mut self) -> &mut Option<Href>;

    /// Sets this object's self href.
    fn set_self_href(&mut self, href: impl Into<Href>) {
        *self.self_href_mut() = Some(href.into())
    }

    /// Clear's this object's self href.
    fn clear_self_href(&mut self) {
        *self.self_href_mut() = None
    }
}

impl Href {
    /// Convert this href into an absolute href using the given base.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Href;
    ///
    /// let href = Href::from("./a/b.json").into_absolute(Href::from("/c/d/e.json")).unwrap();
    /// assert_eq!(href, "/c/d/a/b.json");
    /// ```
    pub fn into_absolute(&self, base: impl AsRef<Href>) -> Result<Href> {
        let base = base.as_ref();
        tracing::debug!("making href={self} absolute with base={base}");
        match base {
            Href::Url(url) => url.join(self.as_str()).map(Href::Url).map_err(Error::from),
            Href::String(s) => Ok(Href::String(make_absolute(self.as_str(), s))),
        }
    }

    /// Convert this href into an relative href using to the given base.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Href;
    ///
    /// let href = Href::from("/a/b/c.json").into_relative(Href::from("/a/d.json")).unwrap();
    /// assert_eq!(href, "./b/c.json");
    /// ```
    pub fn into_relative(&self, base: impl AsRef<Href>) -> Result<Href> {
        let base = base.as_ref();
        tracing::debug!("making href={self} relative with base={base}");
        match base {
            Href::Url(base) => match self {
                Href::Url(url) => Ok(base
                    .make_relative(url)
                    .map(Href::String)
                    .unwrap_or_else(|| self.clone())),
                Href::String(s) => {
                    let url = s.parse()?;
                    Ok(base
                        .make_relative(&url)
                        .map(Href::String)
                        .unwrap_or_else(|| self.clone()))
                }
            },
            Href::String(s) => Ok(Href::String(make_relative(self.as_str(), s))),
        }
    }

    /// Returns true if this href is absolute.
    ///
    /// Urls are always absolute. Strings are absolute if they start with a `/`.
    pub fn is_absolute(&self) -> bool {
        match self {
            Href::Url(_) => true,
            Href::String(s) => s.starts_with('/'),
        }
    }

    /// Returns this href as a str.
    pub fn as_str(&self) -> &str {
        match self {
            Href::Url(url) => url.as_str(),
            Href::String(s) => s.as_str(),
        }
    }
}

impl Display for Href {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Href::Url(url) => url.fmt(f),
            Href::String(s) => s.fmt(f),
        }
    }
}

impl From<&str> for Href {
    fn from(value: &str) -> Self {
        if let Ok(url) = Url::parse(value) {
            Href::Url(url)
        } else {
            Href::String(value.to_string())
        }
    }
}

impl From<String> for Href {
    fn from(value: String) -> Self {
        if let Ok(url) = Url::parse(&value) {
            Href::Url(url)
        } else {
            Href::String(value)
        }
    }
}

impl TryFrom<Href> for Url {
    type Error = Error;
    fn try_from(value: Href) -> Result<Self> {
        match value {
            Href::Url(url) => Ok(url),
            Href::String(mut s) => {
                if !s.starts_with("/") {
                    s = std::env::current_dir()?
                        .join(s)
                        .to_string_lossy()
                        .into_owned();
                }
                let url = Url::parse(&format!("file://{s}"))?;
                Ok(url)
            }
        }
    }
}

impl From<Url> for Href {
    fn from(value: Url) -> Self {
        Href::Url(value)
    }
}

impl From<&Path> for Href {
    fn from(value: &Path) -> Self {
        #[cfg(target_os = "windows")]
        if let Ok(url) = Url::from_file_path(value) {
            Href::Url(url)
        } else {
            Href::String(value.to_string_lossy().into_owned())
        }
        #[cfg(not(target_os = "windows"))]
        {
            Href::String(value.to_string_lossy().into_owned())
        }
    }
}

impl From<PathBuf> for Href {
    fn from(value: PathBuf) -> Self {
        #[cfg(target_os = "windows")]
        if let Ok(url) = Url::from_file_path(&value) {
            Href::Url(url)
        } else {
            Href::String(value.to_string_lossy().into_owned())
        }
        #[cfg(not(target_os = "windows"))]
        Href::String(value.to_string_lossy().into_owned())
    }
}

impl PartialEq<&str> for Href {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq(*other)
    }
}

impl AsRef<Href> for Href {
    fn as_ref(&self) -> &Href {
        self
    }
}

fn make_absolute(href: &str, base: &str) -> String {
    if href.starts_with('/') {
        href.to_string()
    } else {
        let (base, _) = base.split_at(base.rfind('/').unwrap_or(0));
        if base.is_empty() {
            normalize_path(href)
        } else {
            normalize_path(&format!("{}/{}", base, href))
        }
    }
}

/// Normalizes a path by removing extra ".." and ".".
pub fn normalize_path(path: &str) -> String {
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

fn make_relative(href: &str, base: &str) -> String {
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

#[cfg(test)]
mod tests {
    use super::Href;
    use url::Url;

    #[test]
    fn href_to_url() {
        let href = Href::from("examples/simple-item.json");
        let url: Url = href.try_into().unwrap();
        assert_eq!(url.scheme(), "file");
        assert!(url.path().ends_with("examples/simple-item.json"));
    }
}
