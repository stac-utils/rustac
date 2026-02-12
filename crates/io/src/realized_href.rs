use std::path::PathBuf;
use url::Url;

/// An href that has been realized to a path or a url.
#[derive(Debug)]
pub enum RealizedHref {
    /// A path buf
    PathBuf(PathBuf),

    /// A url
    Url(Url),
}

impl From<&str> for RealizedHref {
    fn from(s: &str) -> RealizedHref {
        if stac::href::is_windows_absolute_path(s) {
            return RealizedHref::PathBuf(PathBuf::from(s));
        }
        if let Ok(url) = Url::parse(s) {
            if url.scheme() == "file" {
                url.to_file_path()
                    .map(RealizedHref::PathBuf)
                    .unwrap_or_else(|_| RealizedHref::Url(url))
            } else {
                RealizedHref::Url(url)
            }
        } else {
            RealizedHref::PathBuf(PathBuf::from(s))
        }
    }
}
