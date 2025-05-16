use stac::Href;
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

impl From<Href> for RealizedHref {
    fn from(href: Href) -> RealizedHref {
        match href {
            Href::Url(url) => {
                if url.scheme() == "file" {
                    url.to_file_path()
                        .map(RealizedHref::PathBuf)
                        .unwrap_or_else(|_| RealizedHref::Url(url))
                } else {
                    RealizedHref::Url(url)
                }
            }
            Href::String(s) => RealizedHref::PathBuf(PathBuf::from(s)),
        }
    }
}
