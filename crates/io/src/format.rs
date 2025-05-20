use crate::{Error, Readable, RealizedHref, Result, Writeable};
use bytes::Bytes;
use stac::{Href, SelfHref};
use std::{fmt::Display, path::Path, str::FromStr};

/// The format of STAC data.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum Format {
    /// JSON data (the default).
    ///
    /// If `true`, the data will be pretty-printed on write.
    Json(bool),

    /// Newline-delimited JSON.
    NdJson,

    /// [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet)
    #[cfg(feature = "geoparquet")]
    Geoparquet(Option<stac::geoparquet::Compression>),
}

impl Format {
    /// Infer the format from a file extension.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac_io::Format;
    ///
    /// assert_eq!(Format::Json(false), Format::infer_from_href("item.json").unwrap());
    /// ```
    pub fn infer_from_href(href: &str) -> Option<Format> {
        href.rsplit_once('.').and_then(|(_, ext)| ext.parse().ok())
    }

    /// Returns true if this is a geoparquet href.
    #[cfg(feature = "geoparquet")]
    pub fn is_geoparquet_href(href: &str) -> bool {
        matches!(Format::infer_from_href(href), Some(Format::Geoparquet(_)))
    }

    /// Reads a STAC object from an href in this format.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_io::Format;
    ///
    /// let item: Item = Format::json().read("examples/simple-item.json").unwrap();
    /// ```
    #[allow(unused_variables)]
    pub fn read<T: Readable + SelfHref>(&self, href: impl Into<Href>) -> Result<T> {
        let mut href = href.into();
        let mut value: T = match href.clone().into() {
            RealizedHref::Url(url) => {
                #[cfg(feature = "reqwest")]
                {
                    let bytes = reqwest::blocking::get(url)?.bytes()?;
                    self.from_bytes(bytes)?
                }
                #[cfg(not(feature = "reqwest"))]
                {
                    return Err(Error::FeatureNotEnabled("reqwest"));
                }
            }
            RealizedHref::PathBuf(path) => {
                let path = path.canonicalize()?;
                let value = self.from_path(&path)?;
                href = path.as_path().into();
                value
            }
        };
        value.set_self_href(href);
        Ok(value)
    }

    /// Reads a local file in the given format.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_io::Format;
    ///
    /// let item: Item = Format::json().from_path("examples/simple-item.json").unwrap();
    /// ```
    pub fn from_path<T: Readable + SelfHref>(&self, path: impl AsRef<Path>) -> Result<T> {
        let path = path.as_ref().canonicalize()?;
        match self {
            Format::Json(_) => T::from_json_path(&path),
            Format::NdJson => T::from_ndjson_path(&path),
            #[cfg(feature = "geoparquet")]
            Format::Geoparquet(_) => T::from_geoparquet_path(&path),
        }
        .map_err(|err| {
            if let Error::Io(err) = err {
                Error::FromPath {
                    io: err,
                    path: path.to_string_lossy().into_owned(),
                }
            } else {
                err
            }
        })
    }

    /// Reads a STAC object from some bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_io::Format;
    /// use std::{io::Read, fs::File};
    ///
    /// let mut buf = Vec::new();
    /// File::open("examples/simple-item.json").unwrap().read_to_end(&mut buf).unwrap();
    /// let item: Item = Format::json().from_bytes(buf).unwrap();
    /// ```
    pub fn from_bytes<T: Readable>(&self, bytes: impl Into<Bytes>) -> Result<T> {
        let value = match self {
            Format::Json(_) => T::from_json_slice(&bytes.into())?,
            Format::NdJson => T::from_ndjson_bytes(bytes)?,
            #[cfg(feature = "geoparquet")]
            Format::Geoparquet(_) => T::from_geoparquet_bytes(bytes)?,
        };
        Ok(value)
    }

    /// Writes a STAC value to the provided path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use stac::Item;
    /// use stac_io::Format;
    ///
    /// Format::json().write("an-id.json", Item::new("an-id")).unwrap();
    /// ```
    pub fn write<T: Writeable>(&self, path: impl AsRef<Path>, value: T) -> Result<()> {
        match self {
            Format::Json(pretty) => value.to_json_path(path, *pretty),
            Format::NdJson => value.to_ndjson_path(path),
            #[cfg(feature = "geoparquet")]
            Format::Geoparquet(compression) => value.into_geoparquet_path(path, *compression),
        }
    }

    /// Converts a STAC object into some bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use stac::Item;
    /// use stac_io::Format;
    ///
    /// let item = Item::new("an-id");
    /// let bytes = Format::json().into_vec(item).unwrap();
    /// ```
    pub fn into_vec<T: Writeable>(&self, value: T) -> Result<Vec<u8>> {
        let value = match self {
            Format::Json(pretty) => value.to_json_vec(*pretty)?,
            Format::NdJson => value.to_ndjson_vec()?,
            #[cfg(feature = "geoparquet")]
            Format::Geoparquet(compression) => value.into_geoparquet_vec(*compression)?,
        };
        Ok(value)
    }

    /// Returns the default JSON format (compact).
    pub fn json() -> Format {
        Format::Json(false)
    }

    /// Returns the newline-delimited JSON format.
    pub fn ndjson() -> Format {
        Format::NdJson
    }

    /// Returns the default geoparquet format (snappy compression if compression is enabled).
    #[cfg(feature = "geoparquet")]
    pub fn geoparquet() -> Format {
        #[cfg(feature = "geoparquet-compression")]
        {
            Format::Geoparquet(Some(stac::geoparquet::Compression::SNAPPY))
        }
        #[cfg(not(feature = "geoparquet-compression"))]
        {
            Format::Geoparquet(None)
        }
    }
}

impl Default for Format {
    fn default() -> Self {
        Self::Json(false)
    }
}

impl Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json(pretty) => {
                if *pretty {
                    f.write_str("json-pretty")
                } else {
                    f.write_str("json")
                }
            }
            Self::NdJson => f.write_str("ndjson"),
            #[cfg(feature = "geoparquet")]
            Self::Geoparquet(compression) => {
                if let Some(compression) = *compression {
                    write!(f, "geoparquet[{}]", compression)
                } else {
                    f.write_str("geoparquet")
                }
            }
        }
    }
}

impl FromStr for Format {
    type Err = Error;

    #[cfg_attr(not(feature = "geoparquet"), allow(unused_variables))]
    fn from_str(s: &str) -> Result<Format> {
        match s.to_ascii_lowercase().as_str() {
            "json" | "geojson" => Ok(Self::Json(false)),
            "json-pretty" | "geojson-pretty" => Ok(Self::Json(true)),
            "ndjson" => Ok(Self::NdJson),
            _ => {
                #[cfg(feature = "geoparquet")]
                {
                    infer_geoparquet_format(s)
                }
                #[cfg(not(feature = "geoparquet"))]
                Err(Error::UnsupportedFormat(s.to_string()))
            }
        }
    }
}

#[cfg(feature = "geoparquet")]
fn infer_geoparquet_format(s: &str) -> Result<Format> {
    if s.starts_with("parquet") || s.starts_with("geoparquet") {
        if let Some((_, compression)) = s.split_once('[') {
            if let Some(stop) = compression.find(']') {
                let format = compression[..stop]
                    .parse()
                    .map(Some)
                    .map(Format::Geoparquet)?;
                Ok(format)
            } else {
                Err(Error::UnsupportedFormat(s.to_string()))
            }
        } else if cfg!(feature = "geoparquet-compression") {
            Ok(Format::Geoparquet(Some(
                stac::geoparquet::Compression::SNAPPY,
            )))
        } else {
            Ok(Format::Geoparquet(None))
        }
    } else {
        Err(Error::UnsupportedFormat(s.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::Format;

    #[test]
    #[cfg(not(feature = "geoparquet"))]
    fn parse_geoparquet() {
        assert!(matches!(
            "parquet".parse::<Format>().unwrap_err(),
            crate::Error::UnsupportedFormat(_),
        ));
    }

    #[cfg(feature = "geoparquet")]
    mod geoparquet {
        use super::Format;
        use stac::geoparquet::Compression;

        #[test]
        fn parse_geoparquet_compression() {
            let format: Format = "geoparquet[snappy]".parse().unwrap();
            assert_eq!(format, Format::Geoparquet(Some(Compression::SNAPPY)));
        }

        #[test]
        #[cfg(feature = "geoparquet-compression")]
        fn infer_from_href() {
            assert_eq!(
                Format::Geoparquet(Some(Compression::SNAPPY)),
                Format::infer_from_href("out.parquet").unwrap()
            );
        }

        #[test]
        #[cfg(not(feature = "geoparquet-compression"))]
        fn infer_from_href() {
            assert_eq!(
                Format::Geoparquet(None),
                Format::infer_from_href("out.parquet").unwrap()
            );
        }
    }
}
