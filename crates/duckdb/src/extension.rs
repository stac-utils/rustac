/// A DuckDB extension
// TODO implement aliases ... I don't know how vectors work yet 😢
#[derive(Debug)]
pub struct Extension {
    /// The extension name.
    pub name: String,

    /// Is the extension loaded?
    pub loaded: bool,

    /// Is the extension installed?
    pub installed: bool,

    /// The path to the extension.
    ///
    /// This might be `(BUILT-IN)` for the core extensions.
    pub install_path: Option<String>,

    /// The extension description.
    pub description: String,

    /// The extension version.
    pub version: Option<String>,

    /// The install mode.
    ///
    /// We don't bother making this an enum, yet.
    pub install_mode: Option<String>,

    /// Where the extension was installed from.
    pub installed_from: Option<String>,
}
