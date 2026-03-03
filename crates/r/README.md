# rustac

R package for reading and writing [SpatioTemporal Asset Catalog (STAC)](https://stacspec.org/) data in JSON, NDJSON, and [stac-geoparquet](https://github.com/stac-utils/stac-geoparquet) formats.
Powered by Rust via [extendr](https://extendr.github.io/extendr/extendr_api/).

## Prerequisites

- **Rust** toolchain (rustc >= 1.88): <https://rustup.rs/>

## Install

Install directly from GitHub using [pak](https://pak.r-lib.org/) (recommended):

```r
# install.packages("pak")
pak::pak("stac-utils/rustac/crates/r")
```

Or with `devtools`:

```r
# install.packages("devtools")
devtools::install_github("stac-utils/rustac", subdir = "crates/r")
```

### From source

From the repository root:

```bash
R CMD build crates/r
R CMD INSTALL rustac_0.1.0.tar.gz
```

### Troubleshooting

If `arrow` or `sf` fail to install, try [r-universe](https://r-universe.dev/) binaries first:

```r
install.packages(
  c("arrow", "sf"),
  repos = c("https://apache.r-universe.dev", "https://r-spatial.r-universe.dev", "https://cloud.r-project.org")
)
```

Then retry the install.

## Development

For iterative development, use `devtools`:

```r
devtools::load_all("crates/r")
devtools::test("crates/r")
```

To run the full R CMD check:

```bash
R CMD build crates/r
R CMD check rustac_0.1.0.tar.gz
```

## Usage

```r
library(rustac)

# Read a STAC item (returns an R list)
item <- stac_read("spec-examples/v1.1.0/simple-item.json")
item$id
#> [1] "20201211_223832_CS2"

# Read stac-geoparquet (returns an sf data frame)
sf <- stac_read("data/extended-item.parquet")
class(sf)
#> [1] "sf"         "data.frame"

# Write to JSON
stac_write(item, "output.json")

# Write to stac-geoparquet
stac_write(sf, "output.parquet")
```
