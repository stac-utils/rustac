#' Read a STAC value from a file or URL
#'
#' Reads STAC data from JSON, NDJSON, or geoparquet formats. The format is
#' inferred from the file extension. Geoparquet files are returned as sf data
#' frames; all other types are returned as R lists.
#'
#' @param path Path to a file or a URL.
#' @return An sf data frame (for geoparquet/FeatureCollections) or an R list.
#' @export
stac_read <- function(path) {
  if (is_geoparquet(path)) {
    ipc_bytes <- .Call(wrap__stac_read_geoparquet, path)
    stream <- nanoarrow::read_nanoarrow(ipc_bytes)
    sf::st_as_sf(stream)
  } else {
    json <- .Call(wrap__stac_read_json, path)
    value <- jsonlite::fromJSON(json, simplifyVector = FALSE)
    if (identical(value$type, "FeatureCollection")) {
      sf::read_sf(json)
    } else {
      value
    }
  }
}

#' Write a STAC value to a file
#'
#' Writes STAC data to JSON, NDJSON, or geoparquet formats. The format is
#' inferred from the file extension. sf data frames are written via geoarrow for
#' geoparquet output; all other values are serialized with jsonlite.
#'
#' @param x An sf data frame or an R list representing a STAC value.
#' @param path Output file path.
#' @export
stac_write <- function(x, path) {
  if (is_geoparquet(path)) {
    stream <- nanoarrow::as_nanoarrow_array_stream(sf::st_as_sf(x))
    buf <- nanoarrow::write_nanoarrow(stream, raw())
    invisible(.Call(wrap__stac_write_geoparquet, buf, path))
  } else {
    if (inherits(x, "sf")) {
      if (!requireNamespace("geojsonsf", quietly = TRUE)) {
        stop("Package 'geojsonsf' is required to write sf objects to non-parquet formats")
      }
      json <- geojsonsf::sf_geojson(x)
    } else {
      json <- jsonlite::toJSON(x, auto_unbox = TRUE, null = "null")
    }
    invisible(.Call(wrap__stac_write_json, as.character(json), path))
  }
}

is_geoparquet <- function(path) {
  grepl("\\.(parquet|geoparquet)$", path, ignore.case = TRUE)
}
