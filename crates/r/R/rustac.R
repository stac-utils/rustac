#' Read a STAC value from a file or URL
#'
#' Reads STAC data from JSON, NDJSON, or geoparquet formats. The format is
#' inferred from the file extension. Geoparquet files are returned as sf data
#' frames via Arrow; all other types are returned as R lists.
#'
#' @param path Path to a file or a URL.
#' @return An sf data frame (for geoparquet/FeatureCollections) or an R list.
#' @export
stac_read <- function(path) {
  if (is_geoparquet(path)) {
    ipc_bytes <- wrap__stac_read_geoparquet(path)
    table <- arrow::read_ipc_stream(ipc_bytes, as_data_frame = FALSE)
    df <- as.data.frame(table)
    geom <- sf::st_as_sfc(structure(df$geometry, class = "WKB"), EWKB = TRUE)
    df$geometry <- NULL
    sf::st_sf(df, geometry = geom)
  } else {
    json <- wrap__stac_read_json(path)
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
#' inferred from the file extension. sf data frames are written via Arrow for
#' geoparquet output; all other values are serialized with jsonlite.
#'
#' @param x An sf data frame or an R list representing a STAC value.
#' @param path Output file path.
#' @export
stac_write <- function(x, path) {
  if (is_geoparquet(path)) {
    table <- arrow::as_arrow_table(sf::st_as_sf(x))
    buf <- arrow::write_ipc_stream(table, raw())
    invisible(wrap__stac_write_geoparquet(buf, path))
  } else {
    if (inherits(x, "sf")) {
      if (!requireNamespace("geojsonsf", quietly = TRUE)) {
        stop("Package 'geojsonsf' is required to write sf objects to non-parquet formats")
      }
      json <- geojsonsf::sf_geojson(x)
    } else {
      json <- jsonlite::toJSON(x, auto_unbox = TRUE, null = "null")
    }
    invisible(wrap__stac_write_json(as.character(json), path))
  }
}

is_geoparquet <- function(path) {
  grepl("\\.(parquet|geoparquet)$", path, ignore.case = TRUE)
}
