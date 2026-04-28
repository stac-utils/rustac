test_that("stac_write writes a list as JSON and reads it back", {
  item <- stac_read(fixture("spec-examples", "v1.1.0", "simple-item.json"))
  outfile <- tempfile(fileext = ".json")
  on.exit(unlink(outfile))

  stac_write(item, outfile)
  result <- stac_read(outfile)
  expect_equal(result$id, item$id)
  expect_equal(result$type, item$type)
})

test_that("stac_write writes geoparquet and reads it back", {
  sf <- stac_read(fixture("crates", "io", "data", "extended-item.parquet"))
  outfile <- tempfile(fileext = ".parquet")
  on.exit(unlink(outfile))

  stac_write(sf, outfile)
  result <- stac_read(outfile)
  expect_s3_class(result, "sf")
  expect_equal(nrow(result), nrow(sf))
})

test_that("stac_write roundtrips item collection through JSON", {
  ic <- stac_read(fixture("crates", "io", "data", "item-collection.json"))
  outfile <- tempfile(fileext = ".json")
  on.exit(unlink(outfile))

  stac_write(ic, outfile)
  expect_true(file.exists(outfile))
  result <- jsonlite::fromJSON(outfile, simplifyVector = FALSE)
  expect_equal(result$type, "FeatureCollection")
})
