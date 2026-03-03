test_that("stac_read reads a JSON item as a list", {
  item <- stac_read(fixture("spec-examples", "v1.1.0", "simple-item.json"))
  expect_type(item, "list")
  expect_equal(item$type, "Feature")
  expect_equal(item$id, "20201211_223832_CS2")
})

test_that("stac_read reads a catalog as a list", {
  catalog <- stac_read(fixture("spec-examples", "v1.1.0", "catalog.json"))
  expect_type(catalog, "list")
  expect_equal(catalog$type, "Catalog")
  expect_equal(catalog$id, "examples")
})

test_that("stac_read reads a collection as a list", {
  collection <- stac_read(fixture("spec-examples", "v1.1.0", "collection.json"))
  expect_type(collection, "list")
  expect_equal(collection$type, "Collection")
  expect_equal(collection$id, "simple-collection")
})

test_that("stac_read reads an item collection as an sf data frame", {
  ic <- stac_read(fixture("crates", "io", "data", "item-collection.json"))
  expect_s3_class(ic, "sf")
  expect_gt(nrow(ic), 0)
})

test_that("stac_read reads geoparquet as an sf data frame", {
  sf <- stac_read(fixture("crates", "io", "data", "extended-item.parquet"))
  expect_s3_class(sf, "sf")
  expect_gt(nrow(sf), 0)
})
