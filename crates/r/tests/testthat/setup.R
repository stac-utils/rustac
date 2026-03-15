workspace_root <- normalizePath(file.path(
  testthat::test_path(), "..", "..", "..", ".."
))

fixture <- function(...) {
  file.path(workspace_root, ...)
}
