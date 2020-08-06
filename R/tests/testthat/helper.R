skip_if_no_metaflow <- function() {
  have_metaflow <- reticulate::py_module_available("metaflow")
  if (!have_metaflow) {
    skip("metaflow not available for testing")
  }
}
