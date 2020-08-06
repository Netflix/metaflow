context("test-utils.R")

test_that("serialize functions work properly", {
  skip_if_no_metaflow()
  py_obj <- mf_serialize(mtcars)
  returned_obj <- mf_deserialize(py_obj)
  expect_equal(mtcars, returned_obj)
})
