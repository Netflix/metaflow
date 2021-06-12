context("test-utils.R")

test_that("%||% coalesces NULLs", {
  expect_equal("red panda" %||% NULL, "red panda")
  expect_equal(NULL %||% "red panda", "red panda")
  expect_equal(NULL %||% NULL %||% "red panda", "red panda")
  expect_null(NULL %||% NULL)
})

test_that("serialize functions work properly", {
  skip_if_no_metaflow()
  py_obj <- mf_serialize(mtcars)
  returned_obj <- mf_deserialize(py_obj)
  expect_equal(mtcars, returned_obj)
})
