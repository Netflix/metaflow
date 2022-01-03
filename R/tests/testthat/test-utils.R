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

test_that("can identify valid variable names for Python 2", {
  skip_if_no_metaflow()
  expect_identifier_validity <- function(identifier, valid) {
    eval(bquote(expect_equal(is_valid_python_identifier_py2(identifier), valid)))
  }

  expect_identifier_validity("metaflow", TRUE)
  expect_identifier_validity("metaflow1", TRUE)
  expect_identifier_validity("meta_flow", TRUE)
  expect_identifier_validity("META_FLOW", TRUE)
  expect_identifier_validity("meta1flow", TRUE)
  expect_identifier_validity("_metaflow", TRUE)
  expect_identifier_validity("__metaflow", TRUE)
  expect_identifier_validity("metaflow_", TRUE)
  expect_identifier_validity("1metaflow", FALSE)
  expect_identifier_validity("metaflow%", FALSE)
  expect_identifier_validity("meta flow", FALSE)
  expect_identifier_validity("metæflow", FALSE)
  expect_identifier_validity("æmetaflow", FALSE)
  expect_identifier_validity("metaflowæ", FALSE)
})

test_that("can identify valid variable names for Python 3", {
  skip_if_no_metaflow()
  expect_identifier_validity <- function(identifier, valid) {
    eval(bquote(expect_equal(is_valid_python_identifier_py3(identifier), valid)))
  }

  expect_identifier_validity("metaflow", TRUE)
  expect_identifier_validity("metaflow1", TRUE)
  expect_identifier_validity("meta_flow", TRUE)
  expect_identifier_validity("META_FLOW", TRUE)
  expect_identifier_validity("meta1flow", TRUE)
  expect_identifier_validity("_metaflow", TRUE)
  expect_identifier_validity("__metaflow", TRUE)
  expect_identifier_validity("metaflow_", TRUE)
  expect_identifier_validity("1metaflow", FALSE)
  expect_identifier_validity("metaflow%", FALSE)
  expect_identifier_validity("meta flow", FALSE)
  expect_identifier_validity("metæflow", TRUE)
  expect_identifier_validity("æmetaflow", TRUE)
  expect_identifier_validity("metaflowæ", TRUE)
})

test_that("can identify valid variable names for Python with version detection", {
  skip_if_no_metaflow()
  # The Python version here is most likely 3
  expect_identifier_validity <- function(identifier, valid) {
    eval(bquote(expect_equal(is_valid_python_identifier(identifier), valid)))
  }

  expect_identifier_validity("metaflow", TRUE)
  expect_identifier_validity("metaflow1", TRUE)
  expect_identifier_validity("meta_flow", TRUE)
  expect_identifier_validity("META_FLOW", TRUE)
  expect_identifier_validity("meta1flow", TRUE)
  expect_identifier_validity("_metaflow", TRUE)
  expect_identifier_validity("__metaflow", TRUE)
  expect_identifier_validity("metaflow_", TRUE)
  expect_identifier_validity("1metaflow", FALSE)
  expect_identifier_validity("metaflow%", FALSE)
  expect_identifier_validity("meta flow", FALSE)
})
