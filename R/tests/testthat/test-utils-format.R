context("test-utils-format.R")

test_that("quotes are properly escaped", {
  skip_if_no_metaflow()
  actual <- escape_quote("TRUE")
  expected <- "True"
  expect_equal(actual, expected)
  actual <- escape_quote("parameter")
  expected <- "'parameter'"
  expect_equal(actual, expected)
})
