context("test-metaflow.R")

test_that("metaflow() creates flow object", {
  skip_if_no_metaflow()
  metaflow("TestFlow")
  expect_true(exists("TestFlow"))
})
