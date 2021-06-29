context("test-decorators.R")

test_that("error on duplicate arguments", {
  skip_if_no_metaflow()
  expect_error(decorator_arguments(list(cpu = 10, cpu = 10)))
})

test_that("decorator arguments parsed correctly", {
  skip_if_no_metaflow()
  
  actual <- decorator_arguments(list(cpu = 10))
  expected <- "cpu=10"
  expect_equal(actual, expected)
  
  actual <- decorator_arguments(list(memory = 60000, cpu = 10))
  expected <- "memory=60000, cpu=10"
  expect_equal(actual, expected)
  
  actual <- decorator_arguments(list(memory = 60000, image = NULL))
  expected <- "memory=60000, image=None"
  expect_equal(actual, expected)
  
  actual <- decorator_arguments(list(abc = "red panda"), .convert_args = FALSE)
  expected <- "abc=red panda" # invalid Python because we're not converting
  expect_equal(actual, expected)
})

test_that("decorator without arguments parsed correctly", {
  skip_if_no_metaflow()
  actual <- decorator("batch")[1]
  expected <- "@batch"
  expect_equal(actual, expected)
})

test_that("@timeout parsed correctly", {
  skip_if_no_metaflow()
  actual <- decorator("timeout", seconds = 5)[1]
  expected <- "@timeout(seconds=5)"
  expect_equal(actual, expected)
})

test_that("add_decorators takes multiple args", {
  skip_if_no_metaflow()
  actual <- add_decorators(
    list(
      decorator("catch"),
      decorator("batch", memory = 60000, cpu = 8)
    )
  )
  expected <- c("@catch", "\n", "@batch(memory=60000, cpu=8)", "\n")
  expect_equal(actual, expected)
})

test_that("decorator with unnamed arguments errors", {
  skip_if_no_metaflow()
  expect_error(
    decorator("batch", memoy = 60000, 8),
    "All arguments to a decorator must be named"
  )
})
