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
})

test_that("decorator without arguments parsed correctly", {
  skip_if_no_metaflow()
  actual <- decorator("batch")[1]
  expected <- "@batch"
  expect_equal(actual, expected)
})

test_that("@catch parsed correctly", {
  skip_if_no_metaflow()
  actual <- decorator("catch", var = "model_failed")[1]
  expected <- "@catch(var='model_failed')"
  expect_equal(actual, expected)
  actual <- decorator("catch", print_exception = FALSE, var = "timeout")[1]
  expected <- "@catch(print_exception=False, var='timeout')"
  expect_equal(actual, expected)
})

test_that("@timeout parsed correctly", {
  skip_if_no_metaflow()
  actual <- decorator("timeout", seconds = 5)[1]
  expected <- "@timeout(seconds=5)"
  expect_equal(actual, expected)
})

test_that("@resources parsed correctly", {
  skip_if_no_metaflow()
  actual <- decorator("resources", cpu = 16, memory = 220000, disk = 150000, network = 4000)[1]
  expected <- "@resources(cpu=16, memory=220000, disk=150000, network=4000)"
  expect_equal(actual, expected)
})

test_that("@batch parsed corectly", {
  skip_if_no_metaflow()
  actual <- decorator("batch", memory = 60000, cpu = 8)[1]
  expected <- "@batch(memory=60000, cpu=8)"
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
