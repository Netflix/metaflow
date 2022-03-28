context("test-flow.R")

teardown(if ("sqrt" %in% names(.GlobalEnv)) rm("sqrt", envir = .GlobalEnv))

test_that("header() formatted correctly", {
  skip_if_no_metaflow()
  actual <- header("TestFlow")
  expected <- "from metaflow import FlowSpec, step, Parameter, retry, environment, batch, catch, resources, schedule\nfrom metaflow.R import call_r\n\n\nclass TestFlow(FlowSpec):\n"
  expect_equal(actual, expected)
})

test_that("footer() formatted correctly", {
  skip_if_no_metaflow()
  actual <- footer("TestFlow")
  expected <- "FLOW=TestFlow\nif __name__ == '__main__':\n    TestFlow()"
  expect_equal(actual, expected)
})

test_that("get_flow() returns correct string", {
  skip_if_no_metaflow()
  metaflow("TestFlow") %>%
    step(
      step = "start",
      next_step = "middle"
    ) %>%
    step(
      step = "middle",
      next_step = "end"
    ) %>%
    step(step = "end")
  actual <- TestFlow$get_flow()
  expected <- "from metaflow import FlowSpec, step, Parameter, retry, environment, batch, catch, resources, schedule\nfrom metaflow.R import call_r\n\n\nclass TestFlow(FlowSpec):\n\n    @step\n    def start(self):\n        self.next(self.middle)\n\n    @step\n    def middle(self):\n        self.next(self.end)\n\n    @step\n    def end(self):\n        pass\n\n\nFLOW=TestFlow\nif __name__ == '__main__':\n    TestFlow()"
  expect_equal(actual, expected)
  TestFlow$get_flow(save = TRUE)
  actual <- readChar("flow.py", nchars = nchar(expected))
  expect_equal(actual, expected)
  on.exit(file.remove("flow.py"))
})

test_that("get_functions() works", {
  skip_if_no_metaflow()
  start <- function(self) {
    print("start")
  }
  end <- function(self) {
    print("end")
  }
  metaflow("TestFlow") %>%
    step(
      step = "start",
      r_function = start,
      next_step = "end"
    ) %>%
    step(step = "end")
  actual <- TestFlow$get_functions()
  expected <- list(start = function(self) {
    original_func <- function() {
      print("start")
    }
    original_func()
    return(0)
  })
  expect_equal(actual, expected)
  metaflow("TestFlow") %>%
    step(
      step = "start",
      r_function = start,
      next_step = "end"
    ) %>%
    step(
      step = "end",
      r_function = end
    )
  actual <- TestFlow$get_functions()
  expected <- list(
    start = function(self) {
      original_func <- function() {
        print("start")
      }
      original_func()
      return(0)
    },
    end = function(self) {
      original_func <- function() {
        print("end")
      }
      original_func()
      return(0)
    }
  )
  expect_equal(actual, expected)
})

test_that("flow names are assigned to global environment", {
  expect_false("sqrt" %in% names(.GlobalEnv))
  step(metaflow("sqrt"), step = "start")
  expect_true("sqrt" %in% names(.GlobalEnv))
  expect_s3_class(get("sqrt", envir = .GlobalEnv), "Flow")
  expect_equal(base::sqrt(4), 2)
})
