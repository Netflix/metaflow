library(reticulate)
context("test-step.R")

test_that("can't define step with an invalid name", {
  skip_if_no_metaflow()
  expect_error(
    metaflow("TestFlow") %>%
      step(
        step = "meta flow", # invalid Python identifier because of the space
        next_step = "end"
      ),
    "meta flow is not a valid step name"
  )
})

test_that("test join step", {
  skip_if_no_metaflow()
  metaflow("TestFlow") %>%
    step(
      step = "join",
      join = TRUE,
      next_step = "end"
    )
  actual <- TestFlow$get_flow()
  expected <- "from metaflow import FlowSpec, step, Parameter, retry, environment, batch, catch, resources, schedule\nfrom metaflow.R import call_r\n\n\nclass TestFlow(FlowSpec):\n\n    @step\n    def join(self, inputs):\n        self.next(self.end)\n\n\nFLOW=TestFlow\nif __name__ == '__main__':\n    TestFlow()"
  expect_equal(actual, expected)
})

test_that("test foreach step", {
  skip_if_no_metaflow()
  metaflow("TestFlow") %>%
    step(
      step = "join",
      foreach = "parameters",
      next_step = "end"
    )
  actual <- TestFlow$get_flow()
  expected <- "from metaflow import FlowSpec, step, Parameter, retry, environment, batch, catch, resources, schedule\nfrom metaflow.R import call_r\n\n\nclass TestFlow(FlowSpec):\n\n    @step\n    def join(self):\n        self.next(self.end, foreach='parameters')\n\n\nFLOW=TestFlow\nif __name__ == '__main__':\n    TestFlow()"
  expect_equal(actual, expected)
})

test_that("test join + r_function step", {
  skip_if_no_metaflow()
  join_fun <- function(self) {
    "join stuff"
  }
  metaflow("TestFlow") %>%
    step(
      step = "join",
      join = TRUE,
      r_function = join_fun,
      next_step = "end"
    )
  actual <- TestFlow$get_flow()
  expected <- "from metaflow import FlowSpec, step, Parameter, retry, environment, batch, catch, resources, schedule\nfrom metaflow.R import call_r\n\n\nclass TestFlow(FlowSpec):\n\n    @step\n    def join(self, inputs):\n        r_inputs = {node._current_step : node for node in inputs} if len(inputs[0].foreach_stack()) == 0 else list(inputs)\n        call_r('join_fun', (self, r_inputs))\n        self.next(self.end)\n\n\nFLOW=TestFlow\nif __name__ == '__main__':\n    TestFlow()"
  expect_equal(actual, expected)
})

test_that("new step returns valid python", {
  skip_if_no_metaflow()
  actual <- fmt_new_step("start")
  expected <- c("    @step", "\n", "    def start(self):\n")
  expect_equal(actual, expected)
  # join step
  actual <- fmt_new_step("join", join = TRUE)[3]
  expected <- "    def join(self, inputs):\n"
  expect_equal(actual, expected)
})

test_that("new step fails on invalid input", {
  skip_if_no_metaflow()
  expect_error(fmt_new_step(1))
  expect_error(fmt_new_step(c("branch_a", "branch_b")))
})

test_that("next_step returns valid python", {
  skip_if_no_metaflow()
  actual <- fmt_next_step("end")
  expected <- c("        self.next(self.end)", "\n\n")
  expect_equal(actual, expected)
  actual <- fmt_next_step(c("branch_a", "branch_b"))
  expected <- c("        self.next(self.branch_a, self.branch_b)", "\n\n")
  expect_equal(actual, expected)
  actual <- fmt_next_step("fit_gbrt_for_given_param", foreach = "parameter_grid")
  expected <- c(
    "        self.next(self.fit_gbrt_for_given_param, foreach='parameter_grid')",
    "\n\n"
  )
  expect_equal(actual, expected)
})

test_that("test function format", {
  skip_if_no_metaflow()
  actual <- fmt_r_function("test_fun")
  expected <- c("        call_r('test_fun', (self,))", "\n")
  expect_equal(actual, expected)
  actual <- fmt_r_function("test_fun", join = TRUE)
  expected <- c("        call_r('test_fun', (self, list(inputs)))", "\n")
})

test_that("we can define a step with an anonymous function", {
  skip_if_no_metaflow()
  flow <- metaflow("TestFlow") %>%
    step(
      step = "anonymous",
      r_function = function(step) step$x <- 3
    )
  expected_function_name <- "anonymous_function_616fb45ef54cbfa9"
  functions <- flow$get_functions()
  expect_true(expected_function_name %in% names(functions))
})
