test_that("serialize functions work properly", {
  skip_if_no_metaflow()
  py_obj <- mf_serialize(mtcars)
  returned_obj <- mf_deserialize(py_obj)
  expect_equal(mtcars, returned_obj)
})

test_that("R objects are invertible", {
  # We define an R object to be invertible under the serialize/deserialize
  # functions if it is unchanged when it is serialised, converted to a Python
  # object through reticulate, converted back to an R object, and deserialized.
  # This is the process that an object goes through when it saved to the "self"
  # object of a step, and so informally an invertible object is uncorrupted
  # by Metaflow.

  expect_invertible <- function(object) {
    eval(bquote({
      inverted <- object %>%
        mf_serialize() %>%
        reticulate::r_to_py() %>%
        reticulate::py_to_r() %>%
        mf_deserialize()

      # Force the use of waldo::compare in testthat by forcing the edition of
      # testthat to be 3, so that we can ignore environments for functions and
      # formulae
      testthat::local_edition(3)
      expect_identical(
        object,
        inverted,
        ignore_function_env = TRUE,
        ignore_formula_env = TRUE
      )
    }))
  }

  expect_invertible(0)
  expect_invertible(5)
  expect_invertible(5L)
  expect_invertible(5.5)
  expect_invertible(1+4i)
  expect_invertible(Inf)
  expect_invertible(-Inf)
  expect_invertible(NaN)
  expect_invertible("character")
  expect_invertible(mtcars)
  expect_invertible(list())
  expect_invertible(TRUE)
  expect_invertible(FALSE)
  expect_invertible(NULL)
  expect_invertible(NA) # logical type
  expect_invertible(NA_character_)
  expect_invertible(NA_integer_)
  expect_invertible(NA_real_)
  expect_invertible(NA_complex_)
  expect_invertible(as.Date("2021-07-13"))
  expect_invertible(as.POSIXct("2021-07-13 11:28:30", tz = "UTC"))
  expect_invertible(logical(0))
  expect_invertible(integer(0))
  expect_invertible(numeric(0))
  expect_invertible(complex(0))
  expect_invertible(character(0))
  expect_invertible(c(1, 2, 3))
  expect_invertible(list(1, 2, 3))
  expect_invertible(list("red panda", 5))
  expect_invertible(list(animal = "red panda", number = 5))
  expect_invertible(as.formula(y ~ x))
  expect_invertible(function(x) x + 1)
  expect_invertible(as.raw(c(1:10)))
  expect_invertible(factor(c("a", "b", "c")))
  expect_invertible(globalenv()) # an arbitrary environment won't work though
  expect_invertible(emptyenv())
  expect_invertible(matrix(c(1,2,3,4), nrow = 2, ncol = 2))
  expect_invertible(structure(list(1, 2, 3), class = "custom")) # custom class
})

test_that("we can retrieve objects in a flow that are tricky to serialize", {
  # This is an end-to-end tests of (de)serialization to ensure that objects are
  # not corrupted by the serialise/reticulate process. We pick a few objects
  # that have traditionally caused problems. For example, `integer(0)` would
  # formerly be converted to `list()`. We save them to `self` in a step, run
  # the flow, and check that when retrieved they are unchanged.

  skip_if_no_metaflow()

  start <- function(self) {
    self$empty_integer_vector <- integer(0)
    self$named_list <- list(animal = "red panda", number = 5)
  }
  end <- function(self) {
    self$empty_integer_vector_type <- typeof(self$empty_integer_vector)
  }

  metaflow("TestFlow") %>%
    step(
      step = "start",
      r_function = start,
      next_step = "end"
    ) %>%
    step(
      step = "end",
      r_function = end) %>%
    run()

  TestFlowClient <- flow_client$new("TestFlow")
  TestFlowRun <- run_client$new(TestFlowClient, TestFlowClient$latest_run)

  expect_identical(
    TestFlowRun$artifact("empty_integer_vector"),
    integer(0)
  )
  expect_identical(
    TestFlowRun$artifact("empty_integer_vector_type"),
    "integer"
  )
  expect_identical(
    TestFlowRun$artifact("named_list"),
    list(animal = "red panda", number = 5)
  )
})
