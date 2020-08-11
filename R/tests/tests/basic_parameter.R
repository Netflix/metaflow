test <- new.env()
test$name <- "BasicParameterTest"
test$priority <- 1
test$parameters <- list(
  bool_param = list(default = "TRUE"),
  int_param = list(default = "123"),
  str_param = list(default = '"foobar"')
)

test$all <- decorated_function(
  function(self) {
    source("utils.R")
    stopifnot(self$bool_param)
    stopifnot(self$int_param == 123)
    stopifnot(self$str_param == "foobar")
    # parameters should be immutable
    assert_exception(
      expression(self$int_param <- 5),
      "AttributeError"
    )
  },
  type = "step", prio = 0, qual = c("all")
)

test$check_artifact <- decorated_function(
  function(checker, test_flow) {
    test_run <- test_flow$run(test_flow$latest_run)
    for (step_name in test_run$steps) {
      stopifnot(fetch_artifact(checker,
        step = step_name,
        var = "bool_param"
      ) == TRUE)

      stopifnot(fetch_artifact(checker,
        step = step_name,
        var = "int_param"
      ) == 123)

      stopifnot(fetch_artifact(checker,
        step = step_name,
        var = "str_param"
      ) == "foobar")
    }
  },
  type = "check"
)
