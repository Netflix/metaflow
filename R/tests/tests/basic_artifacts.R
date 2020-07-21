test <- new.env()
test$name <- "BasicArtifactsTest"
test$priority <- 0

test$step_start <- decorated_function(
  function(self) {
    self$data <- "abc"
  },
  type = "step", prio = 0, qual = c("start"), required = TRUE
)

test$step_join <- decorated_function(
  function(self, inputs) {
    inputset <- gather_inputs(inputs, "data")
    for (item in inputset) {
      print(item)
      stopifnot(item == "abc")
    }
    self$data <- inputset[[1]]
  },
  type = "step", prio = 1, qual = c("join"), required = TRUE
)


test$step_all <- decorated_function(
  function(self) {
  },
  type = "step", prio = 2, qual = c("all")
)


test$check_artifact <- decorated_function(
  function(checker, test_flow) {
    test_run <- test_flow$run(test_flow$latest_run)
    for (step_name in test_run$steps) {
      stopifnot(fetch_artifact(checker,
        step = step_name,
        var = "data"
      ) == "abc")
    }
  },
  type = "check"
)
