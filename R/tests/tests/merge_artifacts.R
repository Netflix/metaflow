test <- new.env()
test$name <- "MergeArtifactsTest"
test$priority <- 1

test$start <- decorated_function(
  function(self) {
    self$non_modified_passdown <- "a"
    self$modified_to_same_value <- "b"
    self$manual_merge_required <- "c"
    self$ignore_me <- "d"
  },
  type = "step", prio = 0, qual = c("start"), required = TRUE
)

test$modify_things <- decorated_function(
  function(self) {
    task_id <- current("task_id")
    self$manual_merge_required <- task_id
    self$ignore_me <- task_id
    self$modified_to_same_value <- "e"
  },
  type = "step", prio = 2, qual = c("linear"), required = TRUE
)


test$merge_things <- decorated_function(
  function(self, inputs) {
    source("utils.R")

    # test to see if we raise an exception when merging a conflicted artifact
    assert_exception(
      expression(merge_artifacts(self, inputs)),
      "MergeArtifactsException"
    )

    # Test to make sure nothing is set if failed merge_artifacts
    assert_exception(
      expression(print(self$non_modified_passdown)),
      "has no attribute"
    )
    assert_exception(
      expression(print(self$manual_merge_required)),
      "has no attribute"
    )


    # Test to make sure nothing is set if failed merge_artifacts
    assert_exception(
      expression(print(self$non_modified_passdown)),
      "has no attribute"
    )
    assert_exception(
      expression(print(self$manual_merge_required)),
      "has no attribute"
    )

    # Test actual merge (ignores set values and excluded names, merges common and non modified)
    task_id <- current("task_id")
    self$manual_merge_required <- task_id
    merge_artifacts(self, inputs, exclude = list("ignore_me"))

    # Ensure that everything we expect is passed down
    stopifnot(self$non_modified_passdown == "a")
    stopifnot(self$manual_merge_required == task_id)
    stopifnot(self$modified_to_same_value == "e")

    assert_exception(
      expression(print(self$ignore_me)),
      "has no attribute"
    )
  },
  type = "step", prio = 0, qual = c("join"), required = TRUE
)


test$end <- decorated_function(
  function(self) {
    # Check that all values made it through
    stopifnot(self$non_modified_passdown == "a")
    stopifnot(self$modified_to_same_value == "e")
    print(self$manual_merge_required)
  },
  type = "step", prio = 0, qual = c("end"), required = TRUE
)

test$all <- decorated_function(
  function(self) {
    stopifnot(self$non_modified_passdown == "a")
  },
  type = "step", prio = 3, qual = c("all"), required = TRUE
)
