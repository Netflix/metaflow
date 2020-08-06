test <- new.env()
test$name <- "MergeArtifactsPropagationTest"
test$priority <- 1

test$start <- decorated_function(
  function(self) {
    self$non_modified_passdown <- "a"
  },
  type = "step", prio = 0, qual = c("start"), required = TRUE
)

test$modify_things <- decorated_function(
  function(self) {
    # Set different names to different things
    val <- self$index + 1
    self[[sprintf("val%d", val)]] <- val
  },
  type = "step", prio = 0, qual = c("foreach-inner-small"), required = TRUE
)


test$merge_things <- decorated_function(
  function(self, inputs) {
    merge_artifacts(self, inputs)

    stopifnot(self$non_modified_passdown == "a")
    for (i in 1:length(inputs)) {
      stopifnot(self[[sprintf("val%d", i)]] == i)
    }
  },
  type = "step", prio = 0, qual = c("join"), required = TRUE
)

test$all <- decorated_function(
  function(self) {
    stopifnot(self$non_modified_passdown == "a")
  },
  type = "step", prio = 1, qual = c("all"), required = TRUE
)
