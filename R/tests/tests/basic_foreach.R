test <- new.env()
test$name <- "BasicForeachTest"
test$priority <- 0

test$split <- decorated_function(
  function(self) {
    self$my_index <- "None"
    self$arr <- 1:10
  },
  type = "step", prio = 0, qual = c("foreach-split"), required = TRUE
)

test$inner <- decorated_function(
  function(self) {
    # index must stay constant over multiple steps inside foreach
    if (self$my_index == "None") {
      self$my_index <- self$index + 1
    }
    stopifnot(self$my_index == self$index + 1)
    stopifnot(self$my_index == self$arr[self$my_index])
    self$my_input <- self$input
  },
  type = "step", prio = 0, qual = c("foreach-inner"), required = TRUE
)

test$join <- decorated_function(
  function(self, inputs) {
    got <- sort(unlist(gather_inputs(inputs, "my_input")))
    stopifnot(all(got == 1:10))
  },
  type = "step", prio = 0, qual = c("foreach-join"), required = TRUE
)

test$all <- decorated_function(
  function(self) {
  },
  type = "step", prio = 1, qual = c("all")
)
