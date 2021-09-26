test <- new.env()
test$name <- "NestedForeachTest"
test$priority <- 1

test$inner <- decorated_function(
  function(self) {
    stack <- self$foreach_stack()
    x <- stack[[1]]
    y <- stack[[2]]
    z <- stack[[3]]

    # assert that lengths are correct
    stopifnot(length(self$x) == x[[2]])
    stopifnot(length(self$y) == y[[2]])
    stopifnot(length(self$z) == z[[2]])

    # assert that variables are correct given their indices
    stopifnot(mf_deserialize(x[[3]]) == self$x[[x[[1]] + 1]])
    stopifnot(mf_deserialize(y[[3]]) == self$y[[y[[1]] + 1]])
    stopifnot(mf_deserialize(z[[3]]) == self$z[[z[[1]] + 1]])
  },
  type = "step", prio = 0, qual = c("foreach-nested-inner"), required = TRUE
)

test$all <- decorated_function(
  function(self) {
  },
  type = "step", prio = 1, qual = c("all")
)
