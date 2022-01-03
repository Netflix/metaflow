#' Assign a step to the flow
#' @include utils.R
#'
#' @param flow metaflow object
#' @param ... decorators
#' @param step character name for the step. Step names must be valid Python
#'   identifiers; they can contain letters, numbers, and underscores, although
#'   they cannot begin with a number.
#' @param r_function R function to execute as part of this step
#' @param foreach optional input variable to iterate over as input to next step
#' @param join optional logical (defaults to FALSE) denoting whether the step is
#' a join step
#' @param next_step list of step names to execute after this step is executed
#' @section Usage:
#' \preformatted{
#' step(flow, step = "start", r_function = start, next_step = "b")
#' step(flow, decorator("batch"), step = "start",
#'    r_function = start, next_step = "a", foreach = "parameters")
#' step(flow, step = "start", r_function = start, next_step = c("a", "b"))
#' step(flow, step = "c", r_function = c, next_step = "d", join = TRUE)
#' }
#' @export
step <- function(flow, ..., step, r_function = NULL, foreach = NULL, join = FALSE, next_step = NULL) {
  if (!is_valid_python_identifier(step)) {
    stop(step, " is not a valid step name. Step names must be valid Python
identifiers; they can contain letters, numbers, and underscores, although they
cannot begin with a number.")
  }
  decorators <- add_decorators(list(...))
  if (!is.null(decorators)) {
    decorators <- paste0(space(4), decorators)
  }
  .step <- decorators
  if (join) {
    .step <- c(.step, fmt_new_step(step, join = TRUE))
  } else {
    .step <- c(.step, fmt_new_step(step))
  }
  if (!is.null(r_function)) {
    function_name <- as.character(substitute(r_function))
    # If r_function is anonymous then function_name will be a vector of its
    # components. In this case we give the function a pseudonym prefixed by the
    # step name and suffixed with a hash of the function.
    if (length(function_name) > 1) {
      function_hash <- digest::digest(deparse(r_function), algo = "sha256")
      trunc_function_hash <- substr(function_hash, 1, 16)
      function_name <- paste(step, "function", trunc_function_hash, sep = "_")
    }
    body(r_function) <- wrap_function(r_function)
    if (join) {
      .step <- c(.step, fmt_r_function(function_name, join = TRUE))
    } else {
      .step <- c(.step, fmt_r_function(function_name))
    }
    add_R_object_to_flow(flow, r_function, function_name)
  }

  if (!is.null(next_step)) {
    if (!is.null(foreach)) {
      .step <- c(.step, fmt_next_step(next_step, foreach))
    } else {
      .step <- c(.step, fmt_next_step(next_step))
    }
  } else {
    if (!is.null(r_function)) {
    } else {
      .step <- c(.step, c(space(8), "pass", space(2, type = "v")))
    }
  }
  flow$add_step(paste0(.step, collapse = ""))
}

step_decorator <- paste0(space(4), "@step")

step_def <- paste0(space(4), "def")

add_R_object_to_flow <- function(flow, obj, name) {
  fun <- list(obj)
  names(fun) <- name
  flow$add_function(fun)
}

# wrap user's function to fix zero as the return value for user's r_functions to avoid reticulate failures.
# Note: R functions by default return execution results of the last line if there's no explicit return(..).
# With our call_r hooks in python, reticulate will try to convert each r_function return value into python.
# A print statement at the last line would sometimes unintentionally return an S4 object to python,
# which leads to reticulate error, for example the overloaded print function in R library glmnet.
wrap_function <- function(func) {
  # we only need body of the wrapped_func so no need to handle the arguments
  wrapped_func <- function() {
    original_func <- function() {
    }
    original_func()
    return(0)
  }

  # insert function body of original f into the
  # original_func sub function inside masked_func
  if (length(body(func)) > 1) {
    for (i in 2:length(body(func))) {
      body(wrapped_func)[[2]][[3]][[3]][[i]] <- body(func)[[i]]
    }
  }
  return(body(wrapped_func))
}

fmt_new_step <- function(x, join = NULL) {
  stopifnot(
    length(x) == 1, is.character(x)
  )
  fmt <- paste0(step_def, " ", x, "(self):", space(1, type = "v"))
  if (!is.null(join)) {
    fmt <- gsub("):", ", inputs):", fmt)
  }
  c(step_decorator, space(1, type = "v"), fmt)
}

fmt_next_step <- function(x, foreach = NULL) {
  stopifnot(is.character(x))
  fmt <- paste0(space(8), "self.next(self.", x, ")")
  if (length(x) > 1) {
    steps <- paste0("self.", x, collapse = ", ")
    fmt <- paste0(space(8), "self.next(", steps, ")")
  } else if (!is.null(foreach)) {
    stopifnot(is.character(foreach))
    foreach_string <- paste0(", foreach=", escape_quote(foreach), ")")
    fmt <- gsub(")", foreach_string, fmt)
  }
  c(fmt, space(2, type = "v"))
}

fmt_r_function <- function(x, join = NULL) {
  fmt <- paste0(space(8), paste0("call_r('", x, "', (self,))", collapse = ""))
  if (!is.null(join)) {
    fmt_inputs <- paste0(space(8), "r_inputs = {node._current_step : node for node in inputs} if len(inputs[0].foreach_stack()) == 0 else list(inputs)", collapse = "")
    fmt <- gsub(",))", ", r_inputs))", fmt)
    line <- c(fmt_inputs, space(1, type = "v"), fmt, space(1, type = "v"))
  } else {
    line <- c(fmt, space(1, type = "v"))
  }
  line
}
