#' Metaflow Decorator.
#'
#' Decorates the \code{step} with the parameters present in its arguments.
#'
#' For this method to work properly, the arguments \code{...} should be named, and decorator
#' type should be the first argument.
#'
#' @param x Type of decorator (e.g, resources, catch, retry, timeout, batch ...)
#' @param ... Named arguments for the decorator (e.g, cpu=1, memory=1000 ...). Note that memory unit is in MB.
#' @return A decorator object
#' @section Usage:
#' \preformatted{
#' decorator("catch", print_exception=FALSE)
#' decorator("resources", cpu=2, memory=10000)
#' }
#' @export
decorator <- function(x, ...) {
  fmt_decorator(x, ...) %>%
    new_decorator()
}

is.decorator <- function(x) inherits(x, "decorator")

new_decorator <- function(x) {
  structure(
    class = "decorator",
    x
  )
}

add_decorators <- function(decorators) {
  decorator_idx <- unlist(lapply(decorators, is.decorator))
  unlist(decorators[decorator_idx])
}

fmt_decorator <- function(x, ...) {
  args <- decorator_arguments(list(...))
  decorator_string <- paste0("@", x)
  if (is.null(args)) {
    decorator_string
  } else {
    decorator_string <- paste0(decorator_string, "(", args, ")")
  }
  c(decorator_string, "\n")
}

decorator_arguments <- function(args) {
  argument_names <- names(args)
  if (any(duplicated(argument_names))) {
    stop("duplicate decorator arguments")
  }
  if (!is.null(argument_names)) {
    unlist(lapply(seq_along(args), function(x) {
      if (x != length(args)) {
        paste0(names(args[x]), "=", wrap_argument(args[x]), ",")
      } else {
        paste0(names(args[x]), "=", (wrap_argument(args[x])))
      }
    })) %>%
      paste(collapse = " ")
  }
}
