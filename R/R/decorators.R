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

#' Format a list of decorators as a character vector
#'
#' @section Python decorators: Metaflow decorators are so called because they
#'   translate directly to Python decorators that are applied to a step. So, for
#'   example, `decorator("batch", cpu = 1)` in R becomes `@batch(cpu = 1)` in
#'   Python. A new line is appended as well, as Python decorators are placed
#'   above the function they take as an input.
#'
#' @param decorators List of decorators, as created by the
#'   \code{\link{decorators}} function.
#'
#' @return character vector
#' @keywords internal
#' 
#' @examples \dontrun{
#' add_decorators(list(decorator("batch", cpu = 4), decorator("retry")))
#' #> c("@batch(cpu=4)", "\n", "@retry", "\n")
#' }
add_decorators <- function(decorators) {
  decorator_idx <- unlist(lapply(decorators, is.decorator))
  unlist(decorators[decorator_idx])
}

#' Format an R decorator as a Python decorator
#' 
#' @inheritSection add_decorators Python decorators
#'
#' @param x Decorator name.
#' @param ... Additional named arguments provided to the decorator
#'
#' @return character vector of length two, in which the first element is the 
#' translated decorator and the second element is a new line character.
#' @keywords internal
#'
#' @examples \dontrun{
#' fmt_decorator("resources", cpu = 1, memory = 1000)
#' # returns c("@resources(cpu=1, memory=1000)", "\n")
#' }
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

#' Format the arguments of a decorator as inputs to a Python function
#'
#' @inheritSection add_decorators Python decorators
#'
#' @param args Named list of arguments, as would be provided to the `...` of a
#'   function.
#'
#' @return atomic character of arguments, separated by a comma
#' @keywords internal
#'
#' @examples \dontrun{
#' decorator_arguments(list(cpu = 1, memory = 1000))
#' #> "cpu=1, memory=1000"
#' }
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
