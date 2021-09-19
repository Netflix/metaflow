#' Assign parameter to the flow
#'
#' @description
#' \code{parameter} assigns variables to the flow that are
#' automatically available in all the steps.
#'
#'
#' @param flow metaflow object
#' @param parameter name of the parameter
#' @param required logical (defaults to FALSE) denoting if
#' parameter is required as an argument to \code{run} the flow
#' @param help optional help text
#' @param default optional default value of the parameter
#' @param type optional type of the parameter
#' @param is_flag optional logical (defaults to FALSE) flag to denote is_flag
#' @param separator optional separator for string parameters.
#' Useful in defining an iterable as a delimited string inside a parameter
#' @section Usage:
#' \preformatted{
#' parameter("alpha", help = "learning rate", required = TRUE)
#' parameter("alpha", help = "learning rate", default = 0.05)
#' }
#' @export
parameter <- function(flow, parameter, required = FALSE, help = NULL,
                      separator = NULL, default = NULL, type = NULL,
                      is_flag = FALSE) {
  pad <- 17 + nchar(parameter)
  param <- NULL
  if (!is.null(default) && is.function(default)) {
    param <- paste0(
      space(4), "from metaflow.R import get_r_func", space(1, type = "v")
    )
  }
  param <- paste0(
    param,
    space(4), parameter, " = Parameter('", parameter, "',", space(1, type = "v"),
    space(pad)
  )
  if (required) {
    param <- fmt_parameter(param, parameter_arg = paste0("required = True,"), pad)
  }
  if (!is.null(help)) {
    param <- fmt_parameter(param, paste0("help = '", help, "',"), pad)
  }
  if (!is.null(separator)) {
    param <- fmt_parameter(param, paste0("separator = '", separator, "',"), pad)
  }
  if (!is.null(default)) {
    if (is.character(default)) {
      default <- paste0("'", default, "'")
    } else if (is.logical(default)) {
      default <- escape_bool(default)
    } else if (is.function(default)) {
      function_name <- as.character(substitute(default))
      fun <- list(default)
      names(fun) <- function_name
      flow$add_function(fun)
      default <- paste0("get_r_func('", function_name, "')", collapse = "")
    }
    param <- fmt_parameter(param, paste0("default = ", default, ","), pad)
  }
  if (!is.null(type)) {
    param <- fmt_parameter(param, paste0("type = ", type, ","), pad)
  }
  if (is_flag) {
    param <- fmt_parameter(param, "is_flag = True,", pad)
  }

  param <- paste0(param, collapse = "")
  param <- paste0(substr(param, 1, nchar(param) - (pad + 2)), ")\n")
  flow$add_parameter(paste0(param, sep = ""))
}

fmt_parameter <- function(parameter_string = NULL, parameter_arg, space) {
  if (is.null(parameter_string)) {
    fmt <- c(
      parameter_arg, space(1, type = "v"),
      space(space)
    )
  } else {
    fmt <- c(
      parameter_string, parameter_arg, space(1, type = "v"),
      space(space)
    )
  }
  fmt[!is.na(fmt)]
}
