#' @description  R binding for Metaflow. Metaflow is a human-friendly Python/R library
#' that helps scientists and engineers build and manage real-life data science projects.
#' Metaflow was originally developed at Netflix to boost productivity of data scientists
#' who work on a wide variety of projects from classical statistics to state-of-the-art deep learning.
#' @aliases metaflow-r
"_PACKAGE"

# directly setting global var would cause a NOTE from R CMD check
set_global_variable <- function(key, val, pos = 1) {
  assign(key, val, envir = as.environment(pos))
}

#' Instantiate a flow
#'
#' @param cls flow class name
#' @param ... flow decorators
#' @return flow object
#' @section Usage:
#' \preformatted{
#' metaflow("HelloFlow")
#' }
#' @export
metaflow <- function(cls, ...) {
  set_global_variable(cls, Flow$new(cls, list(...)))
  get(cls, pos = 1)
}
