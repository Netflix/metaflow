#' Switch to a namespace specified by the given tag.
#'
#' @param ns namespace
#'
#' @details NULL maps to global namespace.
#'
#' @export
set_namespace <- function(ns = NULL) {
  mf$namespace(ns)
}

#' Return the current namespace (tag).
#'
#' @export
get_namespace <- function() {
  mf$get_namespace()
}

#' Set the default namespace.
#'
#' @export
set_default_namespace <- function() {
  mf$default_namespace()
}
