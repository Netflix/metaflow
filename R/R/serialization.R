#' Helper utility to serialize R object to metaflow
#' data format
#'
#' @param object object to serialize
#' @return metaflow data format object
mf_serialize <- function(object) {
  serialize(object, NULL)
}

#' Helper utility to deserialize objects from metaflow
#' data format to R object
#'
#' @param object object to deserialize
#' @return R object
mf_deserialize <- function(object) {
  r_obj <- object

  if (is.raw(object)) {
    # for bytearray try to unserialize
    tryCatch(
      {
        r_obj <- object %>% unserialize()
      },
      error = function(e) {
        r_obj <- object
      }
    )
  }

  return(r_obj)
}

#' Overload getter for self object
#'
#' @param self the metaflow self object for each step function
#' @param name attribute name
#'
#' @section Usage:
#' \preformatted{
#'  print(self$var)
#' }
#' @export
"$.metaflow.flowspec.FlowSpec" <- function(self, name) {
  value <- NextMethod(name)
  mf_deserialize(value)
}

#' Overload setter for self object
#'
#' @param self the metaflow self object for each step function
#' @param name attribute name
#' @param value value to assign to the attribute
#'
#' @section Usage:
#' \preformatted{
#'  self$var <- "hello"
#' }
#' @export
"$<-.metaflow.flowspec.FlowSpec" <- function(self, name, value) {
  value <- mf_serialize(value)
  NextMethod(name, value)
}

#' Overload getter for self object
#'
#' @param self the metaflow self object for each step function
#' @param name attribute name
#'
#' @section Usage:
#' \preformatted{
#'  print(self[["var"]])
#' }
#' @export
"[[.metaflow.flowspec.FlowSpec" <- function(self, name) {
  value <- NextMethod(name)
  mf_deserialize(value)
}

#' Overload setter for self object
#'
#' @param self the metaflow self object for each step function
#' @param name attribute name
#' @param value value to assign to the attribute
#'
#' @section Usage:
#' \preformatted{
#'  self[["var"]] <- "hello"
#' }
#' @export
"[[<-.metaflow.flowspec.FlowSpec" <- function(self, name, value) {
  value <- mf_serialize(value)
  NextMethod(name, value)
}
