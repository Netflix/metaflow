#' Serialize an R object to the Metaflow data format, and deserialize it back
#'
#' @description
#' When an object is saved to the `self` object in a step, it is serialised with
#' `mf_serialize` and converted to a Python object via `reticulate`. When it is
#' later retrieved from the `self` object, it is converted back into an R object
#' and deserialized with `mf_deserialize`.
#'
#' @section History:
#' In versions 2.3.0 and prior, `mf_serialize` left some objects untouched.
#' These objects would be left unserialised, and would therefore be subject to
#' conversion by reticulate. This occurred whenever an object was saved to the
#' `self` object and then loaded again. For example,
#' `self$x <- integer(); self$x` would have returned `list()`. The current
#' approach serializes everything into raw vectors, which are not touched by
#' reticulate. The `mf_serialize` function has not changed, and should work for
#' both methods.
#'
#' @param object object to serialize or deserialize
#' @return For `mf_serialize`, a raw vector. For `mf_deserialize`, an R object.
#'
#' @keywords internal
mf_serialize <- function(object) {
  serialize(object, NULL)
}

#' @rdname mf_serialize
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
