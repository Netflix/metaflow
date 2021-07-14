#' Switch Metadata provider
#' @description  This call has a global effect.
#' Selecting the local metadata will, for example, not allow access to information
#' stored in remote metadata providers
#'
#' @return a string of the description of the metadata selected
#'
#' @param ms string. Can be a path (selects local metadata), a URL starting with http (selects
#' the service metadata) or an explicit specification {metadata_type}@{info}; as an
#' example, you can specify local@{path} or service@{url}.
#' @export
set_metadata <- function(ms = NULL) {
  pkg.env$mf$metadata(ms)
}

#' Returns the current Metadata provider.
#' @description This call returns the current Metadata being used to return information
#' about Metaflow objects. If this is not set explicitly using metadata(), the default value is
#' determined through environment variables.
#'
#' @return String type. Information about the Metadata provider currently selected.
#' This information typically returns provider specific information (like URL for remote
#' providers or local paths for local providers.
#' @export
get_metadata <- function() {
  pkg.env$mf$get_metadata()
}

#' Resets the Metadata provider to the default value.
#' @description The default value of the Metadata provider is determined through a
#' combination of environment variables.
#' @return String type. The result of get_metadata() after resetting the provider.
#' @export
reset_default_metadata <- function() {
  pkg.env$mf$default_metadata()
}
