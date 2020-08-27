#' Install Metaflow Python package
#'
#' This function wraps installation functions from [reticulate][reticulate::reticulate] to install the Python packages
#' **metaflow** and it's Python dependencies.
#'
#' This package uses the [reticulate][reticulate::reticulate] package
#' to make an interface with the [Metaflow](https://metaflow.org/)
#' Python package. 
#'
#' @param method `character`, indicates to use `"conda"` or `"virtualenv"`
#' @param envname `character`, name of environment into which to install
#' @param version `character`, version of Metaflow to install. For general use of this package,
#'   this is set automatically, so you should not need to specify this.
#' @param ... other arguments sent to [reticulate::conda_install()] or
#'    [reticulate::virtualenv_install()]
#'
#' @seealso
#' [reticulate: Using reticulate in an R Package](https://rstudio.github.io/reticulate/articles/package.html),
#' [reticulate: Installing Python Packages](https://rstudio.github.io/reticulate/articles/python_packages.html)
#' @examples
#' \dontrun{
#' # not run because it requires Python
#' install_metaflow()
#' }
#' @export
install_metaflow <- function(method = c("conda", "virtualenv"),
                             envname = "metaflow",
                             version = NULL,
                             restart_session = TRUE,
                             ...) {
  
  # validate stage, method arguments
  method <- match.arg(method)
  
  # conda and pip use different syntax for indicating versions
  if (identical(method, "conda")) {
    version_sep <- "="
  } else {
    version_sep <- "=="
  }
  
  if (is.null(version)) {
    metaflow_pkg_version <- "metaflow"
  } else {
    metaflow_pkg_version <- paste("metaflow", version, sep = version_sep)
  }
  
  packages <- c(metaflow_pkg_version, "numpy", "pandas")
  
  # create environment if not present
  if (method == "conda" && !envname %in% conda_list()$name) {
      conda_create(envname)
  }
  
  if (method == "virtualenv" && !envname %in% virtualenv_list()) {
    virtualenv_create(envname)
  }
  
  py_install(
    packages = packages,
    envname = envname,
    ...
  )
  
  cat("\nInstallation complete.\n\n")
  
  if (restart_session && hasFun("restartSession")) {
    restartSession()
  }
  
  invisible(NULL)
}

ensure_metaflow <- function() {
  metaflow_python <- Sys.getenv("METAFLOW_PYTHON", unset = NA)
  if (is.na(metaflow_python)) {
    if ("metaflow" %in% conda_list()$name) {
      use_condaenv("metaflow", required = TRUE)
    } else if ("metaflow" %in% virtualenv_list()) {
      use_virtualenv("metaflow", required = TRUE)
    } 
  } 
  py_module_available("metaflow")
}
