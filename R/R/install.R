#' Install Metaflow Python package
#'
#' This function wraps installation functions from [reticulate][reticulate::reticulate] to install the Python packages
#' **metaflow** and it's Python dependencies.
#'
#' This package uses the [reticulate][reticulate::reticulate] package
#' to make an interface with the [Metaflow](https://metaflow.org/)
#' Python package.
#'
#' @param method `character`, indicates to use `"conda"` or `"virtualenv"`.
#' @param envname `character`, name of environment into which to install.
#' @param version `character`, version of Metaflow to install. The default version
#' is the latest available on PyPi.
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
                             envname = "metaflow-r",
                             version = NULL,
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
  } else if (method == "virtualenv" && !envname %in% virtualenv_list()) {
    virtualenv_create(envname)
  }

  py_install(
    packages = packages,
    envname = envname,
    ...
  )

  invisible(NULL)
}

ensure_metaflow <- function(envname) {
  metaflow_python <- Sys.getenv("METAFLOW_PYTHON", unset = NA)
  if (is.na(metaflow_python)) {
    env_set <- check_environment(envname)
    if (env_set[["conda"]] || all(env_set)) {
      use_condaenv(envname, required = TRUE)
    } else if (env_set[["virtualenv"]]) {
      use_virtualenv(envname, required = TRUE)
    }
  }
  check_python_dependencies()
}

check_python_dependencies <- function() {
  all(
    py_module_available("numpy"),
    py_module_available("pandas"),
    py_module_available("metaflow")
  )
}

check_environment <- function(envname) {
  conda_check <- envname %in% conda_list()$name
  virtualenv_check <- envname %in% virtualenv_list()
  c(conda = conda_check, virtualenv = virtualenv_check)
}
