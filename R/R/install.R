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
                             version = NULL,
                             ...) {
  envname <- pkg.env$envname

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
  if (method == "conda"){
      conda <- tryCatch(reticulate::conda_binary(), 
                        error = function(e) NULL)
      have_conda <- !is.null(conda)
      if (!have_conda) {
        message("No conda was found in the system.")
        message("Would you like to download and install Miniconda?")
        message("Miniconda is an open source environment management system for Python.")
        message("See https://docs.conda.io/en/latest/miniconda.html for more details.")
        ans <- utils::menu(c("Yes", "No"), 
                    title = "Would you like to install Miniconda?")
        if (ans == 1) {
          reticulate::install_miniconda()
          conda <- tryCatch(reticulate::conda_binary("auto"), error = function(e) NULL)
        } else {
          stop("Conda environment installation failed (no conda binary found)\n", 
                call. = FALSE)
        } 
      }

      if (!envname %in% reticulate::conda_list()$name){
        reticulate::conda_create(envname)
      }
  } else if (method == "virtualenv" && !envname %in% reticulate::virtualenv_list()) {
    reticulate::virtualenv_create(envname)
  }

  reticulate::py_install(
    packages = packages,
    envname = envname,
    ...
  )

  # activate Metaflow environment
  pkg.env$activated <- activate_metaflow_env(pkg.env$envname)
  # load metaflow python library
  metaflow_load()

  invisible(NULL)
}
