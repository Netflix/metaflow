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
#' @param prompt boolean, whether or not to prompt user for confirmation before installation. Default is TRUE.
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
                             prompt = TRUE,
                             version = NULL,
                             ...) {
  envname <- pkg.env$envname

  env_set <- check_environment(envname)
  if (method == "conda" && env_set[["virtualenv"]]) {
    stop("An existing virtualenv <", envname, "> detected for Metaflow installation.\n", 
       "To continue, remove that environment by executing metaflow::remove_metaflow_env()",
       " and try installing Metaflow again.", call.=FALSE)
  }

  if (method == "virtualenv" && env_set[["conda"]]) {
    stop("An existing conda environment <", envname, "> detected for Metaflow installation.\n", 
       "To continue, remove that environment by executing metaflow::remove_metaflow_env()",
       " and try installing Metaflow again.", call.=FALSE)
  }

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
  if (method == "conda") {
    conda <- tryCatch(reticulate::conda_binary(),
      error = function(e) NULL
    )
    have_conda <- !is.null(conda)
    if (!have_conda) {
      message("No conda installation found.")
      message("Miniconda is an open source package manager and environment management system.")
      message("See https://docs.conda.io/en/latest/miniconda.html for more details.")
      if (interactive()) {
        ans <- ifelse(prompt, utils::menu(c("Yes", "No"),
          title = "Would you like to download and install Miniconda?"
        ), 1)
      } else {
        ans <- 1
      }
      if (ans == 1) {
        reticulate::install_miniconda()
        conda <- tryCatch(reticulate::conda_binary("auto"), error = function(e) NULL)
      } else {
        stop("Metaflow installation failed (no conda binary found).",
          call. = FALSE
        )
      }
    }

    if (!envname %in% reticulate::conda_list()$name) {
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

#' Remove Metaflow Python package.
#'
#' @param prompt `bool`, whether to ask for user prompt before removal. Default to TRUE.
#'
#' @examples
#' \dontrun{
#' # not run because it requires Python
#' remove_metaflow_env()
#' }
#' @export
remove_metaflow_env <- function(prompt = TRUE) {
  # validate stage, method arguments
  envname <- pkg.env$envname

  env_set <- check_environment(envname)

  if (env_set[["conda"]]) {
    message("Conda environment <", envname, "> will be deleted.\n")
    ans <- ifelse(prompt, utils::menu(c("No", "Yes"), title = "Proceed?"), 2)
    if (ans == 1) stop("Cancelled...", call. = FALSE)
    python <- reticulate::conda_remove(envname = envname)
    message("\nRemoval complete. Please restart the current R session.\n\n")
  }

  if (env_set[["virtualenv"]]) {
    message("Virtualenv environment <", envname, "> will be removed\n")
    ans <- ifelse(prompt, utils::menu(c("No", "Yes"), title = "Proceed?"), 2)
    if (ans == 1) stop("Cancelled...", call. = FALSE)
    python <- reticulate::virtualenv_remove(envname = envname, confirm = FALSE)
    message("\nRemoval complete. Please restart the current R session.\n\n")
  }

  if (!env_set[["conda"]] && !env_set[["virtualenv"]]) {
    stop("Nothing to remove.", call. = FALSE)
  }
}
