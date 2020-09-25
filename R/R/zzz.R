pkg.env <- new.env()

pkg.env$configs <- list(
  default = list(
    metaflow_path = expression(reticulate::py_discover_config("metaflow")$required_module_path)
  ),
  batch = list(
    metaflow_path = expression(path.expand(paste0(getwd(), "/metaflow")))
  )
)

pkg.env$envname = "r-metaflow"

pkg.env$activated = FALSE

.onLoad <- function(libname, pkgname) {
  # activate Metaflow conda/virtualenv if they're available
  # need to call this before check_python_dependencies()
  pkg.env$activated <- activate_metaflow_env(pkg.env$envname)

  if (pkg.env$activated && check_python_dependencies()) {
    metaflow_load()
    print_metaflow_versions()
  } else {
    print_metaflow_install_options()
  }
}

print_metaflow_install_options <- function(){
  packageStartupMessage(
      "* Metaflow Python dependencies not found *\n",
      "  Available options:\n",
      "    - Call `install_metaflow()` to install into a new conda or virtualenv\n",
      "    - Set `METAFLOW_PYTHON` environment variable to the path of your python executable\n",
      "      which has metaflow, numpy, and pandas available as dependencies."
  )
}

activate_metaflow_env <- function(envname) {
  metaflow_python <- Sys.getenv("METAFLOW_PYTHON", unset = NA)
  if (!is.na(metaflow_python)) {
    Sys.setenv(RETICULATE_PYTHON = metaflow_python)
  }

  if (is.na(metaflow_python)) {
    env_set <- check_environment(envname)
    if (env_set[["conda"]] || all(env_set[["conda"]], env_set[["virtualenv"]])) {
      reticulate::use_condaenv(envname, required=TRUE)
      return(TRUE)
    } else if (env_set[["virtualenv"]]) {
      reticulate::use_virtualenv(envname, required=TRUE)
      return(TRUE)
    } else{
      return(FALSE)
    }
  } else {
    reticulate::use_python(metaflow_python, required=TRUE)
  }
  return(TRUE)
}

check_python_dependencies <- function() {
  all(
    reticulate::py_module_available("numpy"),
    reticulate::py_module_available("pandas"),
    reticulate::py_module_available("metaflow")
  )
}

check_environment <- function(envname) {
  conda_try <- try(reticulate::conda_binary(), silent = TRUE)
  if (class(conda_try) != "try-error") {
    conda_check <- envname %in% reticulate::conda_list()$name
  } else {
    conda_check <- FALSE
  }
  virtualenv_check <- envname %in% reticulate::virtualenv_list()
  list(conda = conda_check, virtualenv = virtualenv_check)
}

print_metaflow_versions <- function() {
  packageStartupMessage(sprintf("Metaflow (R) %s loaded", r_version()))
  packageStartupMessage(sprintf("Metaflow (Python) %s loaded", py_version()))
  invisible(NULL)
}

metaflow_load <- function() {
  config_name <- Sys.getenv("R_CONFIG_ACTIVE", unset = "default")
  configs <- pkg.env$configs
  config <- list()
  for (key in names(configs[[config_name]])) {
    config[[key]] <- eval(configs[[config_name]][[key]])
  }
  if (config_name == "batch") {
    pkg.env$mf <- reticulate::import_from_path("metaflow", path = config$metaflow_path)
  } else {
    pkg.env$mf <- reticulate::import("metaflow", delay_load = TRUE)
  }
  invisible(NULL)
}
