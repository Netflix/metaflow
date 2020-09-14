pkg.env <- new.env()

pkg.env$configs <- list(
  default = list(
    metaflow_path = expression(reticulate::py_discover_config("metaflow")$required_module_path)
  ),
  batch = list(
    metaflow_path = expression(path.expand(paste0(getwd(), "/metaflow")))
  )
)

pkg.env$envname = "metaflow-r"


.onAttach <- function(libname, pkgname) {
  # activate Metaflow conda/virtualenv if they're available
  # need to call this before check_python_dependencies()
  env_activated <- activate_metaflow_env(pkg.env$envname)

  if (env_activated && check_python_dependencies()) {
    metaflow_attach()
  } else {
    print_metaflow_install_options()
  }
}

.onLoad <- function(libname, pkgname) {
  metaflow_python <- Sys.getenv("METAFLOW_PYTHON", unset = NA)
  if (!is.na(metaflow_python)) {
    Sys.setenv(RETICULATE_PYTHON = metaflow_python)
  }

  # activate Metaflow conda/virtualenv if they're available
  # need to call this before check_python_dependencies()
  env_activated <- activate_metaflow_env(pkg.env$envname)

  if (env_activated && check_python_dependencies()) {
    metaflow_load()
  }
}

print_metaflow_install_options <- function(){
  packageStartupMessage(
      "* Metaflow Python dependencies not found *\n",
      "  Available options:\n",
      "    - Call `install_metaflow()` to install into a new conda or virtualenv\n",
      "    - Set `METAFLOW_PYTHON` environment variable to the path of your python executable.\n",
      "      Note: Metaflow needs to be available in the environment specified by `METAFLOW_PYTHON`"
  )
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
