pkg.env <- new.env()

pkg.env$configs <- list(
  default = list(
    metaflow_path = expression(reticulate::py_discover_config("metaflow")$required_module_path)
  ),
  batch = list(
    metaflow_path = expression(path.expand(paste0(getwd(), "/metaflow")))
  )
)

.onAttach <- function(libname, pkgname) {
  if (check_python_dependencies()) {
    metaflow_attach()
  }
}

.onLoad <- function(libname, pkgname) {
  metaflow_python <- Sys.getenv("METAFLOW_PYTHON", unset = NA)
  if (!is.na(metaflow_python)) {
    Sys.setenv(RETICULATE_PYTHON = metaflow_python)
  }

  if (!ensure_metaflow("metaflow-r")) {
    packageStartupMessage(
      "* Metaflow Python dependencies not found *\n",
      "  Available options:\n",
      "    - Call `install_metaflow()` to install into a new conda or virtualenv\n",
      "    - Set `METAFLOW_PYTHON` environment variable to the path of your python executable.\n",
      "      Note: Metaflow needs to be available in the environment specified by `METAFLOW_PYTHON`"
    )
  } else {
    metaflow_load()
  }
}

metaflow_load <- function() {
  config_name <- Sys.getenv("R_CONFIG_ACTIVE", unset = "default")
  configs <- pkg.env$configs
  config <- list()
  for (key in names(configs[[config_name]])) {
    config[[key]] <- eval(configs[[config_name]][[key]])
  }
  if (config_name == "batch") {
    pkg.env$mf <- import_from_path("metaflow", path = config$metaflow_path)
  } else {
    pkg.env$mf <- import("metaflow", delay_load = TRUE)
  }
  invisible(NULL)
}
