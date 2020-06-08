.onAttach <- function(libname, pkgname){
  metaflow_attach()
} 

.onLoad <- function(libname, pkgname) {
  options(scipen = 999)
  reticulate::use_python(python_3(), required = TRUE)

  config_name <- Sys.getenv("R_CONFIG_ACTIVE", unset = "default")
  source(system.file("config.R", package = "metaflow"))
  config <- list() 
  for (key in names(configs[[config_name]])){
    config[[key]] <- eval(configs[[config_name]][[key]])
  }

  if (config_name == "batch") {
    mf <<- reticulate::import_from_path("metaflow", path = config$metaflow_path)
  } else {
    mf <<- reticulate::import("metaflow", delay_load = TRUE)
  }
  invisible()
}


python_3 <- function() {
  system("which python3", intern = TRUE)
}

py_version <- function() {
  reticulate::use_python(python_3(), required = TRUE)
  mf <- reticulate::import('metaflow', delay_load = TRUE)
  version <- mf$metaflow_version$get_version()
  c(python_version = substr(version, 1, 5))
}

metaflow_version <- function(x) {
  # utils library usually comes with the standard installation of R
  version <- as.character(unclass(utils::packageVersion(x))[[1]])
  if (length(version) > 3) {
    version[4:length(version)] <- as.character(version[4:length(version)])
  }
  paste0(version, collapse = ".")
}

metaflow_attach <- function() {
  R_mf_version <- metaflow_version("metaflow")
  py_mf_version <- py_version()
  packageStartupMessage(sprintf("metaflow (R) version %s", R_mf_version))
  packageStartupMessage(sprintf("metaflow (python) version %s", py_mf_version))
  invisible()
}