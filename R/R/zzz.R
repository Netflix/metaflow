.onAttach <- function(libname, pkgname) {
  tryCatch(
    {
      metaflow_attach()
    },
    error = function(e) {
      packageStartupMessage("  * Call 'metaflow::install()' to finish installation * ")
    }
  )
  invisible(NULL)
}

.onLoad <- function(libname, pkgname) {
  tryCatch(
    {
      metaflow_load()
    },
    error = function(e) {
    }
  )
  invisible(NULL)
}
